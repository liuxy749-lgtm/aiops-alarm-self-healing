"""Alert 生命周期：去重（upsert）、恢复、过期。

去重的硬保证是 alerts 表上的部分唯一索引（同一 fingerprint 只能有一条 FIRING）；
应用层先查后建只是快路径，并发下靠唯一约束兜底 + 重试。
"""
from __future__ import annotations

import contextlib
import logging
from datetime import datetime, timedelta

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.db.models import Alert
from app.models.schemas import NormalizedEvent
from app.timeutil import ensure_utc, utcnow


def new_alert_id(fingerprint: str) -> str:
    return f"ALT-{fingerprint[:12]}-{utcnow().strftime('%Y%m%d%H%M%S%f')}"


def _apply_fields(alert: Alert, norm: NormalizedEvent) -> None:
    """用最新事件刷新字段。只在有新值时覆盖，避免富化降级把好数据冲掉。"""
    alert.severity = norm.severity or alert.severity
    alert.entity_type = norm.entity.type or alert.entity_type
    alert.entity_id = norm.entity.id or alert.entity_id
    alert.ip = norm.entity.ip or alert.ip
    alert.hostname = norm.entity.hostname or alert.hostname
    alert.node = norm.entity.node or norm.scope.node or alert.node
    alert.namespace = norm.entity.namespace or norm.scope.namespace or alert.namespace
    alert.cluster = norm.scope.cluster or alert.cluster
    alert.region = norm.scope.region or alert.region
    alert.zone = norm.scope.zone or alert.zone
    alert.projset = norm.scope.projset or alert.projset
    alert.project = norm.scope.project or alert.project
    alert.accelerator_model = norm.hardware.accelerator_model or alert.accelerator_model
    alert.value = norm.value if norm.value is not None else alert.value
    alert.summary = norm.summary or alert.summary
    first_seen = ensure_utc(alert.first_seen)
    if first_seen is None or norm.occurred_at < first_seen:
        alert.first_seen = norm.occurred_at
    if norm.enrichment:
        merged = dict(alert.enrichment or {})
        merged.update(norm.enrichment)
        alert.enrichment = merged
    if norm.enrichment_status and norm.enrichment_status != "PENDING":
        alert.enrichment_status = norm.enrichment_status


def _find_firing(session: Session, fingerprint: str) -> Alert | None:
    return session.scalar(select(Alert).where(Alert.fingerprint == fingerprint, Alert.status == "FIRING"))


def _touch(alert: Alert, norm: NormalizedEvent) -> Alert:
    alert.last_seen = max(ensure_utc(alert.last_seen) or norm.occurred_at, norm.occurred_at)
    alert.occurrence_count += 1
    _apply_fields(alert, norm)
    return alert


def upsert_firing(session: Session, norm: NormalizedEvent, fingerprint: str) -> tuple[Alert, bool]:
    """返回 (alert, created)。created=False 表示合并进了已有 Alert。"""
    existing = _find_firing(session, fingerprint)
    if existing is not None:
        return _touch(existing, norm), False

    for _attempt in range(3):
        alert = Alert(
            alert_id=new_alert_id(fingerprint),
            fingerprint=fingerprint,
            alertname=norm.event_type,
            status="FIRING",
            first_seen=norm.occurred_at,
            last_seen=norm.occurred_at,
            occurrence_count=1,
            labels=norm.labels,
            annotations=norm.annotations,
            summary=norm.summary,
            value=norm.value,
        )
        _apply_fields(alert, norm)
        try:
            with session.begin_nested():
                session.add(alert)
                session.flush()
            return alert, True
        except IntegrityError:
            # 并发下有请求先建好了同指纹的 FIRING → 转成 update
            with contextlib.suppress(Exception):
                session.expunge(alert)
            existing = _find_firing(session, fingerprint)
            if existing is not None:
                return _touch(existing, norm), False

    raise RuntimeError(f"alert upsert 重试耗尽: fingerprint={fingerprint}")


def mark_resolved(session: Session, fingerprint: str, occurred_at: datetime, reason: str = "resolved") -> Alert | None:
    alert = _find_firing(session, fingerprint)
    if alert is None:
        return None
    alert.status = "RESOLVED"
    alert.resolved_at = occurred_at
    alert.resolution_reason = reason
    return alert


def reset_active_firing(session: Session, fingerprint: str, reason: str = "debug_reset") -> int:
    """调试用：把该指纹下活跃的 Alert 置为 RESOLVED，让下一次事件能新建 Alert。

    为什么需要它：同一条测试告警在 15 分钟过期窗口内重复发送时，
    指纹相同 → 会被合并进已有 Alert（ALERT_UPDATED），不会产生新工单，
    联调时看不到完整链路。只在 AIOPS_DEBUG_SKIP_DEDUP 打开时调用。
    """
    alerts = list(
        session.scalars(select(Alert).where(Alert.fingerprint == fingerprint, Alert.status == "FIRING")).all()
    )
    for alert in alerts:
        alert.status = "RESOLVED"
        alert.resolved_at = utcnow()
        alert.resolution_reason = reason
    if alerts:
        # sessionmaker 关了 autoflush：不 flush 的话紧接着的 upsert 查询
        # 仍会看到旧的 FIRING 记录，等于没重置
        session.flush()
    return len(alerts)


def expire_stale_alerts(session: Session, now: datetime, stale_seconds: int) -> list[Alert]:
    """夜莺不发 resolve 时的兜底：长时间无事件的 FIRING 自动判定恢复。

    没有这条，老 Alert 会永远 FIRING，第二天的同类告警会被合并进几天前那条 Alert。
    """
    cutoff = now - timedelta(seconds=stale_seconds)
    alerts = list(session.scalars(select(Alert).where(Alert.status == "FIRING", Alert.last_seen < cutoff)).all())
    for alert in alerts:
        alert.status = "RESOLVED"
        alert.resolved_at = now
        alert.resolution_reason = "stale"
    if alerts:
        from app.logging_setup import get_logger, log

        log(get_logger("app.services.alert"), logging.WARNING, "alerts_expired", count=len(alerts))
    return alerts
