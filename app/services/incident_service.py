"""Incident 引擎：创建、关联、根因提升、合并、时间线、恢复观察。"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session

from app.config import settings
from app.correlation.rules import get_rules
from app.correlation.topology import TopologyService
from app.db import queries
from app.db.models import Alert, FeishuMessage, Incident, IncidentAlert, IncidentEvent, LLMCall
from app.logging_setup import get_logger, log
from app.timeutil import ensure_utc, utcnow

logger = get_logger("app.services.incident")

SEVERITY_RANK = {"P0": 0, "P1": 1, "P2": 2, "P3": 3}
OPEN_STATES = queries.OPEN_STATES  # 兼容既有引用
# 夜莺显式恢复时写进 alerts.resolution_reason 的值（见 alert_service.mark_resolved 默认值）
RECOVERY_REASON = "resolved"
# 告警超时未再来（非恢复信号）时写进 resolution_reason 的值
STALE_REASON = "stale"


def worst_severity(values: list[str | None]) -> str | None:
    ranked = [value for value in values if value]
    return sorted(ranked, key=lambda item: SEVERITY_RANK.get(item, 99))[0] if ranked else None


def add_event(
    session: Session,
    incident_id: str,
    event_type: str,
    actor: str | None = None,
    content: dict | None = None,
) -> None:
    session.add(IncidentEvent(incident_id=incident_id, event_type=event_type, actor=actor, content=content))


def next_incident_id(session: Session, at: datetime) -> str:
    """INC-YYYYMMDD-NNN。序号必须**单调递增**，所以不能只看 incidents 表的 MAX。

    原因：工单被删除（如人工治理）后，只看 incidents 的 MAX 会把已用过的号段再发一次，
    而飞书留档 feishu_messages 是 append-only 的 —— 撞号会让 already_notified 误判
    「这张工单已经推过卡」，真实告警的卡片被静默跳过。
    曾出现过：清理测试单后，人工触发的 GPU XID 告警建单时卡片没发出去。
    因此同时把飞书留档与时间线里用过的号段算进来。
    """
    prefix = f"INC-{at.strftime('%Y%m%d')}-"
    candidates = [
        session.scalar(select(func.max(Incident.incident_id)).where(Incident.incident_id.like(prefix + "%"))),
        session.scalar(select(func.max(FeishuMessage.incident_id)).where(FeishuMessage.incident_id.like(prefix + "%"))),
        session.scalar(select(func.max(IncidentEvent.incident_id)).where(IncidentEvent.incident_id.like(prefix + "%"))),
    ]
    sequences = [
        int(value.rsplit("-", 1)[-1])
        for value in candidates
        if value and value.rsplit("-", 1)[-1].isdigit()
    ]
    return f"{prefix}{max(sequences) + 1 if sequences else 1:03d}"


def refresh_counts(session: Session, incident: Incident) -> None:
    """重算 alert_count。调用方需先 flush，否则计数读不到刚写入的行。"""
    incident.alert_count = int(
        session.scalar(
            select(func.count())
            .select_from(IncidentAlert)
            .where(IncidentAlert.incident_id == incident.incident_id)
        )
        or 0
    )


def _as_confidence(value) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def apply_diagnosis(session: Session, incident: Incident, result) -> None:
    """把诊断结果写回工单字段。/analyze 接口与主流水线共用，避免两处实现漂移。"""
    diagnosis = result.data or {}
    incident.suspected_root_cause = diagnosis.get("suspected_root_cause")
    incident.root_cause_confidence = _as_confidence(diagnosis.get("confidence"))
    incident.ai_summary = diagnosis.get("summary")
    incident.ai_diagnosis = diagnosis
    # 标题只接受模型给的**短标题**（≤40 字）。早期版本直接把整段 summary（可能 200+ 字）当标题，
    # 飞书卡片标题变成一整段话，刷屏且看不清。模型没给就保留原生成标题。
    short_title = str(diagnosis.get("title") or "").strip()
    if short_title and len(short_title) <= 40:
        incident.title = short_title


# ----------------------------------------------------------------------
# 工单生命周期
# ----------------------------------------------------------------------
def create_incident(session: Session, alert: Alert, decision=None) -> Incident:
    incident = Incident(
        incident_id=next_incident_id(session, alert.first_seen),
        title=f"{alert.alertname} @ {alert.entity_id}",
        status="OPEN",
        severity=alert.severity,
        root_entity_type=alert.entity_type,
        root_entity_id=alert.entity_id,
        root_alert_id=alert.alert_id,
        node=alert.node,
        cluster=alert.cluster,
        first_seen=alert.first_seen,
        last_seen=alert.last_seen,
        alert_count=0,
    )
    session.add(incident)
    session.flush()
    add_event(
        session,
        incident.incident_id,
        "INCIDENT_CREATED",
        content={
            "root_alert": alert.alert_id,
            "alertname": alert.alertname,
            "entity": f"{alert.entity_type}:{alert.entity_id}",
            "score": getattr(decision, "score", None),
            "reasons": getattr(decision, "reasons", None),
        },
    )
    attach_alert(session, incident, alert, decision, relation="ROOT")
    return incident


def attach_alert(
    session: Session,
    incident: Incident,
    alert: Alert,
    decision=None,
    relation: str | None = None,
) -> None:
    exists = session.scalar(
        select(IncidentAlert).where(
            IncidentAlert.incident_id == incident.incident_id, IncidentAlert.alert_id == alert.alert_id
        )
    )
    if exists is not None:
        return

    session.add(
        IncidentAlert(
            incident_id=incident.incident_id,
            alert_id=alert.alert_id,
            relation_type=relation or getattr(decision, "relation", None) or "UNKNOWN",
            correlation_score=getattr(decision, "score", None),
            correlation_reason={
                "reasons": getattr(decision, "reasons", []),
                "rule_id": getattr(decision, "rule_id", None),
                "action": getattr(decision, "action", None),
                "evaluated": getattr(decision, "evaluated", []),
            },
        )
    )
    alert.incident_id = incident.incident_id
    incident.last_seen = max(ensure_utc(incident.last_seen) or alert.last_seen, alert.last_seen)
    incident.severity = worst_severity([incident.severity, alert.severity])
    if incident.status in ("RECOVERING", "RESOLVED"):
        # RESOLVED 也要能复开：同类故障复发时挂回原单（见 pipeline 里按 fingerprint 复用），
        # 否则一场反复复发的故障会摊成一串各自独立的工单。
        was = incident.status
        incident.status = "OPEN"
        incident.recovery_deadline = None
        incident.resolved_at = None
        add_event(
            session,
            incident.incident_id,
            "INCIDENT_REOPENED",
            content={"by_alert": alert.alert_id, "from_status": was},
        )

    add_event(
        session,
        incident.incident_id,
        "ALERT_ATTACHED",
        content={
            "alert_id": alert.alert_id,
            "alertname": alert.alertname,
            "entity": f"{alert.entity_type}:{alert.entity_id}",
            "relation": relation or getattr(decision, "relation", None),
            "score": getattr(decision, "score", None),
            "reasons": getattr(decision, "reasons", []),
            "rule_id": getattr(decision, "rule_id", None),
        },
    )
    # sessionmaker 关了 autoflush：不 flush 的话 refresh_counts 读不到刚 add 的行
    session.flush()
    refresh_counts(session, incident)
    _maybe_promote_root(session, incident, alert, decision)


def _maybe_promote_root(session: Session, incident: Incident, alert: Alert, decision=None) -> None:
    """根因提升：新告警是因果链里的 cause、或优先级高于当前根因时上位。

    不做这个的话，「症状先到、根因后到」会把根因永久判成那个症状。
    """
    rules = get_rules()
    relation = getattr(decision, "relation", None)
    current_root_name = queries.root_alertname(session, incident)

    new_priority = rules.priority(alert.alertname)
    old_priority = rules.priority(current_root_name) if current_root_name else -1
    if relation != "ROOT" and new_priority <= old_priority:
        return

    old_root = {
        "alert_id": incident.root_alert_id,
        "alertname": current_root_name,
        "entity": f"{incident.root_entity_type}:{incident.root_entity_id}",
    }
    incident.root_alert_id = alert.alert_id
    incident.root_entity_type = alert.entity_type
    incident.root_entity_id = alert.entity_id
    incident.node = alert.node or incident.node
    incident.title = f"{alert.alertname} @ {alert.entity_id}"

    session.execute(
        IncidentAlert.__table__.update()
        .where(
            (IncidentAlert.incident_id == incident.incident_id)
            & (IncidentAlert.alert_id == old_root["alert_id"])
        )
        .values(relation_type="SYMPTOM")
    )
    session.execute(
        IncidentAlert.__table__.update()
        .where(
            (IncidentAlert.incident_id == incident.incident_id)
            & (IncidentAlert.alert_id == alert.alert_id)
        )
        .values(relation_type="ROOT")
    )
    add_event(
        session,
        incident.incident_id,
        "INCIDENT_ROOT_CHANGED",
        content={
            "old_root": old_root,
            "new_root": {
                "alert_id": alert.alert_id,
                "alertname": alert.alertname,
                "entity": f"{alert.entity_type}:{alert.entity_id}",
            },
            "new_priority": new_priority,
            "old_priority": old_priority,
        },
    )
    log(
        logger,
        20,
        "incident_root_promoted",
        incident_id=incident.incident_id,
        new_root=alert.alertname,
        old_root=current_root_name,
    )


def merge_incidents(session: Session, keep: Incident, drop: Incident, reason: str) -> None:
    for link in session.scalars(
        select(IncidentAlert).where(IncidentAlert.incident_id == drop.incident_id)
    ).all():
        duplicate = session.scalar(
            select(IncidentAlert).where(
                IncidentAlert.incident_id == keep.incident_id, IncidentAlert.alert_id == link.alert_id
            )
        )
        if duplicate is not None:
            session.delete(link)
            continue
        link.incident_id = keep.incident_id

    for alert in session.scalars(select(Alert).where(Alert.incident_id == drop.incident_id)).all():
        alert.incident_id = keep.incident_id

    keep.first_seen = min(ensure_utc(keep.first_seen), ensure_utc(drop.first_seen))  # type: ignore[arg-type]
    keep.last_seen = max(ensure_utc(keep.last_seen), ensure_utc(drop.last_seen))  # type: ignore[arg-type]
    keep.severity = worst_severity([keep.severity, drop.severity])
    session.flush()
    refresh_counts(session, keep)

    drop.status = "MERGED"
    drop.merged_into = keep.incident_id
    add_event(
        session, keep.incident_id, "INCIDENT_MERGED_IN", content={"merged_incident": drop.incident_id, "reason": reason}
    )
    add_event(
        session, drop.incident_id, "INCIDENT_MERGED_OUT", content={"merged_into": keep.incident_id, "reason": reason}
    )


def reconcile_incidents(session: Session, incident: Incident) -> str | None:
    """与其它 OPEN 工单核对一次能否合并。

    漏关联比错关联更容易发生，而且没人会手动去合并两个工单。
    典型场景是「症状先到、根因后到」：PodNotReady 各建了工单，NodeNotReady 到达后
    这些工单其实同根同因，必须自动收敛。
    合并触发条件（任一）：force_link / 因果规则 / 拓扑距离 ≤2；
    但 never_link 优先，且跨节点不合并（否则 node01 的故障会吞掉 node02 的工单）。
    """
    rules = get_rules()
    topology = TopologyService(session)
    window = timedelta(seconds=rules.max_window())
    my_names = queries.attached_alert_names(session, incident.incident_id)
    if not my_names:
        return None

    for other in queries.open_incidents(session):
        if other.incident_id == incident.incident_id:
            continue
        other_last_seen = ensure_utc(other.last_seen)
        mine_first_seen = ensure_utc(incident.first_seen)
        if other_last_seen is None or mine_first_seen is None:
            continue
        if abs((other_last_seen - mine_first_seen).total_seconds()) > window.total_seconds():
            continue
        if incident.cluster and other.cluster and incident.cluster != other.cluster:
            continue

        other_names = queries.attached_alert_names(session, other.incident_id)
        if not other_names:
            continue
        if any(rules.never(a, b) for a in my_names for b in other_names):
            continue

        forced = any(rules.force(a, b) for a in my_names for b in other_names)
        causal = None
        if not forced:
            for name in sorted(my_names):
                causal = rules.causal_match(name, other_names)
                if causal is not None:
                    break
            if causal is None:
                for name in sorted(other_names):
                    causal = rules.causal_match(name, my_names)
                    if causal is not None:
                        break

        distance = topology.distance(
            (incident.root_entity_type or "", incident.root_entity_id or ""),
            (other.root_entity_type or "", other.root_entity_id or ""),
        )
        # force_link / 因果规则都要求同一节点（拓扑距离是实体级关系，不套这个限制）
        node_mismatch = bool(incident.node and other.node and incident.node != other.node)
        if node_mismatch and (forced or causal is not None):
            continue
        if not (forced or causal or distance in (1, 2)):
            continue

        if forced:
            reason = "force_link"
        elif causal is not None:
            reason = f"causal_rule:{causal[0].id}"
        else:
            reason = f"topology_distance_{distance}"
        keep, drop = (
            (other, incident)
            if (other.first_seen, other.incident_id) <= (incident.first_seen, incident.incident_id)
            else (incident, other)
        )
        merge_incidents(session, keep, drop, reason)
        log(logger, 20, "incidents_merged", keep=keep.incident_id, drop=drop.incident_id, reason=reason)
        return keep.incident_id
    return None


def refresh_state(session: Session, incident: Incident, now: datetime | None = None) -> str:
    """按 Alert 状态推进工单状态机。

    ️ 「告警不再来」不等于「恢复」（2026-09-14 修正）：
    只有夜莺**显式恢复**（is_recovered=true → resolution_reason=resolved）才进恢复观察期；
    单纯 stale 过期只说明这段时间没有新通知，不能自动关单 ——
    几周不可达的僵尸节点每小时被判一次「恢复」并推「工单恢复」卡，语义完全错。
    """
    now = now or utcnow()
    # 必须先 flush：调用方刚把 Alert 置为 RESOLVED 时，不 flush 就查还是 FIRING，
    # 工单永远进不了 RECOVERING。
    session.flush()
    firing = int(
        session.scalar(
            select(func.count())
            .select_from(Alert)
            .where(Alert.incident_id == incident.incident_id, Alert.status == "FIRING")
        )
        or 0
    )
    if firing > 0:
        if incident.status == "RECOVERING":
            incident.status = "OPEN"
            incident.recovery_deadline = None
            add_event(session, incident.incident_id, "INCIDENT_REOPENED", content={"reason": "alert_firing_again"})
        return incident.status

    if incident.status == "RESOLVED":
        return incident.status

    explicit_recovery = int(
        session.scalar(
            select(func.count())
            .select_from(Alert)
            .where(Alert.incident_id == incident.incident_id, Alert.resolution_reason == RECOVERY_REASON)
        )
        or 0
    )
    if explicit_recovery == 0:
        # 只是「静下来了」：不进恢复流程、不关单、不推恢复卡；标记一次待人工/待恢复信号。
        if incident.status == "RECOVERING":
            incident.status = "OPEN"
            incident.recovery_deadline = None
        _record_unconfirmed_once(session, incident)
        return incident.status

    if incident.status != "RECOVERING":
        incident.status = "RECOVERING"
        incident.recovery_deadline = now + timedelta(seconds=settings.recovery_observe_seconds)
        add_event(
            session,
            incident.incident_id,
            "RECOVERY_OBSERVING",
            content={
                "observe_seconds": settings.recovery_observe_seconds,
                "deadline": incident.recovery_deadline.isoformat(),
                "signal": "nightingale_is_recovered",
            },
        )
    return incident.status


def _record_unconfirmed_once(session: Session, incident: Incident) -> None:
    """只记一次「未确认恢复」，避免每轮巡检往时间线里刷同一条。"""
    exists = session.scalar(
        select(func.count())
        .select_from(IncidentEvent)
        .where(
            IncidentEvent.incident_id == incident.incident_id,
            IncidentEvent.event_type == "RECOVERY_UNCONFIRMED",
        )
    )
    if exists:
        return
    add_event(
        session,
        incident.incident_id,
        "RECOVERY_UNCONFIRMED",
        content={
            "reason": "silence_only",
            "note": "告警已停止（stale 过期）但未收到夜莺恢复信号，不自动关单",
        },
    )
    log(
        logger,
        logging.WARNING,
        "recovery_unconfirmed",
        incident_id=incident.incident_id,
    )


def ack_incident(session: Session, incident: Incident, actor: str) -> Incident:
    if incident.status == "OPEN":
        incident.status = "ACKNOWLEDGED"
    incident.acknowledged_at = utcnow()
    add_event(session, incident.incident_id, "USER_ACKNOWLEDGED", actor=actor)
    return incident


def resolve_incident(session: Session, incident: Incident, actor: str, reason: str | None = None) -> Incident:
    incident.status = "RESOLVED"
    incident.resolved_at = utcnow()
    incident.recovery_deadline = None
    add_event(session, incident.incident_id, "INCIDENT_RESOLVED", actor=actor, content={"reason": reason, "manual": True})
    return incident


def detach_alert(session: Session, incident: Incident, alert_id: str, actor: str) -> bool:
    """人工纠错：把误关联的告警从工单里摘出去。"""
    link = session.scalar(
        select(IncidentAlert).where(
            IncidentAlert.incident_id == incident.incident_id, IncidentAlert.alert_id == alert_id
        )
    )
    if link is None:
        return False
    session.delete(link)
    alert = session.scalar(select(Alert).where(Alert.alert_id == alert_id))
    if alert is not None:
        alert.incident_id = None
    session.flush()
    refresh_counts(session, incident)
    add_event(session, incident.incident_id, "ALERT_DETACHED", actor=actor, content={"alert_id": alert_id})
    return True


def purge_old_records(session: Session, now: datetime | None = None) -> dict[str, int]:
    """保留策略：清理过期时间线、模型调用与推送记录，避免 SQLite 无界膨胀。

    raw_events（含本地 JSONL 归档）由 sweeper 单独处理。
    """
    now = now or utcnow()
    cutoff = now - timedelta(days=settings.event_retention_days)
    removed: dict[str, int] = {}
    for model in (IncidentEvent, LLMCall, FeishuMessage):
        table = model.__table__
        result = session.execute(delete(table).where(table.c.created_at < cutoff))
        removed[table.name] = int(result.rowcount or 0)
    return removed
