"""后台巡检：Alert 过期、工单恢复观察期满、保留策略、自身指标。

逻辑放在 run_sweep() 里（同步、可测），异步任务只负责定时触发；
调用方必须把它放到线程池执行 —— 里面有 Prometheus 与飞书的同步网络调用，
直接跑在事件循环里会冻结所有 HTTP 请求。
"""
from __future__ import annotations

import logging
import re
import threading
from datetime import datetime, timedelta

from sqlalchemy import delete, or_, select
from sqlalchemy.orm import Session

from app.config import settings
from app.db import queries
from app.db.models import Alert, Incident, RawEvent
from app.db.queries import OPEN_STATES
from app.integrations.prometheus import get_client as get_prometheus_client
from app.logging_setup import get_logger, log
from app.metrics import set_gauge
from app.netutil import promql_string
from app.services import alert_service, incident_service, raw_store
from app.services.locks import PROCESS_LOCK
from app.timeutil import ensure_utc, utcnow

logger = get_logger("app.services.sweeper")


def verify_recovery(session: Session, incident: Incident) -> tuple[str, dict]:
    """恢复验证。返回 (state, detail)，state ∈ recovered / not_recovered / unverified。

    判据必须「同类型」且精确（2026-09-14 修正，之前出过假恢复）：
      · 只用 `up{instance=~"<ip>.*"}` 会被中间件地址骗到 —— 节点告警的 instance 是
        kube-state-metrics 的 198.51.100.210:8080（4 台节点共用），它永远 up=1，
        于是任何这类工单 20 分钟后都判「已恢复」并推卡，而节点其实一直 NotReady。
      · 节点类先看 K8s 权威判据：kube_node_status_condition{node,condition="Ready",status="true"}。
      · `up` 匹配要带 `(:.*)?`：PromQL 的 `=~` 两端自动锚定，写成 `^<ip>:` 等于要求
        以冒号结尾 → 永远查不到。
      · **查不到任何序列 = 无法确认（unverified），绝不能当成「已恢复」**。
    """
    session.flush()
    alerts = queries.attached_alerts(session, incident.incident_id)
    firing = [alert.alert_id for alert in alerts if alert.status == "FIRING"]
    if firing:
        return "not_recovered", {"reason": "alerts_still_firing", "alerts": firing}

    client = get_prometheus_client()
    nodes = sorted({alert.node for alert in alerts if alert.node})
    ips = sorted({alert.ip for alert in alerts if alert.ip})
    if not client.configured:
        # 没配指标源时的降级口径：只能按 Alert 状态判定（与历史行为一致）
        return "recovered", {"reason": "alerts_resolved", "verified_by": "alert_state_only"}

    # 判据要「同类型」：NodeNotReady 用节点 Ready 条件；
    # 而 GPU/Pod 这类挂在节点上的实体，节点 Ready **不能代表它自己好了**
    # （GPU XID 恢复了没有，节点 Ready 完全看不出来）→ 这类优先用采集端的 up{}。
    node_scoped = (incident.root_entity_type or "") == "node"
    evidence: dict = {"nodes": nodes, "ips": ips}

    notice = {}
    if not node_scoped:
        # 非节点类实体：节点 Ready 只是兜底依据，必须标注（GPU XID 恢复没恢复，节点 Ready 看不出来）
        notice = {"node_condition_fallback": True}

    def node_ready_verdict() -> tuple[str, dict] | None:
        ready: dict[str, bool] = {}
        for node in nodes:
            rows = client.instant(
                f'kube_node_status_condition{{node="{promql_string(node)}",condition="Ready",status="true"}}'
            )
            if rows:
                ready[node] = max(float(row.get("value", [0, 0])[1]) for row in rows) == 1
        if not ready:
            return None
        evidence["node_ready"] = ready
        evidence.update(notice)
        not_ready = sorted(node for node, ok in ready.items() if not ok)
        if not_ready:
            return "not_recovered", {**evidence, "reason": "node_not_ready", "not_ready": not_ready}
        if len(ready) == len(nodes):
            return "recovered", {**evidence, "reason": "node_ready", "verified_by": "kube_state_metrics"}
        return None

    if nodes and (node_scoped or not ips):
        verdict = node_ready_verdict()
        if verdict is not None:
            return verdict

    if ips:
        broken: list[str] = []
        no_series: list[str] = []
        for ip in ips:
            # PromQL 的 =~ 是**两端自动锚定**的：写成 "^IP:" 等于要求以冒号结尾，
            # 而真实 instance 是 "IP:9400" —— 永远匹配不到，探测就会一直 unverified
            # 。所以尾部必须带 `(:.*)?`。
            rows = client.instant(f'up{{instance=~"^{promql_string(re.escape(ip))}(:.*)?"}}')
            if not rows:
                no_series.append(ip)
                continue
            if any(float(row.get("value", [0, 0])[1]) == 0 for row in rows):
                broken.append(ip)
        evidence["up_no_series"] = no_series
        if broken:
            return "not_recovered", {**evidence, "reason": "metrics_not_recovered", "ips": broken}
        if not no_series:
            return "recovered", {**evidence, "reason": "metrics_recovered", "verified_by": "prometheus"}
        return "unverified", {**evidence, "reason": "no_metrics_series"}

    return "unverified", {**evidence, "reason": "nothing_to_verify"}


def _expire_stale_alerts(session: Session, now: datetime) -> int:
    expired = alert_service.expire_stale_alerts(session, now, settings.alert_stale_seconds)
    touched = {alert.incident_id for alert in expired if alert.incident_id}
    for incident_id in touched:
        incident = queries.get_incident(session, incident_id)
        if incident is None:
            continue
        incident_service.add_event(session, incident_id, "ALERT_STALE_EXPIRED")
        incident_service.refresh_state(session, incident, now)
    session.flush()
    return len(expired)


def _apply_verdict(session: Session, incident: Incident, state: str, detail: dict, now: datetime, source: str) -> bool:
    """把恢复探测结论落到工单状态（只做 DB 写，必须持锁调用）。返回是否已关闭。"""
    if state == "recovered":
        incident.status = "RESOLVED"
        incident.resolved_at = now
        incident.recovery_deadline = None
        incident_service.refresh_counts(session, incident)
        incident_service.add_event(
            session, incident.incident_id, "INCIDENT_RESOLVED", content={**detail, "detected_by": source}
        )
        return True
    # not_recovered = 还能看到没恢复；unverified = 拿不到判据（不能当恢复）
    event = "RECOVERY_NOT_CONFIRMED" if state == "not_recovered" else "RECOVERY_UNVERIFIED"
    incident.status = "OPEN"
    incident.recovery_deadline = None
    incident_service.add_event(session, incident.incident_id, event, content={**detail, "detected_by": source})
    return False


def _push_resolved_cards(session: Session, resolved_ids: list[str]) -> None:
    """推关闭卡片（网络阶段，不持锁）。send_incident_card 内部有发送幂等。"""
    for incident_id in resolved_ids:
        incident = queries.get_incident(session, incident_id)
        if incident is None:
            continue
        try:
            from app.services.pipeline import send_incident_card

            send_incident_card(session, incident, "incident_resolved")
        except Exception as exc:  # 推送失败不能影响状态推进
            log(logger, logging.ERROR, "incident_resolved_notify_failed", incident_id=incident_id, error=str(exc))


def _resolve_recovered(session: Session, now: datetime) -> tuple[int, int]:
    """显式恢复信号（夜莺 is_recovered）走观察期后的判定与关闭。

    ️ 锁纪律：验证（可能打 Prometheus）与推卡片（打飞书）**都不持锁**，只有改状态在锁内。
    本项目的硬约束是「锁只包 DB 读-判-写」（见 locks.py）：巡检每 30 秒跑一次，
    持锁打网络会把 webhook 的入库一起堵住 —— 而 webhook 被堵正是夜莺超时重试的根因。
    """
    recovering = session.scalars(
        select(Incident).where(
            Incident.status == "RECOVERING",
            Incident.recovery_deadline.is_not(None),
            Incident.recovery_deadline <= now,
        )
    ).all()
    if not recovering:
        return 0, 0

    # ① 网络阶段（不持锁）：逐个验证是否真的恢复
    verdicts = [(incident, *verify_recovery(session, incident)) for incident in recovering]

    resolved = held = 0
    resolved_ids: list[str] = []
    for incident, state, detail in verdicts:
        with PROCESS_LOCK:  # ② DB 写阶段（短）
            closed = _apply_verdict(session, incident, state, detail, now, source="explicit_recovery")
            resolved += closed
            held += not closed
            session.flush()
        if closed:
            log(logger, logging.INFO, "incident_resolved", incident_id=incident.incident_id, detail=detail)
            resolved_ids.append(incident.incident_id)
        else:
            log(logger, logging.WARNING, "incident_recovery_" + state, incident_id=incident.incident_id, detail=detail)

    # ③ 网络阶段（不持锁）：推关闭卡片
    _push_resolved_cards(session, resolved_ids)
    return resolved, held


def _probe_recoveries(session: Session, now: datetime) -> tuple[int, int]:
    """主动探测静默工单是否真的恢复。

    用途：人工处理完之后，故障是否好了不能靠"告警不来"猜（静默只是没消息），
    夜莺也不一定发 is_recovered。所以对「告警已静默超过 recovery_probe_after_seconds」的工单，
    按 recovery_probe_seconds 的间隔主动查权威判据：
      recovered     → 关单 + 群里通报恢复卡（恢复卡不受每日封顶限制）
      not_recovered → 保持 OPEN，等次日 09:00 汇总
      unverified    → 保持 OPEN（拿不到判据就不下结论）

    ️ 触发条件是"静默"而不是"告警已过期(stale)"：夜莺重发周期是小时级、stale 窗口 2 小时，
    若等 stale 才探测，运维修好之后最长要 2 小时才有结论。按静默判（默认 5 分钟）
    可以让恢复通报在 ~10 分钟内到群里，而"静默但还没好"只会多花一次查询、不影响状态。

    限流与留痕：last_probe_at 控制间隔，last_probe_result 只在"结论变化"时记时间线事件，
    避免每 5 分钟往时间线里刷一条。
    """
    candidates = session.scalars(
        select(Incident).where(
            Incident.status.in_(OPEN_STATES),
            Incident.merged_into.is_(None),
        )
    ).all()
    if not candidates:
        return 0, 0

    # 探测必须有判据来源：没配指标源就没法验证，"探测不出来"不能当"已恢复"
    # （静默不等于恢复 —— 这正是 09-14 修掉的假恢复）
    if not get_prometheus_client().configured:
        return 0, 0

    # 还在持续刷新的告警说明故障仍在报，不进入待探测集合
    silence_cutoff = now - timedelta(seconds=settings.recovery_probe_after_seconds)
    recent_ids = {
        row[0]
        for row in session.execute(
            select(Alert.incident_id)
            .where(Alert.incident_id.is_not(None), Alert.last_seen >= silence_cutoff)
            .distinct()
        )
    }

    due: list[Incident] = []
    for incident in candidates:
        if incident.incident_id in recent_ids:
            continue  # 最近还在报，不是"静默待探测"
        if incident.status == "RECOVERING" and incident.recovery_deadline is not None:
            continue  # 显式恢复信号走 _resolve_recovered 的观察期路径
        last_seen = ensure_utc(incident.last_seen)
        if last_seen is None or last_seen >= silence_cutoff:
            continue  # 工单本身最近还有动静，给一点缓冲
        last_probe = ensure_utc(incident.last_probe_at)
        if last_probe is not None and (now - last_probe).total_seconds() < settings.recovery_probe_seconds:
            continue  # 限流：同一工单按间隔探测
        due.append(incident)
    if not due:
        return 0, 0

    # ① 网络阶段（不持锁）：查权威判据
    verdicts = [(incident, *verify_recovery(session, incident)) for incident in due]

    resolved = held = 0
    resolved_ids: list[str] = []
    for incident, state, detail in verdicts:
        changed = state != (incident.last_probe_result or "")
        with PROCESS_LOCK:  # ② DB 写阶段（短）
            incident.last_probe_at = now
            incident.last_probe_result = state
            if state == "recovered":
                closed = _apply_verdict(session, incident, state, detail, now, source="probe")
            else:
                # 结论没变就不刷时间线，只更新探测痕迹
                closed = _apply_verdict(session, incident, state, detail, now, source="probe") if changed else False
            resolved += closed
            held += not closed
            session.flush()
        if closed:
            log(logger, logging.INFO, "incident_resolved_by_probe", incident_id=incident.incident_id, detail=detail)
            resolved_ids.append(incident.incident_id)
        elif changed:
            log(
                logger,
                logging.INFO,
                "incident_recovery_probe",
                incident_id=incident.incident_id,
                state=state,
                detail=detail,
            )

    # ③ 网络阶段（不持锁）：恢复则通报群里
    _push_resolved_cards(session, resolved_ids)
    return resolved, held


_last_purge_at: datetime | None = None
_PURGE_INTERVAL_SECONDS = 3600


def _purge(session: Session, now: datetime) -> dict[str, int]:
    """保留策略：清理过期记录与归档文件，避免 SQLite/磁盘无界增长。"""
    removed = incident_service.purge_old_records(session, now)
    cutoff = now - timedelta(days=settings.raw_retention_days)
    result = session.execute(delete(RawEvent.__table__).where(RawEvent.__table__.c.received_at < cutoff))
    removed["raw_events"] = int(result.rowcount or 0)
    removed["raw_archive_files"] = raw_store.purge_older_than(cutoff)
    return removed


def _purge_due(now: datetime) -> bool:
    """保留策略每小时跑一次即可，不必每一跳都清库。"""
    global _last_purge_at
    if _last_purge_at is None or (now - _last_purge_at).total_seconds() >= _PURGE_INTERVAL_SECONDS:
        _last_purge_at = now
        return True
    return False


def _update_gauges(session: Session) -> None:
    counts = queries.status_counts(session)
    set_gauge("aiops_alerts_active", counts["alerts_firing"])
    set_gauge("aiops_incidents_open", counts["incidents_open"])
    set_gauge("aiops_raw_events_failed", counts["raw_events_failed"])


def _retry_stale_analysis(session: Session, now: datetime) -> int:
    """兜底：卡住的分析重新入队。

    worker 是单线程内存队列，任务可能因为异常或重启停在中间；
    有这一层，工单不会永久停在「有工单、没诊断」。
    只重试 PENDING/RUNNING（没跑完 = 卡片也没发出去），
    FAILED 不自动重试 —— 避免重复推卡片，先让人通过 analysis_error 看到原因。
    """
    from app.services import worker

    cutoff = now - timedelta(seconds=settings.analysis_stale_seconds)
    rows = session.execute(
        select(Incident.incident_id, Incident.analysis_kind).where(
            Incident.analysis_status.in_(worker.RESUMABLE_STATUSES),
            Incident.analysis_attempts < settings.analysis_max_attempts,
            or_(Incident.analysis_updated_at.is_(None), Incident.analysis_updated_at < cutoff),
        )
    ).all()
    for incident_id, kind in rows:
        worker.request_analysis(incident_id, kind or worker.DEFAULT_KIND)
    if rows:
        log(logger, logging.WARNING, "analysis_requeued_by_sweeper", count=len(rows))
    return len(rows)


def run_sweep(session: Session, now: datetime | None = None) -> dict:
    """跑一轮巡检。长网络调用不持锁（见 _resolve_recovered 的说明）。"""
    now = now or utcnow()
    with PROCESS_LOCK:
        stale = _expire_stale_alerts(session, now)
    summary = {"stale_alerts": stale}
    resolved, held = _resolve_recovered(session, now)
    summary.update({"incidents_resolved": resolved, "incidents_held": held})
    # 主动探测：人工处理完之后由我们自己发现"好了没有"，不等夜莺恢复信号
    probed, probed_held = _probe_recoveries(session, now)
    summary.update({"probe_resolved": probed, "probe_held": probed_held})
    summary["analysis_requeued"] = _retry_stale_analysis(session, now)
    if _purge_due(now):
        with PROCESS_LOCK:
            summary["purged"] = _purge(session, now)
    with PROCESS_LOCK:
        _update_gauges(session)
    if any(value for value in summary.values() if not isinstance(value, dict)):
        log(logger, logging.INFO, "sweep_done", **summary)
    return summary


_SWEEP_GUARD = threading.Lock()


def run_sweep_single_flight(session: Session, now: datetime | None = None) -> dict | None:
    """定时与手动巡检的统一入口：同一时刻只允许一个 sweep 在跑。

    没有这层，手动触发与定时巡检并发时会重复遍历 RECOVERING 工单
    （重复推「工单关闭」卡片、重复 purge、_last_purge_at 竞态）。
    返回 None 表示已有巡检在跑、本次跳过。
    """
    if not _SWEEP_GUARD.acquire(blocking=False):
        log(logger, logging.WARNING, "sweep_skipped_already_running")
        return None
    try:
        return run_sweep(session, now)
    finally:
        _SWEEP_GUARD.release()
