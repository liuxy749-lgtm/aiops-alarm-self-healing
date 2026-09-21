"""统一处理流水线：Raw → Normalize → Enrich → Fingerprint → Alert → 关联 → 工单 → (后台) 上下文 → AI → 飞书。

写入串行化：一期是单副本 SQLite，用进程内可重入锁把「读-判-写」串起来，
数据库侧还有部分唯一索引兜底。开多副本必须换 PostgreSQL + 分布式锁。

副作用出锁（2026-09-11 落地）：
webhook 只做 DB 短事务（raw 落盘 → 标准化 → 富化 → 指纹 → Alert/工单），
随即返回 200；采集 / AI 诊断 / 飞书推送交给 app/services/worker.py 在后台执行。
原因是实测：整链同步时 AI 诊断 10~13 秒会超过夜莺客户端超时（报
`context deadline exceeded` 并重试），且锁把 LLM 也包住会让告警风暴串行排队。
AIOPS_INLINE_ANALYSIS=true 可退回同步执行（对照与回滚用）。
"""
from __future__ import annotations

import logging
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta

from sqlalchemy import select, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from app.config import settings
from app.correlation.engine import CorrelationEngine, LinkDecision
from app.db import queries
from app.db.models import Alert, FeishuMessage, Incident, RawEvent
from app.db.session import SessionLocal, WorkerSessionLocal
from app.integrations.deepseek import DiagnosisClient
from app.integrations.feishu import notify_alert_created, notify_alert_resolved, notify_incident
from app.logging_setup import get_logger, log
from app.metrics import inc
from app.services import alert_service, context_collector, incident_service, normalizer, raw_store
from app.services import fingerprint as fingerprint_service
from app.services import worker
from app.services.enricher import enrich
from app.services.locks import PROCESS_LOCK
from app.timeutil import local_day_start, utcnow

logger = get_logger("app.services.pipeline")

# 允许推工单卡片的三个时机：创建 / 根因变化 / 关闭。
# 其它 kind（如 manual_reanalyze）只刷新诊断、不推卡，避免刷群。
CARD_KINDS = {"incident_created", "incident_root_changed", "incident_resolved"}


@dataclass
class EventResult:
    event_id: str | None = None
    status: str = "PROCESSED"  # PROCESSED | DUPLICATE | FAILED
    source: str | None = None
    alert_id: str | None = None
    incident_id: str | None = None
    action: str | None = None
    alert_created: bool | None = None
    error: str | None = None

    def as_dict(self) -> dict:
        return {key: value for key, value in asdict(self).items() if value is not None}


# ----------------------------------------------------------------------
# 飞书工单卡片
# ----------------------------------------------------------------------
def _correlation_reasons(session: Session, incident_id: str) -> list[str]:
    """汇总关联原因，卡片上要能回答「为什么这些告警算同一个故障」。"""
    from app.db.models import IncidentAlert

    reasons: list[str] = []
    for link in session.scalars(
        select(IncidentAlert).where(IncidentAlert.incident_id == incident_id)
    ).all():
        for reason in (link.correlation_reason or {}).get("reasons") or []:
            if reason not in reasons:
                reasons.append(reason)
    return reasons


def send_incident_card(session: Session, incident: Incident, kind: str) -> None:
    """把工单卡片推到飞书工单通道。

    只允许三个时机：工单创建 / 根因变化 / 工单关闭。
    每次关联告警都推的话，一场节点故障风暴会往群里刷 N 张卡片，比原来的告警还吵。

    另外做了发送幂等（同一工单同一 kind 已成功推过就跳过）：后台重试、
    进程重启后的启动补偿、卡片已发但状态未落 DONE 时被重启，都会重复触发这一步。
    """
    if already_notified(session, kind, incident_id=incident.incident_id):
        log(logger, logging.INFO, "incident_card_skipped_already_sent", incident_id=incident.incident_id, kind=kind)
        return
    # 每天只发一次：当天已经为这张工单发过卡就不再发（恢复卡例外，见 _DAILY_CAP_EXEMPT）。
    # 未恢复的故障由每日 09:00 汇总重新提醒，不靠重复推卡。
    if settings.daily_card_cap and kind not in _DAILY_CAP_EXEMPT and sent_today(session, incident.incident_id):
        inc("aiops_incident_card_suppressed_total", {"reason": "daily_cap"})
        log(logger, logging.INFO, "incident_card_suppressed_daily_cap", incident_id=incident.incident_id, kind=kind)
        return
    alerts = queries.attached_alerts(session, incident.incident_id)
    # 合并/关联后 alert_count 可能滞后 → 发卡前校准，避免卡片数字与「节点详情」自相矛盾
    if len(alerts) != (incident.alert_count or 0):
        incident_service.refresh_counts(session, incident)
    events = queries.timeline(session, incident.incident_id)
    # 恢复卡带上「依据」：从时间线里取最近一次 INCIDENT_RESOLVED 的验证细节
    resolution = None
    if kind == "incident_resolved":
        resolution = next(
            (event.content for event in events if event.event_type == "INCIDENT_RESOLVED" and event.content), None
        )
    notify_incident(
        session,
        incident,
        alerts,
        incident.ai_diagnosis or {},
        events,
        kind=kind,
        reasons=_correlation_reasons(session, incident.incident_id),
        resolution=resolution,
    )
    incident_service.add_event(
        session, incident.incident_id, "FEISHU_SENT", content={"channel": "incident", "kind": kind}
    )


def _analyze_and_notify(session: Session, incident: Incident, *, notify: bool, kind: str) -> DiagnosisResult:
    """上下文采集 + AI 诊断（+ 可选推卡片）。

    采集与诊断失败一律不阻断告警本身，只记录并降级。
    notify=False 时只刷新数据不推卡片 —— 告警挂到已有工单属于同一故障的更多证据。
    """
    try:
        incident_service.add_event(session, incident.incident_id, "CONTEXT_COLLECTION_STARTED")
        context = context_collector.collect(session, incident)
    except Exception as exc:
        # 必须写回 incident.context：只记日志的话，接口与卡片上完全看不出采集失败，
        # 会误以为「没有异常」而不是「拿不到数据」。
        log(logger, logging.ERROR, "context_collection_failed", incident_id=incident.incident_id, error=str(exc))
        context = {"error": f"{type(exc).__name__}: {exc}"}
        incident.context = context

    result = DiagnosisClient().diagnose(session, incident.incident_id, context)
    incident_service.apply_diagnosis(session, incident, result)
    incident_service.add_event(
        session,
        incident.incident_id,
        "AI_ANALYSIS_FINISHED",
        content={
            "ok": result.ok,
            "mocked": result.mocked,
            "engine": (result.data or {}).get("engine"),
            "confidence": (result.data or {}).get("confidence"),
            "error": result.error,
            "duration_ms": result.duration_ms,
        },
    )
    inc("aiops_ai_analysis_total", {"ok": str(result.ok).lower()})
    if result.duration_ms:
        inc("aiops_ai_analysis_duration_seconds", value=result.duration_ms / 1000)

    if notify:
        try:
            send_incident_card(session, incident, kind)
        except Exception as exc:  # 推送失败不能影响告警与工单状态
            log(logger, logging.ERROR, "incident_card_failed", incident_id=incident.incident_id, error=str(exc))
    log(
        logger,
        logging.INFO,
        "incident_analyzed",
        incident_id=incident.incident_id,
        alerts=incident.alert_count,
        notified=notify,
        kind=kind,
        ai_ok=result.ok,
    )
    return result


def analyze_inline(session: Session, incident: Incident, kind: str) -> DiagnosisResult:
    """同步跑一次「采集 + 诊断」（AIOPS_INLINE_ANALYSIS 回退模式与手动重跑共用）。

    手动重跑传 kind="manual_reanalyze"（不在 CARD_KINDS 里）→ 不会推卡片。
    """
    return _analyze_and_notify(session, incident, notify=kind in CARD_KINDS, kind=kind)


# ----------------------------------------------------------------------
# 单条事件处理
# ----------------------------------------------------------------------
def _insert_raw_event(
    session: Session,
    payload: dict,
    event_id: str,
    dedup_key: str,
    source: str,
    raw_cache: dict[str, object] | None = None,
) -> tuple[RawEvent | None, bool]:
    """先判重再落盘：重复事件不写归档文件。返回 (raw_event, created)。

    raw_cache 用于「同一事件在重试期间只落一次归档」：归档写在文件里无法回滚，
    重试时若再写一遍，JSONL 里会留下重复行（2026-09-14 一次失败反复重试留了 4 行）。
    """
    if settings.debug_skip_dedup:
        # 调试开关：同一份 body 允许反复处理，便于联调重复触发整条链路。
        # 生产必须关闭（否则 HTTP 重试会重复建工单），/readyz 会暴露该状态。
        event_id = f"{event_id}-dbg{int(time.time() * 1000) % 10_000_000}"
        log(logger, logging.WARNING, "debug_dedup_skipped", event_id=event_id)
    elif session.scalar(select(RawEvent.event_id).where(RawEvent.event_id == event_id)) is not None:
        return None, False

    record = (raw_cache or {}).get(event_id)
    if record is None:
        record = raw_store.persist(payload, source=source)
        if raw_cache is not None:
            raw_cache[event_id] = record
    raw = RawEvent(
        event_id=event_id,
        source=source,
        dedup_key=dedup_key,
        payload_file=record.file,
        payload_offset=record.offset,
        payload=payload,
        processing_status="RECEIVED",
    )
    session.add(raw)
    session.flush()
    return raw, True


def _dispatch_alert_notify(session: Session, alert: Alert, kind: str) -> None:
    """按 kind 分派到飞书事件通道的通知实现（两处调用共用）。"""
    if kind == "alert_resolved":
        notify_alert_resolved(session, alert)
    else:
        notify_alert_created(session, alert)


def _notify_alert(session: Session, alert: Alert, kind: str) -> None:
    """Alert 粒度通知（事件通道）。默认交给后台，避免飞书网络调用拖慢 webhook。"""
    if settings.inline_analysis:
        _dispatch_alert_notify(session, alert, kind)
        return
    worker.request_alert_notification(alert.alert_id, kind)


def already_notified(
    session: Session, kind: str, *, incident_id: str | None = None, alert_id: str | None = None
) -> bool:
    """飞书推送幂等：同一目标同一 kind 已成功推过就不再推。

    必须有这层硬保证：后台重试、进程重启后的启动补偿、同一条告警的重放，
    都可能把同一张卡片推第二次（实测踩过：夜莺重试期间一次告警发出多张卡）。
    """
    query = select(FeishuMessage.id).where(FeishuMessage.kind == kind, FeishuMessage.ok.is_(True))
    if incident_id:
        query = query.where(FeishuMessage.incident_id == incident_id)
    if alert_id:
        query = query.where(FeishuMessage.alert_id == alert_id)
    return session.scalar(query.limit(1)) is not None


# 每日封顶不适用的 kind：恢复卡是终态、每单只可能一次，压掉会让人不知道故障已经好了
_DAILY_CAP_EXEMPT = {"incident_resolved"}


def sent_today(session: Session, incident_id: str, now: datetime | None = None) -> bool:
    """今天（本地自然日）是否已经为这张工单推过任何工单卡片。

    「每天只发一次」的实现点。按 incident_id 而不是 kind 计数：
    同一张工单当天的创建卡 + 根因更新卡 + 复发卡加起来也只发第一张。
    """
    day_start = local_day_start(now)
    return (
        session.scalar(
            select(FeishuMessage.id)
            .where(
                FeishuMessage.channel == "incident",
                FeishuMessage.incident_id == incident_id,
                FeishuMessage.ok.is_(True),
                FeishuMessage.kind.not_in(_DAILY_CAP_EXEMPT),
                FeishuMessage.created_at >= day_start,
            )
            .limit(1)
        )
        is not None
    )


def schedule_analysis(session: Session, incident: Incident, kind: str) -> None:
    """排入「采集 + 诊断 + 推卡」。默认交给后台，webhook 随即返回。

    AIOPS_INLINE_ANALYSIS=true 时同步执行（对照/回滚用），且同样遵守 CARD_KINDS ——
    否则一开回滚开关，噪音控制策略就跟着变了。
    """
    if settings.inline_analysis:
        analyze_inline(session, incident, kind)
        return
    incident.analysis_status = "PENDING"
    incident.analysis_kind = kind
    incident.analysis_attempts = 0
    incident.analysis_error = None
    incident.analysis_updated_at = utcnow()
    session.flush()
    worker.request_analysis(incident.incident_id, kind)
    log(logger, logging.INFO, "analysis_deferred", incident_id=incident.incident_id, kind=kind)


def _is_transient(exc: Exception) -> bool:
    """瞬时错误（数据库锁、网络超时）→ 可重试，不该定格为 FAILED。"""
    return isinstance(exc, OperationalError) or any(
        token in str(exc).lower() for token in ("database is locked", "timeout", "timed out")
    )


def analyze_incident_now(incident_id: str, kind: str) -> None:
    """后台任务入口：采集上下文 + AI 诊断 + 推卡片。

    不持 PROCESS_LOCK（慢的都是网络 I/O，见 worker 模块说明）。
    session 用 WorkerSessionLocal（AUTOCOMMIT）：后台跨 HTTP 的长事务会持有读快照，
    回写时与 webhook 抢写锁会直接报 database is locked（实测踩到）。
    analysis_status 记录进度：失败落 analysis_error，重启后由 requeue_pending 续跑。
    """
    session = WorkerSessionLocal()
    try:
        incident = queries.get_incident(session, incident_id)
        if incident is None:
            return
        # 只有这三个时机推卡片（与一期噪音控制一致）；manual_reanalyze 等不推
        notify = kind in CARD_KINDS
        incident.analysis_status = "RUNNING"
        incident.analysis_kind = kind
        incident.analysis_attempts = (incident.analysis_attempts or 0) + 1
        incident.analysis_updated_at = utcnow()
        session.commit()
        try:
            _analyze_and_notify(session, incident, notify=notify, kind=kind)
            incident.analysis_status = "DONE"
            incident.analysis_error = None
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            # 瞬时错误（数据库锁、网络超时）保持 PENDING 让 sweeper 续跑：
            # FAILED 不自动重试，一次抢锁就把诊断永久定格在那里，代价太大。
            incident.analysis_status = "PENDING" if _is_transient(exc) else "FAILED"
            incident.analysis_error = error
            level = logging.WARNING if incident.analysis_status == "PENDING" else logging.ERROR
            log(logger, level, "analysis_failed", incident_id=incident_id, retryable=incident.analysis_status == "PENDING", error=error)
        incident.analysis_updated_at = utcnow()
        session.commit()
    finally:
        session.close()


def notify_alert_now(alert_id: str, kind: str) -> None:
    """后台任务入口：Alert 粒度通知（事件通道）。

    用 AUTOCOMMIT 会话：读 Alert 后要打飞书（网络 8s 超时），事务型会话会把读快照
    一直持到飞书返回，回写时与 webhook 抢写锁会直接报 database is locked。
    """
    session = WorkerSessionLocal()
    try:
        alert = session.scalar(select(Alert).where(Alert.alert_id == alert_id))
        if alert is None:
            return
        if already_notified(session, kind, alert_id=alert_id):
            log(logger, logging.INFO, "alert_notify_skipped_already_sent", alert_id=alert_id, kind=kind)
            return
        _dispatch_alert_notify(session, alert, kind)
        session.flush()
    except Exception as exc:
        log(
            logger,
            logging.ERROR,
            "alert_notify_failed",
            alert_id=alert_id,
            error=f"{type(exc).__name__}: {exc}",
        )
    finally:
        session.close()


def _handle_firing(session: Session, norm, fingerprint: str) -> EventResult:
    debug_fresh = settings.debug_skip_dedup
    if debug_fresh:
        # 调试模式要连 Alert 指纹去重一起绕开，否则重复测试只会得到 ALERT_UPDATED
        reset = alert_service.reset_active_firing(session, fingerprint)
        if reset:
            log(logger, logging.WARNING, "debug_alert_reset", fingerprint=fingerprint, count=reset)
    alert, created = alert_service.upsert_firing(session, norm, fingerprint)
    result = EventResult(event_id=norm.event_id, source=norm.source, alert_id=alert.alert_id, alert_created=created)

    if not created:
        # 同一故障的重复触发：只累加计数，不重复建工单、不重复推卡片
        inc("aiops_alerts_updated_total")
        result.action = "ALERT_UPDATED"
        if alert.incident_id:
            incident = queries.get_incident(session, alert.incident_id)
            if incident is not None:
                incident_service.refresh_state(session, incident)
                result.incident_id = incident.incident_id
        return result

    inc("aiops_alerts_created_total")

    if debug_fresh:
        # 调试模式：跳过关联与合并，让每次请求都完整走一遍
        # 「建单 → 采集上下文 → AI 诊断 → 推卡片」，便于反复联调。
        incident = incident_service.create_incident(session, alert, LinkDecision(action="DEBUG_NEW_INCIDENT"))
        session.flush()
        result.incident_id = incident.incident_id
        result.action = "DEBUG_NEW_INCIDENT"
        _notify_alert(session, alert, "alert_created")
        schedule_analysis(session, incident, "incident_created")
        return result

    decision = CorrelationEngine(session).correlate(alert)
    previous_root = None
    if decision.incident_id:
        incident = queries.get_incident(session, decision.incident_id)
        if incident is None:
            raise RuntimeError(f"关联到不存在的工单: {decision.incident_id}")
        previous_root = incident.root_alert_id
        incident_service.attach_alert(session, incident, alert, decision)
        inc("aiops_correlation_total", {"action": decision.action})
    else:
        # 关联引擎没找到候选，再按 fingerprint 兜一层：同类故障（同规则+同资源）
        # 复发时挂回原来那张工单，必要时复开。
        # 没有这一层的后果实测过：一场 4 天没恢复的 master NotReady 因为中途被
        # 巡检判过恢复，每次复发都新建一张单，最后 9 张标题完全一样的 OPEN 工单。
        recurring = queries.incident_by_fingerprint(
            session, fingerprint, utcnow() - timedelta(hours=settings.incident_recurrence_hours)
        )
        if recurring is not None:
            incident = recurring
            incident_service.attach_alert(
                session, incident, alert, decision, relation="RECURRENCE"
            )
            inc("aiops_correlation_total", {"action": "RECURRENCE"})
            result.action = "RECURRENCE"
            log(
                logger,
                logging.INFO,
                "incident_recurrence_attached",
                incident_id=incident.incident_id,
                alert_id=alert.alert_id,
                fingerprint=fingerprint,
            )
        else:
            incident = incident_service.create_incident(session, alert, decision)
            inc("aiops_correlation_total", {"action": "NEW_INCIDENT"})

    session.flush()
    merged_into = incident_service.reconcile_incidents(session, incident)
    if merged_into:
        merged = queries.get_incident(session, merged_into)
        if merged is not None:
            incident = merged
        inc("aiops_incidents_merged_total")

    result.incident_id = incident.incident_id
    result.action = decision.action
    # 事件通道按 Alert 粒度：每新建一个 Alert 都通知
    _notify_alert(session, alert, "alert_created")

    # 挂到已有工单时不再重跑采集/AI（一场 N 条告警的风暴会白跑 N-1 次，
    # 还会把 Prometheus 查询放大成 O(N²)）；只有根因被提升时才补一次并补卡片。
    root_changed = previous_root is not None and incident.root_alert_id != previous_root
    if decision.incident_id and not root_changed:
        session.flush()
        return result
    schedule_analysis(session, incident, "incident_root_changed" if root_changed else "incident_created")
    return result


def _handle_resolved(session: Session, norm, fingerprint: str) -> EventResult:
    result = EventResult(event_id=norm.event_id, source=norm.source)
    alert = alert_service.mark_resolved(session, fingerprint, norm.occurred_at)
    if alert is None:
        result.action = "RESOLVED_WITHOUT_ACTIVE_ALERT"
        return result

    result.alert_id = alert.alert_id
    result.action = "ALERT_RESOLVED"
    inc("aiops_alerts_resolved_total")
    _notify_alert(session, alert, "alert_resolved")
    if alert.incident_id:
        incident = queries.get_incident(session, alert.incident_id)
        if incident is not None:
            state = incident_service.refresh_state(session, incident)
            result.incident_id = incident.incident_id
            result.action = f"ALERT_RESOLVED:{state}"
    return result


def process_one(session: Session, payload: dict, raw_cache: dict[str, object] | None = None) -> EventResult:
    source = str(payload.get("source") or "nightingale")
    dedup_key = normalizer.build_dedup_key(payload)
    event_id = normalizer.derive_event_id(payload, dedup_key)

    raw, created = _insert_raw_event(session, payload, event_id, dedup_key, source, raw_cache)
    if not created:
        inc("aiops_events_duplicate_total")
        log(logger, logging.INFO, "duplicate_event_ignored", event_id=event_id)
        return EventResult(event_id=event_id, status="DUPLICATE", source=source)

    inc("aiops_events_received_total", {"source": source})
    if raw is None:
        raise RuntimeError(f"raw event 落库异常: {event_id}")

    try:
        norm = normalizer.normalize(payload)
        raw.occurred_at = norm.occurred_at
    except normalizer.NormalizeError as exc:
        raw.processing_status = "FAILED"
        raw.processing_error = str(exc)
        inc("aiops_events_failed_total", {"reason": "normalize"})
        log(logger, logging.ERROR, "normalize_failed", event_id=event_id, error=str(exc))
        return EventResult(event_id=event_id, status="FAILED", source=source, error=str(exc))

    try:
        norm = enrich(session, norm)
    except Exception as exc:  # 富化失败绝不能丢告警
        norm.enrichment_status = "FAILED"
        norm.enrichment = {"error": str(exc)}
        inc("aiops_enrichment_failed_total")
        log(logger, logging.ERROR, "enrichment_exception", event_id=event_id, error=str(exc))

    fingerprint = fingerprint_service.build(norm)
    norm.fingerprint = fingerprint
    raw.normalized = norm.model_dump(mode="json")

    if norm.status == "FIRING":
        result = _handle_firing(session, norm, fingerprint)
    else:
        result = _handle_resolved(session, norm, fingerprint)

    raw.processing_status = "PROCESSED"
    raw.processing_error = None
    return result


def process_items(session: Session, items: list[dict]) -> list[EventResult]:
    """逐条处理，**每条一个独立事务**。

    为什么不是「整批一个事务 + savepoint 重试」（2026-09-14 丢告警后重做）：
      · SQLite 在「事务里先读过、之后才写」时，若期间别的连接提交过写，会**立刻**报
        database is locked（SQLITE_BUSY_SNAPSHOT，busy_timeout 根本不参与）；
      · savepoint 回滚**不会换快照**，所以在外层事务里重试必然继续失败
        （实测：同一请求 4 次重试全败，间隔正好等于我们的退避 50/100/150ms）；
      · 只有回滚最外层事务、重开一个事务，重试才可能成功。
    因此这里：每条独立提交（单条失败不会牵连已成功的条目）+ 失败回滚重开重试。
    """
    results: list[EventResult] = []
    with PROCESS_LOCK:
        for item in items:
            results.append(_process_one_with_retry(session, item))
    return results


def _process_one_with_retry(session: Session, item: dict, attempts: int = 4) -> EventResult:
    source = str(item.get("source") or "nightingale")
    dedup_key = normalizer.build_dedup_key(item)
    event_id = normalizer.derive_event_id(item, dedup_key)
    raw_cache: dict[str, object] = {}  # 同一事件重试期间只落一次归档，避免 JSONL 重复行

    for attempt in range(attempts):
        try:
            # 事务一开始就拿写锁（BEGIN IMMEDIATE）：冲突表现为「排队等待」（busy_timeout 生效），
            # 而不是读到一半快照失效、写的时候立刻失败。
            session.execute(text("BEGIN IMMEDIATE"))
            result = process_one(session, item, raw_cache=raw_cache)
            session.commit()
            return result
        except OperationalError as exc:
            session.rollback()  # ← 关键：换新事务/新快照，重试才有意义
            locked = "database is locked" in str(exc).lower()
            if locked and attempt < attempts - 1:
                inc("aiops_events_locked_retry_total")
                log(logger, logging.WARNING, "event_locked_retry", event_id=event_id, attempt=attempt + 1)
                time.sleep(0.1 * (attempt + 1))
                continue
            inc("aiops_events_failed_total", {"reason": "processing"})
            error = f"{type(exc).__name__}: {exc}"
            log(logger, logging.ERROR, "event_processing_failed", event_id=event_id, attempts=attempt + 1, error=error)
            return EventResult(event_id=event_id, status="FAILED", source=source, error=error)
        except Exception as exc:
            session.rollback()
            inc("aiops_events_failed_total", {"reason": "processing"})
            error = f"{type(exc).__name__}: {exc}"
            log(logger, logging.ERROR, "event_processing_failed", event_id=event_id, error=error)
            return EventResult(event_id=event_id, status="FAILED", source=source, error=error)

    return EventResult(event_id=event_id, status="FAILED", source=source, error="retry_exhausted")


def stats(session: Session) -> dict:
    counts = queries.status_counts(session)
    alerts = counts["alerts"]
    return {
        **counts,
        "compression_ratio": round(1 - (counts["incidents"] / alerts), 4) if alerts else 0.0,
    }
