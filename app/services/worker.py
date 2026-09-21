"""副作用出锁：webhook 只做 DB 短事务，采集/诊断/推卡在后台单线程执行。

模块职责（触发方式）：整链同步执行时，AI 诊断 10~13 秒也算在 HTTP 请求里，
夜莺客户端超时更短 → 报 `context deadline exceeded` 并按 3 次重试投递；
同时 PROCESS_LOCK 把 LLM 调用也锁在里面 → 告警风暴时告警串行排队。

设计约束（单副本 + SQLite）：
  · **后台任务不持 PROCESS_LOCK**：慢的全是网络 I/O（Prometheus/K8s/LLM/飞书），
    持锁会把 webhook 的 DB 段一起堵住。锁只保证 webhook 侧「读-判-写」串行。
  · 后台用 AUTOCOMMIT 会话（见 db/session.py）：跨 HTTP 的长事务会持有读快照，
    回写时与 webhook 抢写锁会直接报 database is locked（busy_timeout 不生效）。
  · 单线程 FIFO：事件通道通知先于工单分析提交，保持原有时序。
  · 同一工单同时只跑一个分析；期间来的新请求记成「最新待跑 kind」，跑完补一次
    （否则人工 /analyze、根因提升会被静默吞掉）。
  · 进程重启不丢在途分析：工单上留 PENDING/RUNNING，启动时 requeue。
"""
from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import select

from app.db.models import Alert, Incident
from app.db.session import SessionLocal
from app.logging_setup import get_logger, log

logger = get_logger("app.services.worker")

DEFAULT_KIND = "incident_created"
# 需要在启动/巡检时补偿的状态（RUNNING 说明上次被重启打断）
RESUMABLE_STATUSES = ("PENDING", "RUNNING")

# 等 webhook 事务提交的窗口：后台任务可能比 webhook 的 commit 更早开始，
# 此时读到的是旧状态（未提交），会导致分析结果被回写覆盖或读不到行。
_COMMIT_WAIT_SECONDS = 5.0
_POLL_INTERVAL = 0.05
_POLL_MAX_INTERVAL = 0.4

_EXECUTOR: ThreadPoolExecutor | None = None
_EXECUTOR_LOCK = threading.Lock()

# incident_id → 正在跑的 kind；incident_id → 最新待跑的 kind（覆盖式，latest wins）
_INFLIGHT: dict[str, str] = {}
_PENDING: dict[str, str] = {}
_STATE_LOCK = threading.Lock()


def _submit(fn, *args) -> bool:
    """提交任务；executor 惰性创建（进程内 lifespan 可重入，别在模块导入时就 shutdown 掉）。"""
    global _EXECUTOR
    with _EXECUTOR_LOCK:
        if _EXECUTOR is None:
            _EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="aiops-worker")
        executor = _EXECUTOR
    try:
        executor.submit(fn, *args)
        return True
    except RuntimeError as exc:  # 已 shutdown
        log(logger, logging.ERROR, "worker_submit_rejected", error=str(exc))
        return False


def _wait_for(kind_of, timeout: float = _COMMIT_WAIT_SECONDS) -> str | None:
    """等 webhook 事务提交：轮询到目标行可见且状态可处理为止。

    单个 session 复用（早期版本每 50ms 新建一个 session，最坏 100 次连接/事务）；
    指数退避到 0.4s，够用且不烧 CPU。
    """
    deadline = time.time() + timeout
    interval = _POLL_INTERVAL
    session = SessionLocal()
    try:
        while True:
            session.rollback()  # 结束上一轮读事务，避免持快照
            value = kind_of(session)
            if value is not None:
                return value
            if time.time() > deadline:
                return None
            time.sleep(interval)
            interval = min(interval * 2, _POLL_MAX_INTERVAL)
    finally:
        session.close()


def _ready_kind(session, incident_id: str) -> str | None:
    row = session.execute(
        select(Incident.analysis_status, Incident.analysis_kind).where(Incident.incident_id == incident_id)
    ).first()
    if row is None:
        return None
    status, kind = row[0], row[1]
    if status in RESUMABLE_STATUSES or status == "DONE":
        return kind or DEFAULT_KIND
    return None


def request_analysis(incident_id: str, kind: str) -> None:
    """排入后台分析任务（同一工单同时只跑一个，期间的请求合并成「最新 kind」）。"""
    with _STATE_LOCK:
        if incident_id in _INFLIGHT:
            # 不能直接丢：否则人工 /analyze、根因提升会被静默吞掉且状态显示已完成
            _PENDING[incident_id] = kind
            log(logger, logging.INFO, "analysis_queued_behind_running", incident_id=incident_id, kind=kind)
            return
        _INFLIGHT[incident_id] = kind
    if not _submit(_analyze_job, incident_id, kind):
        with _STATE_LOCK:
            _INFLIGHT.pop(incident_id, None)


def request_alert_notification(alert_id: str, kind: str) -> None:
    """Alert 粒度的通知（事件通道）：每条 Alert 都要发，不按工单去重。"""
    _submit(_alert_notify_job, alert_id, kind)


def _drain_pending(incident_id: str, ran_kind: str) -> None:
    """跑完后补跑期间累积的最新 kind。"""
    with _STATE_LOCK:
        _INFLIGHT.pop(incident_id, None)
        next_kind = _PENDING.pop(incident_id, None)
    if next_kind and next_kind != ran_kind:
        log(logger, logging.INFO, "analysis_rerun_with_latest_kind", incident_id=incident_id, kind=next_kind)
        request_analysis(incident_id, next_kind)


def _analyze_job(incident_id: str, kind: str) -> None:
    try:
        from app.services.pipeline import analyze_incident_now

        # 等 webhook 提交；等不到也照跑（analyze_incident_now 找不到行会安全返回）。
        # 早期版本在超时时直接丢弃任务，让诊断白等最多 5 分钟。
        ready_kind = _wait_for(lambda session: _ready_kind(session, incident_id))
        if ready_kind == "DONE":
            return  # 没有待处理状态，说明已被其它路径处理完
        # 以**请求的 kind** 为准：卡片类型跟着本次请求走，
        # 用 DB 里的旧 kind 会把「根因更新」推成「新工单」。
        analyze_incident_now(incident_id, kind)
    except Exception as exc:  # 后台任务绝不能把异常抛出去静默丢失
        log(
            logger,
            logging.ERROR,
            "worker_analysis_crashed",
            incident_id=incident_id,
            error=f"{type(exc).__name__}: {exc}",
        )
    finally:
        _drain_pending(incident_id, kind)


def _alert_notify_job(alert_id: str, kind: str) -> None:
    try:
        from app.services.pipeline import notify_alert_now

        if _wait_for(lambda session: _alert_exists(session, alert_id)) is None:
            return
        notify_alert_now(alert_id, kind)
    except Exception as exc:
        log(
            logger,
            logging.ERROR,
            "worker_alert_notify_crashed",
            alert_id=alert_id,
            error=f"{type(exc).__name__}: {exc}",
        )


def _alert_exists(session, alert_id: str) -> str | None:
    return alert_id if session.scalar(select(Alert.alert_id).where(Alert.alert_id == alert_id)) else None


def requeue_pending() -> int:
    """启动时把在途分析重新入队（否则重启期间的告警会停在「只有工单、没有诊断」）。"""
    session = SessionLocal()
    try:
        rows = session.execute(
            select(Incident.incident_id, Incident.analysis_kind).where(
                Incident.analysis_status.in_(RESUMABLE_STATUSES)
            )
        ).all()
    finally:
        session.close()
    for incident_id, kind in rows:
        request_analysis(incident_id, kind or DEFAULT_KIND)
    if rows:
        log(logger, logging.WARNING, "analysis_requeued_on_startup", count=len(rows))
    return len(rows)


def queue_depth() -> dict[str, int]:
    """队列可观测性：正在跑 + 待补跑。"""
    with _STATE_LOCK:
        return {"inflight": len(_INFLIGHT), "pending": len(_PENDING)}


def shutdown(wait: bool = False) -> None:
    """关服：不等在途任务（LLM 可能要 10 余秒），靠下次启动的 requeue_pending 续跑。"""
    global _EXECUTOR
    with _EXECUTOR_LOCK:
        executor, _EXECUTOR = _EXECUTOR, None
    if executor is not None:
        executor.shutdown(wait=wait, cancel_futures=not wait)
