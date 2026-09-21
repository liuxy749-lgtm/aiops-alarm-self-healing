"""AIOps Event Gateway / Correlation / Incident Engine（一期）。

启动：systemctl start aiops-gateway（监听 0.0.0.0:8701，单副本）。
SQLite + 进程内锁，多副本会导致状态不一致与重复通知。
"""
from __future__ import annotations

import asyncio
import contextlib
from contextlib import asynccontextmanager

from fastapi import FastAPI
from starlette.concurrency import run_in_threadpool

from app.api.routes import router
from app.config import settings
from app.db.session import SessionLocal, init_db
from app.logging_setup import get_logger, setup_logging
from app.services import digest, worker
from app.services.sweeper import run_sweep_single_flight

logger = get_logger("app.main")

SWEEP_TASK: asyncio.Task | None = None


def _sweep_once() -> None:
    """同步巡检：提交后释放。必须在线程池里执行。

    这里**不再持 PROCESS_LOCK**：巡检里有 Prometheus 与飞书的同步网络调用，
    持锁会把 webhook 的入库一起堵住（详见 sweeper._resolve_recovered）。
    并发保护改由 sweeper.run_sweep_single_flight 的单飞锁负责。
    """
    session = SessionLocal()
    try:
        summary = run_sweep_single_flight(session)
        session.commit()
        if summary is None:
            logger.info("sweep_skipped_already_running")
    except Exception as exc:
        session.rollback()
        raise
    finally:
        session.close()


def _digest_once() -> None:
    """每日汇总搭巡检的车。独立 session：汇总失败不能回滚掉巡检的恢复判定。"""
    session = SessionLocal()
    try:
        digest.run_if_due(session)
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


async def _sweep_loop() -> None:
    while True:
        await asyncio.sleep(settings.sweep_interval_seconds)
        try:
            # 巡检里有 Prometheus / 飞书的同步网络调用，放线程池，否则冻结事件循环
            await run_in_threadpool(_sweep_once)
        except Exception as exc:
            logger.error(f"sweep_failed: {exc}")
        # 汇总单独 try：它失败不该影响巡检，巡检失败也不该跳过它
        try:
            await run_in_threadpool(_digest_once)
        except Exception as exc:
            logger.error(f"daily_digest_failed: {exc}")


@asynccontextmanager
async def lifespan(_app: FastAPI):
    setup_logging()
    init_db()
    global SWEEP_TASK
    SWEEP_TASK = asyncio.create_task(_sweep_loop())
    # 重启补偿：上次进程被重启时停在 PENDING/RUNNING 的分析重新入队，
    # 否则这批工单会永远停在「有工单、没诊断」的状态且没人发现。
    requeued = worker.requeue_pending()
    logger.info(
        "aiops_gateway_started",
        extra={
            "extra_fields": {
                "db": str(settings.db_path),
                "raw_dir": str(settings.raw_dir),
                "sweep_interval": settings.sweep_interval_seconds,
                "alert_stale_seconds": settings.alert_stale_seconds,
                "recovery_observe_seconds": settings.recovery_observe_seconds,
                "rules_dir": str(settings.rules_dir),
                "raw_retention_days": settings.raw_retention_days,
                "inline_analysis": settings.inline_analysis,
                "analysis_requeued": requeued,
                "daily_card_cap": settings.daily_card_cap,
                "daily_digest_enabled": settings.daily_digest_enabled,
                "daily_digest_hour": settings.daily_digest_hour,
            }
        },
    )
    try:
        yield
    finally:
        if SWEEP_TASK is not None:
            SWEEP_TASK.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await SWEEP_TASK
        # 不等在途分析（LLM 可能还要十几秒），靠下次启动的 requeue_pending 续跑
        worker.shutdown(wait=False)


app = FastAPI(
    title="AIOps Phase 1 — Event Gateway / Correlation / Incident",
    version="0.1.0",
    description="告警事件中心、关联引擎与 AI 辅助诊断（MVP）",
    lifespan=lifespan,
)
app.include_router(router)


@app.get("/")
def root() -> dict:
    return {"service": "aiops-gateway", "phase": "1", "docs": "/docs", "webhook": "POST /api/v1/events/nightingale"}
