"""每日 09:00 的未恢复工单汇总。

为什么要有这个：工单卡改成「每天每单最多一张」之后，一场四天没恢复的故障
在第二天起就彻底安静了 —— 没人推，也就没人想起来它还挂着。
汇总卡把「还没好的都在这」每天早上说一次，是那条封顶规则的另一半。

触发靠巡检循环搭车（sweeper 每 30s 跑一次），不引入 cron/APScheduler：
判据是「今天本地日历日有没有发过 daily_digest」，所以进程重启、
或者 09:00 那一刻正好没在运行，都能在下一次巡检补发，不会漏也不会重。
"""
from __future__ import annotations

import logging
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import settings
from app.db import queries
from app.db.models import FeishuMessage
from app.integrations.feishu import notify_digest
from app.logging_setup import get_logger, log
from app.metrics import inc
from app.timeutil import local_day_start, to_local, utcnow

logger = get_logger("app.services.digest")

DIGEST_KIND = "daily_digest"


def already_sent_today(session: Session, now: datetime | None = None) -> bool:
    """今天是否已经发过汇总卡（本地自然日）。

    只认 ok=True：发失败的那次不算发过，下一次巡检会重试 —— 汇总卡失败
    没有别的补偿路径，如果按「尝试过」算，当天就永远丢了。
    """
    return (
        session.scalar(
            select(FeishuMessage.id)
            .where(
                FeishuMessage.channel == "incident",
                FeishuMessage.kind == DIGEST_KIND,
                FeishuMessage.ok.is_(True),
                FeishuMessage.created_at >= local_day_start(now),
            )
            .limit(1)
        )
        is not None
    )


def due(session: Session, now: datetime | None = None) -> bool:
    """到点了吗：开关开着、已过配置的小时、今天还没发过、且（默认）确实有未恢复工单。"""
    if not settings.daily_digest_enabled:
        return False
    moment = to_local(now or utcnow())
    if moment.hour < settings.daily_digest_hour:
        return False
    if already_sent_today(session, now):
        return False
    # 只在有事时汇报（用户 2026-09-15 要求：未恢复的次日早上九点报一次）。
    # 没有未恢复工单就不发，避免每天一张"平安卡"变成新的噪声；
    # 想每天固定报到（心跳）时把 AIOPS_DAILY_DIGEST_ALWAYS 打开。
    if not settings.daily_digest_always and not queries.unresolved_incidents(session):
        return False
    return True


def send_digest(session: Session, now: datetime | None = None) -> bool:
    """无条件发一张汇总卡（调用方负责判时机）。没有未恢复工单也发，报个平安。"""
    incidents = queries.unresolved_incidents(session)
    ok = notify_digest(session, incidents, now)
    # 汇总卡不挂在任何单张工单上，没有时间线可记 → 落库 + 指标就是它的全部痕迹
    session.commit()
    inc("aiops_daily_digest_total", {"ok": str(ok).lower()})
    log(
        logger,
        logging.INFO if ok else logging.ERROR,
        "daily_digest_sent" if ok else "daily_digest_failed",
        unresolved=len(incidents),
    )
    return ok


def run_if_due(session: Session, now: datetime | None = None) -> bool:
    """巡检循环的入口：到点就发，否则什么都不做。返回是否真的发了。"""
    if not due(session, now):
        return False
    return send_digest(session, now)
