"""时间口径：内部一律 UTC-aware，只在给人看的地方转本地时区。

不统一会导致两个实际问题：naive/aware 混用直接抛异常；
以及展示时忘记转换，北京时间少 8 小时。
"""
from __future__ import annotations

from datetime import datetime, timezone
from functools import lru_cache
from zoneinfo import ZoneInfo

from app.config import settings


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def ensure_utc(value: datetime | None) -> datetime | None:
    """补齐/换算成 UTC-aware，None 原样返回。"""
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


@lru_cache(maxsize=1)
def display_zone() -> ZoneInfo:
    try:
        return ZoneInfo(settings.display_timezone)
    except Exception:  # 容器里没有 tzdata 时退回系统本地时区
        return datetime.now(timezone.utc).astimezone().tzinfo  # type: ignore[return-value]


def to_local(value: datetime | None) -> datetime | None:
    converted = ensure_utc(value)
    return converted.astimezone(display_zone()) if converted else None


def local_iso(value: datetime | None) -> str | None:
    converted = to_local(value)
    return converted.isoformat() if converted else None


def local_hm(value: datetime | None) -> str | None:
    converted = to_local(value)
    return converted.strftime("%H:%M:%S") if converted else None


def local_display(value: datetime | None) -> str | None:
    """给人看的时间：月-日 时:分:秒（去掉微秒）。"""
    converted = to_local(value)
    return converted.strftime("%m-%d %H:%M:%S") if converted else None


def local_date(value: datetime | None) -> str | None:
    """本地时区的日期（YYYY-MM-DD）。「每天只发一次」的天界必须按本地日算，
    按 UTC 算会让北京时间 08:00 之前算成前一天。"""
    converted = to_local(value)
    return converted.strftime("%Y-%m-%d") if converted else None


def local_day_start(value: datetime | None = None) -> datetime:
    """所在本地自然日的 00:00，返回 UTC-aware（用于 DB 范围比较）。"""
    converted = to_local(value) or to_local(utcnow())
    assert converted is not None
    return converted.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)


def local_hour_today(hour: int, value: datetime | None = None) -> datetime:
    """所在本地自然日的指定整点，返回 UTC-aware。"""
    converted = to_local(value) or to_local(utcnow())
    assert converted is not None
    return converted.replace(hour=hour, minute=0, second=0, microsecond=0).astimezone(timezone.utc)
