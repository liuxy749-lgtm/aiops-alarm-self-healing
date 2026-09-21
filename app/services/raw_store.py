"""raw event 本地 JSONL 归档。

按天一个文件，append-only；单行写完 flush + fsync（丢事件比写慢严重）。
DB 里只存文件路径 + 该行的起始字节偏移，正文留在归档文件里。
"""
from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from app.config import settings

_WRITE_LOCK = threading.Lock()


@dataclass
class RawRecord:
    file: str
    offset: int


def _day_file(now: datetime) -> Path:
    return settings.raw_dir / f"raw-events-{now.strftime('%Y-%m-%d')}.jsonl"


def persist(payload: dict, source: str = "nightingale") -> RawRecord:
    now = datetime.now(timezone.utc)
    path = _day_file(now)
    line = json.dumps({"received_at": now.isoformat(), "source": source, "payload": payload}, ensure_ascii=False)
    with _WRITE_LOCK:
        with open(path, "a", encoding="utf-8") as handle:
            offset = handle.tell()
            handle.write(line + "\n")
            handle.flush()
            os.fsync(handle.fileno())
    return RawRecord(file=str(path), offset=offset)


def read_at(file: str, offset: int) -> dict | None:
    """按偏移读回原始 payload，用于复核。"""
    try:
        with open(file, "r", encoding="utf-8") as handle:
            handle.seek(offset)
            line = handle.readline()
        return json.loads(line)["payload"]
    except (OSError, ValueError, KeyError):
        return None


def purge_older_than(cutoff: datetime) -> int:
    """删除 cutoff 之前的归档文件，返回删除数量。"""
    if not settings.raw_dir.exists():
        return 0
    removed = 0
    for path in settings.raw_dir.glob("raw-events-*.jsonl"):
        try:
            stamp = datetime.strptime(path.stem.removeprefix("raw-events-"), "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            continue
        if stamp.date() < cutoff.date():
            try:
                path.unlink()
                removed += 1
            except OSError:
                continue
    return removed
