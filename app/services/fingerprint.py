"""Fingerprint：判断「是不是同一个 Alert」。

不含 value / timestamp / 任何会随时间变化的字段，否则每次告警都是新指纹。
"""
from __future__ import annotations

import hashlib

from app.correlation.rules import get_rules
from app.models.schemas import NormalizedEvent

FINGERPRINT_VERSION = "v1"


def build(norm: NormalizedEvent) -> str:
    rules = get_rules()
    dimensions = rules.fingerprint_dimensions_for(norm.entity.type)

    parts: list[str] = [
        FINGERPRINT_VERSION,
        norm.source,
        norm.event_type,
        norm.scope.cluster or "-",
        norm.entity.type,
        norm.entity.id,
    ]
    for key in dimensions:
        value = norm.labels.get(key)
        parts.append(f"{key}={value if value is not None else '-'}")

    return hashlib.sha256("|".join(parts).encode()).hexdigest()
