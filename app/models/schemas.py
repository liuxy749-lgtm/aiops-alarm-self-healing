"""标准事件模型（Pydantic）。

夜莺侧会做格式化，所以这里既是「接收契约」，也是内部统一模型。
校验失败必须报错，不能静默写入脏数据。
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.timeutil import ensure_utc, utcnow

STATUS_FIRING = "FIRING"
STATUS_RESOLVED = "RESOLVED"

_FIRING_ALIASES = {"firing", "alerting", "active", "triggered", "problem", "fire", "异常", "触发"}
_RESOLVED_ALIASES = {"resolved", "resolve", "fixed", "ok", "recovered", "closed", "恢复", "已恢复"}


def normalize_status(raw: str | None) -> str:
    if raw is None:
        raise ValueError("status 缺失")
    value = str(raw).strip().lower()
    if value in _FIRING_ALIASES:
        return STATUS_FIRING
    if value in _RESOLVED_ALIASES:
        return STATUS_RESOLVED
    raise ValueError(f"无法识别的 status: {raw!r}（应为 firing / resolved）")


def _ensure_aware(value: datetime | None) -> datetime | None:
    return ensure_utc(value)


def coerce_kv_map(value: Any) -> dict[str, Any]:
    """把夜莺的各种「标签」形态统一成 dict。

    夜莺 AlertCurEvent 里同一个东西有好几种表示，都出现过：
      tags          = ["k=v", "k2=v2"]   （TagsJSON []string，json tag 是 tags）
      tags_map      = {"k": "v"}         （TagsMap）
      original_tags = ["k=v", ...]
      annotations   = {"summary": "..."}（或同款数组形态）
    只按 dict 声明的话，`tags` 那一条会让整个 payload 校验失败（线上曾出现 422）。
    """
    if value is None:
        return {}
    if isinstance(value, dict):
        return {str(key): val for key, val in value.items()}
    if isinstance(value, (list, tuple)):
        result: dict[str, Any] = {}
        for item in value:
            if isinstance(item, str) and "=" in item:
                key, _, val = item.partition("=")
                if key.strip():
                    result[key.strip()] = val.strip()
        return result
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return {}
        if text.startswith("{"):
            try:
                return coerce_kv_map(json.loads(text))
            except ValueError:
                return {}
        result = {}
        for pair in text.split(","):
            if "=" in pair:
                key, _, val = pair.partition("=")
                if key.strip():
                    result[key.strip()] = val.strip()
        return result
    return {}


class Entity(BaseModel):
    model_config = ConfigDict(extra="allow")

    type: str = "unknown"
    id: str
    ip: str | None = None
    hostname: str | None = None
    namespace: str | None = None
    node: str | None = None


class Scope(BaseModel):
    model_config = ConfigDict(extra="allow")

    cluster: str | None = None
    region: str | None = None
    zone: str | None = None
    projset: str | None = None
    project: str | None = None
    namespace: str | None = None
    node: str | None = None


class Hardware(BaseModel):
    model_config = ConfigDict(extra="allow")

    accelerator_model: str | None = None
    accelerator_count: int | None = None


class NormalizedEvent(BaseModel):
    """系统内部统一事件模型。"""

    model_config = ConfigDict(extra="allow")

    event_id: str
    source: str = "nightingale"
    schema_version: str = "1"

    event_type: str
    status: Literal["FIRING", "RESOLVED"] = STATUS_FIRING
    severity: str | None = None
    occurred_at: datetime

    entity: Entity
    scope: Scope = Field(default_factory=Scope)
    hardware: Hardware = Field(default_factory=Hardware)

    value: str | None = None
    summary: str | None = None
    description: str | None = None

    labels: dict[str, Any] = Field(default_factory=dict)
    annotations: dict[str, Any] = Field(default_factory=dict)

    enrichment_status: str = "PENDING"
    enrichment: dict[str, Any] = Field(default_factory=dict)

    fingerprint: str | None = None

    @field_validator("occurred_at")
    @classmethod
    def _aware(cls, value: datetime) -> datetime:
        return _ensure_aware(value) or utcnow()


class NightingalePayload(BaseModel):
    """夜莺 Webhook 的格式化 payload 契约。

    兼容两种形态：
      1) 夜莺侧已格式化的单事件（推荐，schema_version >= 1）
      2) 夜莺原生 payload（rule_name/tags/... 或 events 数组），由 normalizer 兜底映射
    额外字段一律保留（extra=allow），并原样进 raw_events 归档。
    """

    model_config = ConfigDict(extra="allow")

    schema_version: str | None = None

    event_id: str | None = None
    source: str | None = None

    # 已格式化字段
    alertname: str | None = None
    event_type: str | None = None
    # 注意：夜莺 AlertCurEvent 里也有个 `status`，但它是 `Status int` 的内部瞬时字段、
    # 不是告警状态（权威信号是 is_recovered）。所以要允许 int，且不能当状态用。
    status: str | int | float | None = None
    severity: str | int | None = None
    occurred_at: datetime | str | None = None

    entity: dict[str, Any] | None = None
    scope: dict[str, Any] | None = None
    hardware: dict[str, Any] | None = None

    entity_type: str | None = None
    entity_id: str | None = None
    ip: str | None = None
    hostname: str | None = None
    node: str | None = None
    namespace: str | None = None
    cluster: str | None = None
    region: str | None = None
    zone: str | None = None
    projset: str | None = None
    project: str | None = None
    accelerator_model: str | None = None

    value: str | float | int | None = None
    summary: str | None = None
    description: str | None = None

    labels: dict[str, Any] = Field(default_factory=dict)
    tags: dict[str, Any] = Field(default_factory=dict)
    annotations: dict[str, Any] = Field(default_factory=dict)

    @field_validator("labels", "tags", "tags_map", "annotations", mode="before")
    @classmethod
    def _coerce_maps(cls, value: Any) -> dict[str, Any]:
        return coerce_kv_map(value)

    # ---- 夜莺原生 Webhook（全局 Webhook / 告警规则回调）字段 ----
    # 夜莺 v8 的 Webhook 与「告警回调」直接把 AlertCurEvent 的 JSON POST 过来，
    # 字段名是下面这套（tags_map / is_recovered / target_ident / trigger_value）。
    tags_map: dict[str, Any] = Field(default_factory=dict)
    is_recovered: bool | None = None
    target_ident: str | None = None
    target_note: str | None = None
    trigger_value: str | float | int | None = None
    cate: str | None = None
    rule_prod: str | None = None

    # 夜莺原生字段（兼容用）
    rule_name: str | None = None
    rule_id: str | int | None = None
    hash: str | None = None
    events: list[dict[str, Any]] | None = None
    trigger_time: datetime | str | None = None
