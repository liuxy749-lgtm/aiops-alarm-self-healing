"""数据模型。

设计要点（与设计文档的差异都写在这里）：
- raw_events：落库的是元数据 + 完整原始 payload 副本，正文另外原子写入
  本地 JSONL 归档文件（一期不上对象存储）。
- alerts：用「部分唯一索引」保证同一 fingerprint 同时只存在一条 FIRING 记录，
  这是去重的硬约束，不依赖应用层的先查后建。
- incidents：多出 node / root_alert_id / recovery_deadline / merged_into，
  用于根因提升、恢复观察期与合并。
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Text,
    TypeDecorator,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.timeutil import ensure_utc, utcnow

__all__ = [
    "Alert",
    "FeishuMessage",
    "Incident",
    "IncidentAlert",
    "IncidentEvent",
    "LLMCall",
    "RawEvent",
    "ResourceRelation",
    "UTCDateTime",
    "utcnow",
]


class UTCDateTime(TypeDecorator):
    """SQLite 不保存时区，必须在这里统一口径。

    直接读写会导致：读回 naive 与内存 aware 比较报错；
    以及写库带 "+00:00"、读回裸字符串，做 `last_seen >= :since` 这类范围比较时
    字符串比较错乱 —— 候选工单会永远查不到，每条告警都新建工单。
    """

    impl = DateTime
    cache_ok = True

    def process_bind_param(self, value: datetime | None, dialect):  # noqa: ANN001
        converted = ensure_utc(value)
        return converted.replace(tzinfo=None) if converted else None

    def process_result_value(self, value: datetime | None, dialect):  # noqa: ANN001
        return ensure_utc(value)


class RawEvent(Base):
    """夜莺每次 Webhook 请求的原始留档。event_id 幂等：重发不会产生第二条。"""

    __tablename__ = "raw_events"

    id: Mapped[int] = mapped_column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True)
    event_id: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    dedup_key: Mapped[str | None] = mapped_column(String(128), index=True)
    occurred_at: Mapped[datetime | None] = mapped_column(UTCDateTime)

    # 本地 JSON 归档位置（原子写）：文件路径 + 该行在文件中的起始字节偏移
    payload_file: Mapped[str | None] = mapped_column(String(512))
    payload_offset: Mapped[int | None] = mapped_column(Integer)

    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    normalized: Mapped[dict | None] = mapped_column(JSON)

    processing_status: Mapped[str] = mapped_column(String(32), default="RECEIVED")
    processing_error: Mapped[str | None] = mapped_column(Text)

    received_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow)


class Alert(Base):
    """Alert Instance：同规则 + 同资源持续触发合并成一条。"""

    __tablename__ = "alerts"

    id: Mapped[int] = mapped_column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True)
    alert_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    fingerprint: Mapped[str] = mapped_column(String(128), nullable=False, index=True)

    alertname: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="FIRING")
    severity: Mapped[str | None] = mapped_column(String(16))

    entity_type: Mapped[str | None] = mapped_column(String(64))
    entity_id: Mapped[str | None] = mapped_column(String(255))
    ip: Mapped[str | None] = mapped_column(String(64))
    hostname: Mapped[str | None] = mapped_column(String(255))
    node: Mapped[str | None] = mapped_column(String(255), index=True)
    namespace: Mapped[str | None] = mapped_column(String(255))

    cluster: Mapped[str | None] = mapped_column(String(255), index=True)
    region: Mapped[str | None] = mapped_column(String(255))
    zone: Mapped[str | None] = mapped_column(String(255))
    projset: Mapped[str | None] = mapped_column(String(255))
    project: Mapped[str | None] = mapped_column(String(255))
    accelerator_model: Mapped[str | None] = mapped_column(String(255))

    value: Mapped[str | None] = mapped_column(String(255))
    summary: Mapped[str | None] = mapped_column(Text)

    first_seen: Mapped[datetime] = mapped_column(UTCDateTime, nullable=False)
    last_seen: Mapped[datetime] = mapped_column(UTCDateTime, nullable=False)
    resolved_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    resolution_reason: Mapped[str | None] = mapped_column(String(32))

    occurrence_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)

    incident_id: Mapped[str | None] = mapped_column(String(64), index=True)
    enrichment_status: Mapped[str | None] = mapped_column(String(32))

    labels: Mapped[dict | None] = mapped_column(JSON)
    annotations: Mapped[dict | None] = mapped_column(JSON)
    enrichment: Mapped[dict | None] = mapped_column(JSON)

    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow, onupdate=utcnow)

    __table_args__ = (
        # 硬约束：同一 fingerprint 只能有一条 FIRING。并发下靠它兜底。
        Index(
            "uq_alerts_active_fingerprint",
            "fingerprint",
            unique=True,
            sqlite_where=text("status = 'FIRING'"),
        ),
        Index("idx_alerts_entity", "entity_type", "entity_id"),
        Index("idx_alerts_last_seen", "last_seen"),
    )


class Incident(Base):
    __tablename__ = "incidents"

    id: Mapped[int] = mapped_column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True)
    incident_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)

    title: Mapped[str] = mapped_column(String(512), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="OPEN", index=True)
    severity: Mapped[str | None] = mapped_column(String(16), index=True)

    root_entity_type: Mapped[str | None] = mapped_column(String(64))
    root_entity_id: Mapped[str | None] = mapped_column(String(255))
    root_alert_id: Mapped[str | None] = mapped_column(String(64))
    node: Mapped[str | None] = mapped_column(String(255), index=True)
    cluster: Mapped[str | None] = mapped_column(String(255), index=True)

    suspected_root_cause: Mapped[str | None] = mapped_column(Text)
    root_cause_confidence: Mapped[float | None] = mapped_column(Float)
    ai_summary: Mapped[str | None] = mapped_column(Text)
    ai_diagnosis: Mapped[dict | None] = mapped_column(JSON)

    # --- 副作用出锁：采集/诊断/推送在后台执行，这几列记录进度 ---
    # 放数据库而非内存队列的理由：进程重启后要把在途工单重新入队，
    # 否则重启期间的告警会停在「只有工单、没有诊断」的状态且无人发现。
    analysis_status: Mapped[str] = mapped_column(String(16), default="NONE", index=True)  # NONE/PENDING/RUNNING/DONE/FAILED
    analysis_kind: Mapped[str | None] = mapped_column(String(32))
    analysis_attempts: Mapped[int] = mapped_column(Integer, default=0)
    analysis_error: Mapped[str | None] = mapped_column(Text)
    analysis_updated_at: Mapped[datetime | None] = mapped_column(UTCDateTime)

    first_seen: Mapped[datetime] = mapped_column(UTCDateTime, nullable=False)
    last_seen: Mapped[datetime] = mapped_column(UTCDateTime, nullable=False)
    acknowledged_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    resolved_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    recovery_deadline: Mapped[datetime | None] = mapped_column(UTCDateTime)
    # 主动恢复探测的痕迹：上次探测时间 + 上次结论（recovered/not_recovered/unverified），
    # 用于限流（同一工单按间隔探）与"结论变化时才记事件"（否则每 5 分钟刷一条时间线）
    last_probe_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    last_probe_result: Mapped[str | None] = mapped_column(String(32))

    alert_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    context: Mapped[dict | None] = mapped_column(JSON)
    merged_into: Mapped[str | None] = mapped_column(String(64))

    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow, onupdate=utcnow)

    __table_args__ = (
        # 候选工单检索就是按 (status, last_seen) 过滤+排序，不建索引会全表扫描
        Index("idx_incidents_status_last_seen", "status", "last_seen"),
        Index("idx_incidents_merged_into", "merged_into"),
    )


class IncidentAlert(Base):
    __tablename__ = "incident_alerts"

    id: Mapped[int] = mapped_column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True)
    incident_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    alert_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)

    relation_type: Mapped[str | None] = mapped_column(String(32))
    correlation_score: Mapped[int | None] = mapped_column(Integer)
    correlation_reason: Mapped[dict | None] = mapped_column(JSON)

    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow)

    __table_args__ = (UniqueConstraint("incident_id", "alert_id", name="uq_incident_alert"),)


class IncidentEvent(Base):
    """Incident Timeline。"""

    __tablename__ = "incident_events"

    id: Mapped[int] = mapped_column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True)
    incident_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    actor: Mapped[str | None] = mapped_column(String(128))
    content: Mapped[dict | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow)


class ResourceRelation(Base):
    """拓扑关系：SOURCE --RELATION--> TARGET。一期不用图数据库。"""

    __tablename__ = "resource_relations"

    id: Mapped[int] = mapped_column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True)
    source_type: Mapped[str] = mapped_column(String(64), nullable=False)
    source_id: Mapped[str] = mapped_column(String(255), nullable=False)
    relation: Mapped[str] = mapped_column(String(64), nullable=False)
    target_type: Mapped[str] = mapped_column(String(64), nullable=False)
    target_id: Mapped[str] = mapped_column(String(255), nullable=False)
    metadata_json: Mapped[dict | None] = mapped_column("metadata", JSON)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow, onupdate=utcnow)

    __table_args__ = (
        UniqueConstraint("source_type", "source_id", "relation", "target_type", "target_id", name="uq_relation"),
        Index("idx_relations_source", "source_type", "source_id"),
        Index("idx_relations_target", "target_type", "target_id"),
    )


class LLMCall(Base):
    """每次调用模型的完整 prompt / 响应留档，便于复核误判与后续调优。"""

    __tablename__ = "llm_calls"

    id: Mapped[int] = mapped_column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True)
    incident_id: Mapped[str | None] = mapped_column(String(64), index=True)
    provider: Mapped[str] = mapped_column(String(32))
    model: Mapped[str] = mapped_column(String(64))
    masked: Mapped[bool] = mapped_column(Boolean, default=False)
    prompt: Mapped[str | None] = mapped_column(Text)
    response: Mapped[str | None] = mapped_column(Text)
    parsed: Mapped[dict | None] = mapped_column(JSON)
    ok: Mapped[bool] = mapped_column(Boolean, default=False)
    error: Mapped[str | None] = mapped_column(Text)
    duration_ms: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow)


class FeishuMessage(Base):
    """飞书推送留档（含干跑）。用于排查「到底发出去没有」。"""

    __tablename__ = "feishu_messages"

    id: Mapped[int] = mapped_column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True)
    channel: Mapped[str] = mapped_column(String(32))
    incident_id: Mapped[str | None] = mapped_column(String(64), index=True)
    alert_id: Mapped[str | None] = mapped_column(String(64))
    kind: Mapped[str] = mapped_column(String(32))
    payload: Mapped[dict | None] = mapped_column(JSON)
    ok: Mapped[bool] = mapped_column(Boolean, default=False)
    dry_run: Mapped[bool] = mapped_column(Boolean, default=False)
    error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow)
