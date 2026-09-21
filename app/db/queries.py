"""共享查询。

这里的每段查询原先散落在 engine / pipeline / incident_service / context_collector /
sweeper / routes 里各写一份（同一段 JOIN 最多重复 5 处），集中到这里避免口径漂移。
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.db.models import Alert, Incident, IncidentAlert, IncidentEvent, RawEvent

# 工单的非终态：处于这些状态的工单是可以继续关联告警的
OPEN_STATES: tuple[str, ...] = ("OPEN", "ACKNOWLEDGED", "RECOVERING")


def get_incident(session: Session, incident_id: str) -> Incident | None:
    return session.scalar(select(Incident).where(Incident.incident_id == incident_id))


def attached_alerts(session: Session, incident_id: str) -> list[Alert]:
    return list(
        session.scalars(
            select(Alert)
            .join(IncidentAlert, IncidentAlert.alert_id == Alert.alert_id)
            .where(IncidentAlert.incident_id == incident_id)
        ).all()
    )


def attached_alert_names(session: Session, incident_id: str) -> set[str]:
    return set(
        session.scalars(
            select(Alert.alertname)
            .join(IncidentAlert, IncidentAlert.alert_id == Alert.alert_id)
            .where(IncidentAlert.incident_id == incident_id)
        ).all()
    )


def timeline(session: Session, incident_id: str, limit: int = 200) -> list[IncidentEvent]:
    """按时间正序返回最近 limit 条时间线事件（长工单不会把全量读回内存）。"""
    rows = list(
        session.scalars(
            select(IncidentEvent)
            .where(IncidentEvent.incident_id == incident_id)
            .order_by(IncidentEvent.id.desc())
            .limit(limit)
        ).all()
    )
    return list(reversed(rows))


def root_alertname(session: Session, incident: Incident) -> str | None:
    if not incident.root_alert_id:
        return None
    return session.scalar(select(Alert.alertname).where(Alert.alert_id == incident.root_alert_id))


def incident_by_fingerprint(session: Session, fingerprint: str, since: datetime) -> Incident | None:
    """找「同类故障」的既有工单：曾经关联过同一 fingerprint 告警的最近一张未合并工单。

    fingerprint = 同规则 + 同资源，正是「相同类型的故障」的准确定义。
    有这一层，一个反复复发的故障始终收敛到同一张工单上（必要时复开），
    而不是每次复发都新建一张 —— 09-15 之前一场四天没恢复的故障产生了 17 张单。
    """
    return session.scalar(
        select(Incident)
        .join(IncidentAlert, IncidentAlert.incident_id == Incident.incident_id)
        .join(Alert, Alert.alert_id == IncidentAlert.alert_id)
        .where(
            Alert.fingerprint == fingerprint,
            Incident.merged_into.is_(None),
            Incident.last_seen >= since,
        )
        .order_by(Incident.last_seen.desc())
        .limit(1)
    )


def unresolved_incidents(session: Session, limit: int = 200) -> list[Incident]:
    """仍未恢复的工单（每日汇总与 Web 列表页共用口径）。"""
    return list(
        session.scalars(
            select(Incident)
            .where(Incident.status.in_(OPEN_STATES), Incident.merged_into.is_(None))
            .order_by(Incident.first_seen.asc())
            .limit(limit)
        ).all()
    )


def open_incidents(session: Session) -> list[Incident]:
    return list(
        session.scalars(
            select(Incident).where(
                Incident.status.in_(OPEN_STATES), Incident.merged_into.is_(None)
            )
        ).all()
    )


def status_counts(session: Session) -> dict[str, int]:
    """平台自身指标与 /api/v1/stats 共用的一组计数。"""

    def count(statement) -> int:
        return int(session.scalar(statement) or 0)

    return {
        "raw_events": count(select(func.count()).select_from(RawEvent)),
        "raw_events_failed": count(
            select(func.count()).select_from(RawEvent).where(RawEvent.processing_status == "FAILED")
        ),
        "alerts": count(select(func.count()).select_from(Alert)),
        "alerts_firing": count(select(func.count()).select_from(Alert).where(Alert.status == "FIRING")),
        "incidents": count(
            select(func.count()).select_from(Incident).where(Incident.merged_into.is_(None))
        ),
        "incidents_open": count(
            select(func.count()).select_from(Incident).where(Incident.status.in_(OPEN_STATES))
        ),
    }
