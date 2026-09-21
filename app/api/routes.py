"""HTTP API。"""
from __future__ import annotations

import json
import logging

from fastapi import APIRouter, HTTPException, Query, Request, Response
from pydantic import BaseModel
from sqlalchemy import select
from starlette.concurrency import run_in_threadpool

from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from app.config import settings
from app.correlation.rules import get_rules, reload_rules
from app.correlation.topology import TopologyService
from app.db import queries
from app.db.models import Alert, Incident, IncidentAlert, RawEvent, ResourceRelation
from app.db.session import SessionLocal
from app.integrations.deepseek import DiagnosisClient
from app.integrations.kubernetes import get_client as get_k8s_client
from app.integrations.prometheus import get_client as get_prometheus_client
from app.logging_setup import get_logger, log
from app.metrics import render as render_metrics
from app.services import context_collector, incident_service, normalizer, pipeline, raw_store, sweeper, worker
from app.timeutil import local_iso, utcnow
from app.ui import render_incidents_page

logger = get_logger("app.api")
router = APIRouter()


class RelationCreate(BaseModel):
    source_type: str
    source_id: str
    relation: str
    target_type: str
    target_id: str
    metadata: dict | None = None


# ----------------------------------------------------------------------
# 事件接入
# ----------------------------------------------------------------------
@router.post("/api/v1/events/nightingale")
async def ingest_nightingale(request: Request) -> dict:
    """夜莺 Webhook 入口。

    不声明 pydantic Body 模型：夜莺 HTTP 媒介默认不带 Content-Type，
    声明成 Body(dict) 会因 media type 不匹配直接 422。
    这里手工读 body，兼容单对象 / 数组 / {"events":[...]} 三种形态。
    """
    raw = await request.body()
    if not raw:
        raise HTTPException(
            status_code=422,
            detail="请求体为空：请检查夜莺该通知媒介的『请求体』模板（推荐 {{ jsonMarshal $event }}）",
        )
    try:
        payload = json.loads(raw)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=f"请求体不是合法 JSON: {exc}") from exc

    if isinstance(payload, list):
        items = [item for item in payload if isinstance(item, dict)]
    elif isinstance(payload, dict):
        items = normalizer.split_payloads(payload)  # 兼容 {"events":[...]} 形态
    else:
        raise HTTPException(status_code=422, detail=f"不支持的 payload 类型: {type(payload).__name__}")
    if not items:
        raise HTTPException(status_code=422, detail="请求体里没有可用的事件对象")

    try:
        results = await run_in_threadpool(_process_batch, items)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"处理失败: {exc}") from exc

    body = {
        "received": len(results),
        "processed": sum(1 for item in results if item.status == "PROCESSED"),
        "duplicated": sum(1 for item in results if item.status == "DUPLICATE"),
        "failed": sum(1 for item in results if item.status == "FAILED"),
        "results": [item.as_dict() for item in results],
    }
    if body["failed"] and not body["processed"] and not body["duplicated"]:
        raise HTTPException(status_code=422, detail=body)
    return body


def _process_batch(items: list[dict]) -> list:
    session = SessionLocal()
    try:
        results = pipeline.process_items(session, items)
        session.commit()
        return results
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@router.post("/api/v1/events")
async def ingest_generic(request: Request) -> dict:
    return await ingest_nightingale(request)


# ----------------------------------------------------------------------
# 查询
# ----------------------------------------------------------------------
@router.get("/api/v1/raw-events")
def list_raw_events(limit: int = Query(20, le=200)) -> dict:
    session = SessionLocal()
    try:
        rows = session.scalars(select(RawEvent).order_by(RawEvent.id.desc()).limit(limit)).all()
        return {
            "items": [
                {
                    "event_id": row.event_id,
                    "source": row.source,
                    "processing_status": row.processing_status,
                    "processing_error": row.processing_error,
                    "occurred_at": local_iso(row.occurred_at),
                    "received_at": local_iso(row.received_at),
                    "payload_file": row.payload_file,
                    "payload_offset": row.payload_offset,
                }
                for row in rows
            ]
        }
    finally:
        session.close()


@router.get("/api/v1/raw-events/{event_id}")
def get_raw_event(event_id: str) -> dict:
    session = SessionLocal()
    try:
        row = session.scalar(select(RawEvent).where(RawEvent.event_id == event_id))
        if row is None:
            raise HTTPException(status_code=404, detail="event 不存在")
        archived = raw_store.read_at(row.payload_file or "", row.payload_offset or 0) if row.payload_file else None
        return {
            "event_id": row.event_id,
            "source": row.source,
            "processing_status": row.processing_status,
            "normalized": row.normalized,
            "payload_db": row.payload,
            "payload_archive": archived,
            "archive_match": archived == row.payload,
        }
    finally:
        session.close()


@router.get("/api/v1/alerts")
def list_alerts(status: str | None = None, limit: int = Query(50, le=500)) -> dict:
    session = SessionLocal()
    try:
        statement = select(Alert).order_by(Alert.last_seen.desc()).limit(limit)
        if status:
            statement = statement.where(Alert.status == status.upper())
        return {
            "items": [
                {
                    "alert_id": row.alert_id,
                    "alertname": row.alertname,
                    "status": row.status,
                    "severity": row.severity,
                    "entity": f"{row.entity_type}:{row.entity_id}",
                    "node": row.node,
                    "cluster": row.cluster,
                    "incident_id": row.incident_id,
                    "occurrence_count": row.occurrence_count,
                    "first_seen": local_iso(row.first_seen),
                    "last_seen": local_iso(row.last_seen),
                    "resolution_reason": row.resolution_reason,
                    "enrichment_status": row.enrichment_status,
                }
                for row in session.scalars(statement).all()
            ]
        }
    finally:
        session.close()


def _incident_summary(row: Incident) -> dict:
    return {
        "incident_id": row.incident_id,
        "title": row.title,
        "status": row.status,
        "severity": row.severity,
        "root_entity": f"{row.root_entity_type}:{row.root_entity_id}",
        "node": row.node,
        "cluster": row.cluster,
        "alert_count": row.alert_count,
        "suspected_root_cause": row.suspected_root_cause,
        "root_cause_confidence": row.root_cause_confidence,
        "first_seen": local_iso(row.first_seen),
        "last_seen": local_iso(row.last_seen),
        "resolved_at": local_iso(row.resolved_at),
        # 副作用出锁后的分析进度：运维能一眼看出「还没有诊断」vs「诊断失败」
        "analysis_status": row.analysis_status,
        "analysis_kind": row.analysis_kind,
        "analysis_attempts": row.analysis_attempts,
        "analysis_error": row.analysis_error,
        "analysis_updated_at": local_iso(row.analysis_updated_at),
    }


@router.get("/api/v1/incidents")
def list_incidents(
    status: str | None = None, cluster: str | None = None, limit: int = Query(50, le=500)
) -> dict:
    session = SessionLocal()
    try:
        statement = (
            select(Incident)
            .where(Incident.merged_into.is_(None))
            .order_by(Incident.last_seen.desc())
            .limit(limit)
        )
        if status:
            statement = statement.where(Incident.status == status.upper())
        if cluster:
            statement = statement.where(Incident.cluster == cluster)
        return {"items": [_incident_summary(row) for row in session.scalars(statement).all()]}
    finally:
        session.close()


@router.get("/api/v1/incidents/{incident_id}")
def get_incident(incident_id: str, include_context: bool = True) -> dict:
    session = SessionLocal()
    try:
        incident = queries.get_incident(session, incident_id)
        if incident is None:
            raise HTTPException(status_code=404, detail="Incident 不存在")

        links = session.scalars(select(IncidentAlert).where(IncidentAlert.incident_id == incident_id)).all()
        alert_ids = [link.alert_id for link in links]
        alerts = session.scalars(select(Alert).where(Alert.alert_id.in_(alert_ids))).all() if alert_ids else []
        alert_map = {alert.alert_id: alert for alert in alerts}

        return {
            **_incident_summary(incident),
            "merged_into": incident.merged_into,
            "ai_summary": incident.ai_summary,
            "ai_diagnosis": incident.ai_diagnosis,
            "root_alert_id": incident.root_alert_id,
            "acknowledged_at": local_iso(incident.acknowledged_at),
            "alerts": [
                {
                    "alert_id": link.alert_id,
                    "alertname": alert_map[link.alert_id].alertname if link.alert_id in alert_map else None,
                    "entity": (
                        f"{alert_map[link.alert_id].entity_type}:{alert_map[link.alert_id].entity_id}"
                        if link.alert_id in alert_map
                        else None
                    ),
                    "status": alert_map[link.alert_id].status if link.alert_id in alert_map else None,
                    "occurrence_count": alert_map[link.alert_id].occurrence_count if link.alert_id in alert_map else None,
                    "relation_type": link.relation_type,
                    "correlation_score": link.correlation_score,
                    "correlation_reason": link.correlation_reason,
                }
                for link in links
            ],
            "timeline": [
                {
                    "event_type": event.event_type,
                    "actor": event.actor,
                    "content": event.content,
                    "created_at": local_iso(event.created_at),
                }
                for event in queries.timeline(session, incident_id)
            ],
            "context": incident.context if include_context else None,
        }
    finally:
        session.close()


# ----------------------------------------------------------------------
# 人工闭环
# ----------------------------------------------------------------------
def _load_or_404(session, incident_id: str) -> Incident:
    incident = queries.get_incident(session, incident_id)
    if incident is None:
        raise HTTPException(status_code=404, detail="Incident 不存在")
    return incident


@router.post("/api/v1/incidents/{incident_id}/ack")
def ack_incident(incident_id: str, actor: str = Query("operator")) -> dict:
    session = SessionLocal()
    try:
        incident = _load_or_404(session, incident_id)
        incident_service.ack_incident(session, incident, actor)
        session.commit()
        return {"incident_id": incident_id, "status": incident.status, "acknowledged_at": local_iso(incident.acknowledged_at)}
    finally:
        session.close()


@router.post("/api/v1/incidents/{incident_id}/resolve")
def resolve_incident(incident_id: str, actor: str = Query("operator"), reason: str | None = Query(None)) -> dict:
    session = SessionLocal()
    try:
        incident = _load_or_404(session, incident_id)
        incident_service.resolve_incident(session, incident, actor, reason)
        session.flush()
        try:
            pipeline.send_incident_card(session, incident, "incident_resolved")
        except Exception as exc:  # 推送失败不能影响关闭动作
            log(logger, logging.ERROR, "incident_resolved_notify_failed", incident_id=incident_id, error=str(exc))
        session.commit()
        return {"incident_id": incident_id, "status": incident.status}
    finally:
        session.close()


@router.post("/api/v1/incidents/{incident_id}/alerts/{alert_id}/detach")
def detach_alert(incident_id: str, alert_id: str, actor: str = Query("operator")) -> dict:
    """人工纠错：把误关联的告警摘出工单（防过度关联的兜底出口）。"""
    session = SessionLocal()
    try:
        incident = _load_or_404(session, incident_id)
        if not incident_service.detach_alert(session, incident, alert_id, actor):
            raise HTTPException(status_code=404, detail="该告警不在这个 Incident 中")
        session.commit()
        return {"incident_id": incident_id, "detached": alert_id, "alert_count": incident.alert_count}
    finally:
        session.close()


@router.post("/api/v1/incidents/{incident_id}/analyze")
def reanalyze(incident_id: str):
    """重跑上下文采集与 AI 诊断（不推卡片，避免刷群）。

    默认入队后台执行并返回 202：同步跑要 10 余秒，且长事务会挡住 webhook 的写。
    AIOPS_INLINE_ANALYSIS=true 时同步跑完返回 200 + 诊断结果（对照/回滚用）。
    """
    session = SessionLocal()
    try:
        incident = _load_or_404(session, incident_id)
        if settings.inline_analysis:
            result = pipeline.analyze_inline(session, incident, "manual_reanalyze")
            session.commit()
            return {"incident_id": incident_id, "ok": result.ok, "mocked": result.mocked, "diagnosis": result.data}
        pipeline.schedule_analysis(session, incident, "manual_reanalyze")
        session.commit()
        return JSONResponse(
            status_code=202,
            content={"incident_id": incident_id, "accepted": True, "analysis_status": "PENDING"},
        )
    finally:
        session.close()


@router.post("/api/v1/sweep")
def trigger_sweep() -> dict:
    """手动触发一轮巡检。与定时巡检共用单飞锁，不会两份同时跑。"""
    session = SessionLocal()
    try:
        summary = sweeper.run_sweep_single_flight(session)
        session.commit()
        if summary is None:
            return {"skipped": True, "reason": "sweep_already_running"}
        return summary
    finally:
        session.close()


# ----------------------------------------------------------------------
# 拓扑
# ----------------------------------------------------------------------
@router.post("/api/v1/topology/relations")
def create_relation(payload: RelationCreate) -> dict:
    session = SessionLocal()
    try:
        TopologyService(session).upsert(
            payload.source_type,
            payload.source_id,
            payload.relation,
            payload.target_type,
            payload.target_id,
            payload.metadata,
        )
        session.commit()
        return {"ok": True}
    finally:
        session.close()


@router.get("/api/v1/topology/relations")
def list_relations(limit: int = Query(200, le=2000)) -> dict:
    session = SessionLocal()
    try:
        rows = session.scalars(select(ResourceRelation).limit(limit)).all()
        return {
            "items": [
                {
                    "source": f"{row.source_type}:{row.source_id}",
                    "relation": row.relation,
                    "target": f"{row.target_type}:{row.target_id}",
                    "metadata": row.metadata_json,
                }
                for row in rows
            ]
        }
    finally:
        session.close()


# ----------------------------------------------------------------------
# 运维
# ----------------------------------------------------------------------
@router.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}


@router.get("/readyz")
def readyz() -> dict:
    session = SessionLocal()
    try:
        session.execute(select(Incident.incident_id).limit(1))
        rules = get_rules()
        return {
            "status": "ready",
            "storage": {"type": "sqlite", "db_path": str(settings.db_path), "raw_dir": str(settings.raw_dir)},
            "prometheus": "configured" if get_prometheus_client().configured else "not_configured",
            "kubernetes": "configured" if get_k8s_client().configured else "not_configured",
            "llm": "configured" if DiagnosisClient().available else "rule_stub_only",
            "feishu": {
                "event": "configured" if settings.feishu_event_webhook else "dry_run",
                "incident": "configured" if settings.feishu_incident_webhook else "dry_run",
            },
            "rules": {"causal_rules": len(rules.causal_rules), "threshold": rules.threshold},
            "retention": {"raw_days": settings.raw_retention_days, "event_days": settings.event_retention_days},
            "mask_assets": settings.mask_assets,
            # 调试开关状态必须可见：误开在生产会让重试重复建工单
            "debug_skip_dedup": settings.debug_skip_dedup,
        }
    finally:
        session.close()


@router.get("/metrics")
def metrics() -> Response:
    return Response(content=render_metrics(), media_type="text/plain; version=0.0.4")


@router.get("/api/v1/stats")
def stats() -> dict:
    session = SessionLocal()
    try:
        return pipeline.stats(session)
    finally:
        session.close()


@router.post("/api/v1/rules/reload")
def rules_reload() -> dict:
    rules = reload_rules()
    return {"causal_rules": len(rules.causal_rules), "threshold": rules.threshold}


# ----------------------------------------------------------------------
# 工单 Web 页面（只读、无鉴权 —— 用户 2026-09-15 决定：内网打开即可看）
# 卡片里的「查看工单详情」按钮指 /ui/incidents/{incident_id}
# ----------------------------------------------------------------------
def _render_ui(scope: str, expand: str | None, limit: int) -> HTMLResponse:
    session = SessionLocal()
    try:
        return HTMLResponse(render_incidents_page(session, scope=scope, expand=expand, limit=limit))
    finally:
        session.close()


@router.get("/ui", include_in_schema=False)
def ui_root() -> RedirectResponse:
    return RedirectResponse("/ui/incidents")


@router.get("/ui/incidents", response_class=HTMLResponse, include_in_schema=False)
def ui_incidents(scope: str = Query("open", pattern="^(open|all)$"), limit: int = Query(200, ge=1, le=1000)) -> HTMLResponse:
    """未恢复工单列表（scope=all 看最近全部，含已恢复）。"""
    return _render_ui(scope, None, limit)


@router.get("/ui/incidents/{incident_id}", response_class=HTMLResponse, include_in_schema=False)
def ui_incident_detail(incident_id: str) -> HTMLResponse:
    """单张工单，进来就展开（点飞书卡片按钮落到这里）。"""
    return _render_ui("open", incident_id, 200)
