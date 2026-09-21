"""Context Collector：故障发生时系统到底处于什么状态。

与关联引擎的职责区分：关联判断「这些告警是不是同一个故障」，
这里回答「这个故障发生时系统状态如何」。

指标一律用 range query 回溯：NodeNotReady 时 node_exporter 已经掉线，
即时查询只会拿到 0，没有诊断价值。
"""
from __future__ import annotations

import logging
import re
from datetime import timedelta

from sqlalchemy.orm import Session

from app.config import settings
from app.correlation.rules import get_rules
from app.correlation.topology import TopologyService
from app.db import queries
from app.db.models import Alert, Incident
from app.integrations.kubernetes import get_client as get_k8s_client
from app.integrations.prometheus import get_client as get_prometheus_client
from app.integrations.prometheus import summarize_series
from app.logging_setup import get_logger, log
from app.netutil import promql_string
from app.timeutil import local_iso, utcnow

logger = get_logger("app.services.context_collector")


def _instance_regex(client, alert: Alert, cache: dict[str, str] | None = None) -> str:
    """拼「(节点名|IP)」正则，覆盖 node-exporter 的两种注册形态。

    所以先用 kube_node_info 把节点名解析成 internal_ip，再拼成
    `(节点名|IP)` 的正则，两种注册形态都能命中。

    解析顺序：富化结果 → 本次采集的缓存 → 真去查 Prometheus。
    一场风暴里同一节点会被 N 条告警问到，前两级能省掉绝大部分查询。
    """
    candidates: list[str] = []
    for value in (alert.ip, alert.hostname, alert.entity_id, alert.node):
        if value and value not in candidates:
            candidates.append(value)

    internal_ip: str | None = None
    if alert.node:
        enriched = (alert.enrichment or {}).get("kube_node") or {}
        internal_ip = enriched.get("internal_ip") or (cache or {}).get(alert.node) or None
        if not internal_ip and client is not None and client.configured:
            try:
                rows = client.instant(f'kube_node_info{{node="{promql_string(alert.node)}"}}')
                internal_ip = (rows[0].get("metric") or {}).get("internal_ip") if rows else None
            except Exception:  # 解析失败就退回按名字匹配，不影响主流程
                internal_ip = None
        if cache is not None:
            cache[alert.node] = internal_ip or ""
    if internal_ip and internal_ip not in candidates:
        candidates.append(internal_ip)

    if not candidates:
        return ""
    # 这里只做「正则转义」（re.escape）；PromQL 字符串转义由 _substitute 统一负责，
    # 两边都转会导致反斜杠被转两次，查询直接失效。
    escaped = [re.escape(item) for item in candidates]
    return escaped[0] if len(escaped) == 1 else "(" + "|".join(escaped) + ")"


def _substitute(query: str, alert: Alert, instance_re: str | None = None) -> str | None:
    """把模板变量替换成实际值。

    `$target` 是"目标实例匹配值"：优先用解析出来的 instance 正则，
    其次 ip → hostname → 实体 id 兜底。很多告警的 target_ident 是主机名，
    只用 $ip 会让整条查询被跳过（queries=0，证据里什么都没有）。
    """
    raw_target = instance_re or alert.ip or alert.hostname or alert.entity_id or alert.node or ""
    values = {
        "$target": promql_string(raw_target),
        "$ip": promql_string(raw_target),
        "$node": promql_string(alert.node or alert.hostname or alert.entity_id or ""),
        "$hostname": promql_string(alert.hostname or alert.entity_id or ""),
        "$namespace": promql_string(alert.namespace or ""),
        "$pod": promql_string(alert.entity_id or ""),
    }
    result = query
    for key, value in values.items():
        if key in result and not value:
            return None  # 关键变量缺失就跳过，不要查出全网数据
        result = result.replace(key, value)
    return result


def _prometheus_evidence(incident: Incident, alerts: list[Alert]) -> dict:
    client = get_prometheus_client()
    if not client.configured:
        return {"status": "NOT_CONFIGURED", "series": [], "note": "未配置 Prometheus，跳过指标上下文"}

    rules = get_rules()
    series: list[dict] = []
    start = incident.first_seen - timedelta(seconds=settings.context_lookback_seconds)
    end = incident.last_seen + timedelta(seconds=60)
    deadline = utcnow() + timedelta(seconds=settings.context_deadline_seconds)
    queries_sent = 0
    truncated = False

    internal_ip_cache: dict[str, str] = {}
    for alert in alerts:
        instance_re = _instance_regex(client, alert, internal_ip_cache)
        for template in rules.context_queries.get(alert.alertname) or rules.context_queries.get("default", []):
            if queries_sent >= settings.context_max_queries or utcnow() > deadline:
                truncated = True
                break
            query = _substitute(template, alert, instance_re)
            if query is None:
                continue
            queries_sent += 1
            for summary in summarize_series(client.range(query, start, end, step=settings.context_step_seconds)):
                # 带上查询语句：被 rate()/avg() 等包裹的指标会丢掉 __name__ 标签，
                # 只留 min/max/last 时模型无法判断这是什么指标
                summary = {**summary, "alertname": alert.alertname, "query": query}
                series.append(summary)
        if truncated:
            break

    return {
        "status": "OK",
        "window": {"start": start.isoformat(), "end": end.isoformat()},
        "queries": queries_sent,
        "truncated": truncated,
        "series": series,
    }


def _kubernetes_evidence(incident: Incident, alerts: list[Alert]) -> dict:
    """K8s 事实：优先用 K8s API，未接入时用 kube-state-metrics 的事实兜底。

    指标侧的 kube_node / kube_pod 事实已经在富化阶段拿到了（enricher），
    必须也放进这一节 —— 否则模型只看到 kubernetes.status=NOT_CONFIGURED，
    会在证据里写「无 K8s 事实、无法评估工作负载影响」，而事实其实就在旁边。
    """
    payload: dict = {}
    node_names: set[str] = set()

    client = get_k8s_client()
    if client.configured:
        payload["status"] = "OK"
        node_names |= {alert.node for alert in alerts if alert.node}
        if incident.node:
            node_names.add(incident.node)
        for node_name in list(node_names)[:3]:
            payload.setdefault("nodes", {})[node_name] = {
                "facts": client.node_facts(node_name),
                "pods": client.pods_on_node(node_name),
                "events": client.events_on_node(node_name),
            }

    from_metrics = []
    for alert in alerts:
        enrichment = alert.enrichment or {}
        facts = enrichment.get("kube_node") or enrichment.get("kube_pod")
        if facts:
            from_metrics.append({"entity": f"{alert.entity_type}:{alert.entity_id}", **facts})
    if from_metrics:
        payload["from_metrics"] = from_metrics
        payload.setdefault("status", "METRICS_ONLY")
        payload["source"] = "kube-state-metrics"

    return payload or {"status": "NOT_CONFIGURED"}


def _topology_evidence(session: Session, alerts: list[Alert]) -> list[dict]:
    topology = TopologyService(session)
    edges: list[dict] = []
    seen: set[tuple] = set()
    for alert in alerts:
        if not alert.entity_type or not alert.entity_id:
            continue
        for neighbor_type, neighbor_id in topology.neighbors(alert.entity_type, alert.entity_id):
            key = (alert.entity_type, alert.entity_id, neighbor_type, neighbor_id)
            if key in seen:
                continue
            seen.add(key)
            edges.append({"source": f"{alert.entity_type}:{alert.entity_id}", "target": f"{neighbor_type}:{neighbor_id}"})
    return edges[:40]


def estimate_impact(incident: Incident, alerts: list[Alert], kubernetes: dict) -> dict:
    nodes = {alert.node for alert in alerts if alert.node}
    if incident.node:
        nodes.add(incident.node)

    gpus = sum(1 for alert in alerts if alert.entity_type == "gpu")
    for alert in alerts:
        count = (alert.enrichment or {}).get("machine_info", {}).get("accelerator_count")
        try:
            gpus = max(gpus, int(count)) if count else gpus
        except (TypeError, ValueError):
            continue

    pods = len({alert.entity_id for alert in alerts if alert.entity_type in ("pod", "container")})
    for node_payload in (kubernetes.get("nodes") or {}).values():
        pods = max(pods, (node_payload.get("pods") or {}).get("not_ready_count", 0) or 0)
    return {"nodes": len(nodes), "gpus": gpus, "pods": pods}


def collect(session: Session, incident: Incident) -> dict:
    """采集工单上下文，写回 incident.context 并返回。"""
    alerts = queries.attached_alerts(session, incident.incident_id)
    prometheus = _prometheus_evidence(incident, alerts)
    kubernetes = _kubernetes_evidence(incident, alerts)
    # 时间一律用**北京时间**喂给模型：卡片字段显示的是 +08:00，模型若按 UTC ISO 复述，
    # 同一张卡里会出现两个差 8 小时的时间。
    context = {
        "time_zone": "Asia/Shanghai (+08:00)",
        "incident": {
            "incident_id": incident.incident_id,
            "status": incident.status,
            "severity": incident.severity,
            "root_entity": f"{incident.root_entity_type}:{incident.root_entity_id}",
            "root_alertname": queries.root_alertname(session, incident),
            "node": incident.node,
            "cluster": incident.cluster,
            "first_seen": local_iso(incident.first_seen),
            "last_seen": local_iso(incident.last_seen),
        },
        "alerts": [
            {
                "alert_id": alert.alert_id,
                "alertname": alert.alertname,
                "severity": alert.severity,
                "entity": f"{alert.entity_type}:{alert.entity_id}",
                "node": alert.node,
                "status": alert.status,
                "first_seen": local_iso(alert.first_seen),
                "last_seen": local_iso(alert.last_seen),
                "occurrence_count": alert.occurrence_count,
                "value": alert.value,
                "summary": alert.summary,
                "enrichment_status": alert.enrichment_status,
                "machine_info": (alert.enrichment or {}).get("machine_info"),
            }
            for alert in alerts
        ],
        "topology": _topology_evidence(session, alerts),
        "prometheus": prometheus,
        "kubernetes": kubernetes,
    }
    context["impact"] = estimate_impact(incident, alerts, kubernetes)
    incident.context = context
    log(
        logger,
        logging.INFO,
        "context_collected",
        incident_id=incident.incident_id,
        alerts=len(alerts),
        queries=prometheus.get("queries"),
        prometheus_status=prometheus.get("status"),
        kubernetes_status=kubernetes.get("status"),
    )
    return context
