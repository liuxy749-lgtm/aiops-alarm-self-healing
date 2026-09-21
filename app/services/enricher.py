"""上下文富化：把实体 id 变成完整的资产/集群/项目/节点信息。

监管范围是 K8s 平台，所以实体元数据的权威来源是 **kube-state-metrics**
（`kube_node_info` / `kube_node_labels` / `kube_pod_info`），
它不需要 K8s API 凭证，从指标源就能取到节点与 Pod 的归属关系。

富化失败绝不能导致告警丢失：异常一律吞掉并记进 enrichment_status
（FULL / PARTIAL / FAILED），最差情况只是证据少一些。
"""
from __future__ import annotations

import logging

from sqlalchemy.orm import Session

from app.correlation.topology import TopologyService
from app.integrations.kubernetes import get_client as get_k8s_client
from app.integrations.prometheus import get_client as get_prometheus_client
from app.logging_setup import get_logger, log
from app.models.schemas import NormalizedEvent
from app.netutil import is_ipv4, promql_string

logger = get_logger("app.services.enricher")

_SCOPE_KEYS = ("cluster", "region", "zone", "projset", "project")

_NODE_INFO_FIELDS = (
    "internal_ip",
    "kernel_version",
    "kubelet_version",
    "os_image",
    "system_uuid",
    "container_runtime_version",
)


def _metric_labels(rows: list[dict]) -> dict:
    return (rows[0].get("metric") or {}) if rows else {}


def _k8s_metric_facts(session: Session, norm: NormalizedEvent, prometheus) -> dict:
    """从 kube-state-metrics 取 K8s 实体元数据（不需要 K8s API）。"""
    if not prometheus.configured:
        return {}

    topology = TopologyService(session)
    entity = norm.entity

    if entity.type == "node":
        node_name = entity.node or entity.id
        info: dict = {}
        if node_name:
            info = _metric_labels(prometheus.instant(f'kube_node_info{{node="{promql_string(node_name)}"}}'))
        if not info and entity.ip:
            # 告警的 target_ident 经常直接是 IP（ 192.0.2.46 这种），
            # 用 internal_ip 反查节点名，才能拿到 kubelet 版本/OS/主机名等事实，
            # 也才能让「同节点」关联在 IP 形态下正常工作。
            info = _metric_labels(
                prometheus.instant(f'kube_node_info{{internal_ip="{promql_string(entity.ip)}"}}')
            )
        if not info:
            return {}
        node_name = info.get("node") or node_name
        if node_name:
            # 节点标识统一用 K8s 节点名：告警可能以 IP 形态到达（target_ident=192.0.2.46），
            # 另一条同节点告警可能带主机名。两种写法不一致会让「同节点」关联失效，
            # 所以只要当前值是空的或就是个 IP，就用反查出来的节点名覆盖。
            if not entity.node or is_ipv4(entity.node):
                entity.node = node_name
            if not norm.scope.node or is_ipv4(norm.scope.node):
                norm.scope.node = node_name
            entity.hostname = entity.hostname or node_name
        facts = {key: info[key] for key in _NODE_INFO_FIELDS if info.get(key)}
        # 节点 IP 以 kube_node_info 的 internal_ip 为准 —— 它就是这个节点的地址。
        # 告警自带的 ip 可能是监控组件地址（kube-state-metrics 的 198.51.100.210，
        # 4 台节点共用），拿它做恢复验证会永远判「已恢复」。
        authoritative_ip = facts.get("internal_ip")
        if authoritative_ip and entity.ip != authoritative_ip:
            facts["ip_corrected_from"] = entity.ip or ""
            entity.ip = authoritative_ip
        # 指标源的环境标签就是集群标识（cluster02），比告警自带的 cluster 更可靠
        if not norm.scope.cluster and info.get("__prom_env__"):
            norm.scope.cluster = info["__prom_env__"]
        labels = _metric_labels(
            prometheus.instant(f'kube_node_labels{{node="{promql_string(node_name)}"}}')
        )
        node_labels = {
            key[len("label_") :].replace("_", "/"): value
            for key, value in labels.items()
            if key.startswith("label_") and value
        }
        if node_labels:
            facts["labels"] = node_labels
        topology.upsert("node", node_name, "MEMBER_OF", "cluster", norm.scope.cluster or "unknown")
        return {"kube_node": facts}

    if entity.type in ("pod", "container") and entity.namespace:
        pod_name = entity.id
        selector = f'namespace="{promql_string(entity.namespace)}",pod="{promql_string(pod_name)}"'
        info = _metric_labels(prometheus.instant(f"kube_pod_info{{{selector}}}"))
        if not info:
            return {}
        facts = {"node": info.get("node"), "pod_ip": info.get("pod_ip"), "host_ip": info.get("host_ip")}
        facts = {key: value for key, value in facts.items() if value}
        # Pod → Node 的归属：不接 K8s API 也能建立，直接决定 Pod 告警能否关联上节点告警
        if facts.get("node"):
            entity.node = entity.node or facts["node"]
            norm.scope.node = norm.scope.node or facts["node"]
            topology.upsert("pod", pod_name, "RUNS_ON", "node", facts["node"])
        for key in ("created_by_kind", "created_by_name"):
            if info.get(key):
                topology.upsert("pod", pod_name, "OWNED_BY", info[key].lower(), info[key])
        if not norm.scope.cluster and info.get("__prom_env__"):
            norm.scope.cluster = info["__prom_env__"]
        return {"kube_pod": facts}

    return {}


def enrich(session: Session, norm: NormalizedEvent) -> NormalizedEvent:
    notes: list[str] = []
    payload: dict[str, object] = {}
    attempted = 0
    succeeded = 0
    error_seen = False
    prometheus = get_prometheus_client()

    # --- ① K8s 实体元数据（监管范围内的一等来源）---
    if prometheus.configured:
        attempted += 1
        try:
            k8s_facts = _k8s_metric_facts(session, norm, prometheus)
            if k8s_facts:
                succeeded += 1
                payload.update(k8s_facts)
            else:
                notes.append("kube_state_metrics_empty")
        except Exception as exc:
            error_seen = True
            notes.append(f"kube_state_metrics_error:{exc}")
    else:
        notes.append("prometheus_not_configured")

    # --- ② machine_info（CMDB 兜底，补齐 region/zone/projset/project/加速卡型号）---
    if prometheus.configured and norm.entity.type in ("node",):
        attempted += 1
        info: dict[str, str] = {}
        try:
            # hostname 缺失时用实体 id 兜底：很多告警的 target_ident 就是主机名
            info = prometheus.machine_info(norm.entity.ip, norm.entity.hostname or norm.entity.id)
        except Exception as exc:  # 下层已吞异常，这里双保险
            error_seen = True
            notes.append(f"machine_info_error:{exc}")
        if info:
            succeeded += 1
            payload["machine_info"] = info
            for key in _SCOPE_KEYS:
                if not getattr(norm.scope, key, None) and info.get(key):
                    setattr(norm.scope, key, info[key])
            if not norm.hardware.accelerator_model and info.get("accelerator_model"):
                norm.hardware.accelerator_model = info["accelerator_model"]
        else:
            notes.append("machine_info_empty")

    # --- ③ K8s API（未接入则跳过；接入后用于验证与补充）---
    k8s = get_k8s_client()
    if k8s.configured:
        attempted += 1
        try:
            payload_k8s = _enrich_from_kubernetes(session, norm, k8s)
            if payload_k8s:
                succeeded += 1
                payload["kubernetes"] = payload_k8s
            else:
                notes.append("kubernetes_empty")
        except Exception as exc:
            error_seen = True
            notes.append(f"kubernetes_error:{exc}")
    else:
        notes.append("kubernetes_not_configured")

    # 区分「查了但没有这条记录」与「查询本身报错」：
    # 前者是 PARTIAL（CMDB 里没这台机器，很正常），后者才是 FAILED。
    # 早期版本把两者都标成 FAILED，导致模型在证据里写出
    # 「建议检查 enrichment_status=FAILED 的原因」这种误导性结论。
    if attempted == 0 or not error_seen:
        status = "FULL" if attempted and succeeded == attempted else "PARTIAL"
    else:
        status = "FAILED" if succeeded == 0 else "PARTIAL"

    if notes:
        payload["notes"] = notes
    norm.enrichment = payload
    norm.enrichment_status = status
    log(
        logger,
        logging.INFO,
        "enrichment_done",
        event_id=norm.event_id,
        entity=f"{norm.entity.type}:{norm.entity.id}",
        status=status,
        notes=notes,
    )
    return norm


def _enrich_from_kubernetes(session: Session, norm: NormalizedEvent, k8s) -> dict:
    """K8s API 富化（只读）：按实体类型补全事实并登记拓扑。"""
    topology = TopologyService(session)
    entity = norm.entity

    if entity.type == "pod" and entity.namespace:
        facts = k8s.pod_facts(entity.namespace, entity.id)
        if not facts:
            return {}
        node_name = facts.get("node")
        if node_name:
            entity.node = node_name
            norm.scope.node = node_name
            topology.upsert("pod", entity.id, "RUNS_ON", "node", node_name)
        for owner in facts.get("owned_by", []):
            if owner.get("name"):
                topology.upsert("pod", entity.id, "OWNED_BY", (owner.get("kind") or "workload").lower(), owner["name"])
        return facts

    if entity.type == "node":
        node_name = entity.node or entity.id
        facts = k8s.node_facts(node_name)
        if not facts:
            return {}
        if norm.scope.cluster:
            topology.upsert("node", node_name, "MEMBER_OF", "cluster", norm.scope.cluster)
        # 只取节点事实。该节点上的 Pod 列表与 events 不在 webhook 路径里查：
        # 这两个调用是「证据」而不是「关联所需」，后台的 context_collector 会重新取一遍，
        # 放在请求里等于每次告警多打两次 K8s API（同节点风暴下纯浪费）。
        return {"node": facts}

    return {}
