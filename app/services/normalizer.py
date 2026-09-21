"""标准化：把夜莺 payload 转成统一事件模型。

夜莺侧会做格式化（用户决定），所以这里的主要职责变成「校验 + 兜底映射」：
- 已格式化的 payload 走主路径；
- 夜莺原生 payload（rule_name/tags 或 events 数组）走兼容映射，方便灰度期双跑；
- 缺关键字段（alertname / 无法判定的 status）直接报错，绝不静默写入脏数据。
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from typing import Any

from app.models.schemas import (
    STATUS_FIRING,
    STATUS_RESOLVED,
    Entity,
    Hardware,
    NightingalePayload,
    NormalizedEvent,
    Scope,
    normalize_status,
)
from app.timeutil import utcnow

# 采集端是「集群级组件」的 job：它的 instance 是组件自身地址，不代表告警对象。
# 实测（2026-09-14）：kube_node_status_condition 经 kube-state-metrics 抓取，
# instance=198.51.100.210:8080，4 台不同 master 的告警共用同一个 IP
# → 既污染实体 IP，又让恢复验证永远查到 up=1（假恢复）。
_CLUSTER_SCOPED_JOBS = {
    "kube-state-metrics",
    "kube_state_metrics",
}

_SEVERITY_MAP = {
    "0": "P0",
    "1": "P1",
    "2": "P2",
    "3": "P3",
    "critical": "P1",
    "fatal": "P0",
    "danger": "P1",
    "error": "P1",
    "warning": "P2",
    "warn": "P2",
    "info": "P3",
    "notice": "P3",
}

_IPV4_RE = re.compile(r"\d{1,3}(?:\.\d{1,3}){3}")


def _split_host_port(value: str | None) -> tuple[str | None, str | None]:
    """把 `192.0.2.122:9100` 拆成 (host, port)。

    夜莺的 `target_ident` 常见形态就是 instance（带端口的 IP）。带端口当实体 id 会出问题：
    同一台机器的 `HighDiskUsage`(target=192.0.2.122:9100) 与 `NodeNotReady`(target=192.0.2.122)
    会被当成两个不同实体，same_entity 和「同节点」关联全断，工单不该拆的拆了。
    只认 host:port 且 port 是纯数字的形态，`node01:gpu3` 这种不会被误拆。
    """
    if not value:
        return None, None
    text = value.strip()
    if text.count(":") == 1:
        host, _, port = text.partition(":")
        if host and port.isdigit():
            return host, port
    return text, None


_ENTITY_HINTS: tuple[tuple[str, str], ...] = (
    ("pod", "pod"),
    ("container", "container"),
    ("node", "node"),
    ("kubelet", "node"),
    ("exporter", "node"),
    ("gpu", "gpu"),
    ("nccl", "job"),
    ("rdma", "node"),
    ("disk", "node"),
    ("filesystem", "node"),
    ("job", "job"),
    ("switch", "switch"),
)


class NormalizeError(ValueError):
    """payload 不符合契约，直接拒收（HTTP 422）。"""


def resolve_status(payload: dict, parsed: NightingalePayload | None = None) -> str:
    """确定 firing / resolved。

    优先级：能识别的显式 status > 夜莺的 is_recovered > 默认 firing。

    ⚠️ 夜莺 AlertCurEvent 里有个 `status` 字段，但它是 `Status int` 的内部瞬时字段
    （实测值 0），**不是告警状态**。原来无条件拿它当状态解析，会抛 ValueError
    把整条事件判失败（线上实测 422）。所以「认得出来才用」，认不出来就退回 is_recovered。
    """
    candidates: list[Any] = []
    if parsed is not None:
        candidates.append(parsed.status)
        candidates.append((parsed.annotations or {}).get("status"))
    candidates.append(payload.get("status"))
    for candidate in candidates:
        if candidate is None or isinstance(candidate, bool):
            continue
        try:
            return normalize_status(str(candidate))
        except ValueError:
            continue

    recovered = False
    if parsed is not None:
        recovered = _truthy(_pick(parsed.is_recovered, (parsed.annotations or {}).get("is_recovered")))
    if not recovered:
        recovered = _truthy(_pick(payload.get("is_recovered"), payload.get("resolved")))
    return STATUS_RESOLVED if recovered else STATUS_FIRING


def split_payloads(payload: dict) -> list[dict]:
    """夜莺原生 payload 可能一次带多条事件（events 数组）。"""
    events = payload.get("events")
    if isinstance(events, list) and events:
        inherited = {
            key: value
            for key, value in payload.items()
            if key in ("source", "schema_version", "cluster", "region", "zone", "projset", "project")
            and value is not None
        }
        merged: list[dict] = []
        for event in events:
            if isinstance(event, dict):
                merged.append({**inherited, **event})
        return merged or [payload]
    return [payload]


def _to_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    text = str(value).strip()
    return text or None


def _truthy(value: Any) -> bool:
    """夜莺的 is_recovered 可能是 bool，也可能是 "..."/"true"/1 字符串。"""
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in ("1", "true", "yes", "on", "resolved")


def _parse_time(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        # 夜莺 trigger_time 可能是秒或毫秒
        seconds = value / 1000 if value > 1e11 else value
        return datetime.fromtimestamp(seconds, tz=timezone.utc)
    text = str(value).strip()
    if re.fullmatch(r"\d{10}", text):
        return datetime.fromtimestamp(int(text), tz=timezone.utc)
    if re.fullmatch(r"\d{13}", text):
        return datetime.fromtimestamp(int(text) / 1000, tz=timezone.utc)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def normalize_severity(raw: Any) -> str | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if re.fullmatch(r"[Pp][0-3]", text):
        return text.upper()
    return _SEVERITY_MAP.get(text.lower())


def _pick(*values: Any) -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        if isinstance(value, (list, dict)) and not value:
            continue
        return value
    return None


def _infer_entity_type(alertname: str, labels: dict[str, Any], has_node: bool) -> str:
    lowered = alertname.lower()
    for hint, entity_type in _ENTITY_HINTS:
        if hint in lowered:
            return entity_type
    if labels.get("pod"):
        return "pod"
    if labels.get("gpu_index") is not None or labels.get("device") is not None:
        return "gpu"
    if has_node:
        return "node"
    return "unknown"


def _build_entity(parsed: NightingalePayload, labels: dict[str, Any], alertname: str) -> Entity:
    raw_entity = parsed.entity or {}
    # 节点标识优先级：payload 显式字段 → 告警标签里的节点字段 → target_ident/hostname 兜底。
    # ⚠️ 标签里的节点名必须排在 parsed.hostname **之前**：夜莺很多规则的 hostname 其实是
    # target_ident(=instance，形如 "198.51.100.9:9400")，拿它当节点会得到"带端口的假节点名"。
    # 实测（2026-09-16）：DCGM 的 nvidia-gpu-xid-error 告警 tags 里明明有
    # Hostname=bm-example-zone1-d-a100-40g-2-99，却因为优先取了 instance 而 node=None，
    # 导致卡片上「节点」空白、无法按机器聚合与关联。
    node = _to_text(
        _pick(
            raw_entity.get("node"),
            parsed.node,
            labels.get("node"),
            labels.get("nodename"),
            labels.get("kubernetes_node"),
            labels.get("Hostname"),
            parsed.hostname,
        )
    )
    hostname = _to_text(
        _pick(
            raw_entity.get("hostname"),
            labels.get("hostname"),
            labels.get("Hostname"),
            labels.get("nodename"),
            labels.get("kubernetes_node"),
            node,
            parsed.hostname,
        )
    )
    pod = _to_text(_pick(raw_entity.get("pod"), labels.get("pod")))
    namespace = _to_text(_pick(raw_entity.get("namespace"), parsed.namespace, labels.get("namespace")))
    # instance 只有在「采集端就是这个实体」时才能当实体 IP。
    # 反例（2026-09-14 实测踩到）：节点告警来自 kube-state-metrics，instance=198.51.100.210:8080
    # 是监控组件地址，4 台不同节点共用同一个值 → 恢复验证拿它去查 up{}，永远判「已恢复」。
    instance = labels.get("instance")
    instance_is_entity = str(labels.get("job") or "").strip().lower() not in _CLUSTER_SCOPED_JOBS
    ip = _to_text(
        _pick(
            raw_entity.get("ip"),
            parsed.ip,
            labels.get("ip"),
            instance if instance_is_entity else None,
        )
    )

    entity_type = _to_text(raw_entity.get("type") or parsed.entity_type)
    if entity_type is None:
        entity_type = _infer_entity_type(alertname, labels, bool(node or hostname))

    entity_id = _to_text(_pick(raw_entity.get("id"), parsed.entity_id, parsed.target_ident))
    if entity_id is None:
        if entity_type == "pod" and pod:
            entity_id = pod
        elif entity_type == "container" and pod:
            entity_id = f"{pod}/{_to_text(labels.get('container')) or 'container'}"
        elif entity_type == "gpu":
            gpu_index = _to_text(_pick(labels.get("gpu_index"), labels.get("device"), labels.get("gpu")))
            entity_id = f"{node or hostname or ip}:gpu{gpu_index}" if gpu_index is not None else (node or hostname or ip)
        else:
            entity_id = node or hostname or ip
    if entity_id is None:
        # 兜底：用标签摘要做身份，避免出现 NULL entity 导致 fingerprint 重复
        digest = hashlib.sha256(json.dumps(labels, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:12]
        entity_id = f"unknown-{digest}"

    # entity_id 若是 host:port（夜莺 target_ident = instance），剥掉端口，
    # 否则同一台机器在不同规则下会变成不同实体，跨规则关联全断。
    host_part, port_part = _split_host_port(entity_id)
    if host_part and port_part and _IPV4_RE.fullmatch(host_part):
        entity_id = host_part
        ip = ip or host_part

    node_value = node or hostname
    if not node_value and entity_type == "node" and entity_id:
        # 节点类告警至少把实体 id 当节点标识，否则「同节点」关联（same_physical_node）
        # 和 $node 查询替换全都无从判断。
        # 实测：相当一部分告警的 target_ident 是主机名（如 bm-example-zone1-...）而不是 IP。
        node_value = entity_id

    resolved_ip = None
    if ip:
        resolved_ip = ip.split(":")[0]
    elif entity_id and _IPV4_RE.fullmatch(entity_id):
        resolved_ip = entity_id

    return Entity(
        type=entity_type,
        id=entity_id,
        ip=resolved_ip,
        hostname=hostname,
        namespace=namespace,
        node=node_value,
    )


def _build_scope(parsed: NightingalePayload, labels: dict[str, Any]) -> Scope:
    raw_scope = parsed.scope or {}

    def pick(key: str) -> str | None:
        return _to_text(_pick(raw_scope.get(key), getattr(parsed, key, None), labels.get(key), labels.get(f"{key}_name")))

    return Scope(
        cluster=pick("cluster"),
        region=pick("region"),
        zone=pick("zone"),
        projset=pick("projset"),
        project=pick("project"),
        namespace=pick("namespace"),
        node=pick("node"),
    )


def build_dedup_key(payload: dict) -> str:
    """幂等键：同一份 payload 重发（夜莺 HTTP 重试）不应产生第二条 raw event。

    三种情况要分清：
      1) 格式化 payload 自带 event_id —— 夜莺侧生成的唯一 id，直接用它。
      2) 夜莺原生事件（全局 Webhook / 告警回调）：只有 `hash`，而 hash 是
         rule_id + vector_key 的摘要，**对同一条告警的重复触发是稳定不变的**。
         如果只拿 hash 当幂等键，第二次触发会被误判成重试丢掉、occurrence_count 永远是 1。
         所以必须再拼上「这一次触发」的标记：status + trigger_time + trigger_value。
         真正被重试的请求 body 完全相同（trigger_time 相同）→ 去重；
         重复触发是新的一次评估（trigger_time 变了）→ 保留。
      3) 什么都没有 —— 退化为整包内容摘要。
    """
    source = str(payload.get("source") or "nightingale")

    explicit = _pick(payload.get("event_id"))
    if explicit is not None:
        return hashlib.sha256(f"{source}|event_id={explicit}".encode()).hexdigest()

    n9e_hash = _pick(payload.get("hash"))
    if n9e_hash is not None:
        status = resolve_status(payload)
        marker = _pick(payload.get("trigger_time"), payload.get("first_trigger_time"), payload.get("occurred_at"))
        value = _pick(payload.get("trigger_value"), payload.get("value"))
        base = f"{source}|hash={n9e_hash}|status={status}|marker={marker}|value={value}"
        return hashlib.sha256(base.encode()).hexdigest()

    base = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(base.encode()).hexdigest()


def derive_event_id(payload: dict, dedup_key: str) -> str:
    explicit = _pick(payload.get("event_id"), payload.get("uuid"))
    if explicit:
        return f"evt_{explicit}"
    return f"evt_{dedup_key[:24]}"


def normalize(payload: dict) -> NormalizedEvent:
    try:
        parsed = NightingalePayload.model_validate(payload)
    except Exception as exc:  # pydantic ValidationError
        raise NormalizeError(f"payload 结构不符合契约: {exc}") from exc

    labels: dict[str, Any] = {**(parsed.tags_map or {}), **(parsed.tags or {}), **(parsed.labels or {})}
    annotations: dict[str, Any] = parsed.annotations or {}

    alertname = _to_text(_pick(parsed.alertname, parsed.event_type, parsed.rule_name))
    if not alertname:
        raise NormalizeError("缺少告警名（alertname / event_type / rule_name 至少一个必填）")

    raw_status = resolve_status(payload, parsed)
    status = raw_status
    occurred_at = _parse_time(_pick(parsed.occurred_at, parsed.trigger_time, labels.get("timestamp")))
    if occurred_at is None:
        occurred_at = utcnow()

    entity = _build_entity(parsed, labels, alertname)

    if not entity.ip:
        instance = _to_text(labels.get("instance"))
        if instance:
            entity.ip = instance.split(":")[0]

    scope = _build_scope(parsed, labels)
    if not scope.node and entity.node:
        scope.node = entity.node
    if not entity.node and scope.node:
        entity.node = scope.node
    if not scope.namespace and entity.namespace:
        scope.namespace = entity.namespace

    hardware = parsed.hardware or {}
    accelerator_model = _to_text(
        _pick(hardware.get("accelerator_model"), parsed.accelerator_model, labels.get("accelerator_model"))
    )
    accelerator_count_raw = _pick(hardware.get("accelerator_count"), labels.get("accelerator_count"))
    try:
        accelerator_count = int(accelerator_count_raw) if accelerator_count_raw is not None else None
    except (TypeError, ValueError):
        accelerator_count = None

    dedup_key = build_dedup_key(payload)
    value = _to_text(parsed.value)
    if value is None:
        value = _to_text(_pick(labels.get("value"), annotations.get("value")))

    return NormalizedEvent(
        event_id=derive_event_id(payload, dedup_key),
        source=_to_text(parsed.source) or "nightingale",
        schema_version=_to_text(parsed.schema_version) or "1",
        event_type=alertname,
        status=status,  # type: ignore[arg-type]
        severity=normalize_severity(parsed.severity if parsed.severity is not None else _pick(labels.get("severity"), annotations.get("severity"))),
        occurred_at=occurred_at,
        entity=entity,
        scope=scope,
        hardware=Hardware(accelerator_model=accelerator_model, accelerator_count=accelerator_count),
        value=value,
        summary=_to_text(_pick(parsed.summary, annotations.get("summary"), labels.get("summary"))) or f"{alertname} on {entity.id}",
        description=_to_text(_pick(parsed.description, annotations.get("description"), annotations.get("desc"))),
        labels=labels,
        annotations=annotations,
    )
