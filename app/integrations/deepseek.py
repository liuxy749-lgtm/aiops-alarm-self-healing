"""AI 诊断（DeepSeek）。

工程约束：
- 强制结构化输出（response_format=json_object）并二次校验；
- 没有证据不给高置信度；不允许虚构查询结果；
- 未配置 API Key 时降级为规则 stub，绝不阻塞告警；
- 每次调用完整留档（脱敏后的 prompt + 原始响应）进 llm_calls 表；
- 走公网模型时按需脱敏资产信息（IP / 主机名）。
"""
from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any

import httpx
from sqlalchemy.orm import Session

from app.config import settings
from app.db.models import LLMCall
from app.logging_setup import get_logger, log

logger = get_logger("app.integrations.deepseek")

SYSTEM_PROMPT = """你是 BAAI 基础设施运维的故障诊断助手。你会收到一个已聚合的 Incident，
包含关联告警列表、拓扑关系、Prometheus 指标摘要和 Kubernetes 事实。

必须遵守：
1. 只根据给定证据推理，禁止虚构任何未提供的查询结果或事实。
2. 每条结论必须能对应到具体证据；没有证据支持的判断，confidence 不得高于 0.4。
3. 严禁声称执行了任何操作（重启、登录、修改配置等）。你只有只读上下文。
4. 不确定就说不确定，并给出需要人工确认的排查项。
5. 只输出 JSON，不要输出任何解释性文字或 Markdown。
6. 上下文里的时间**已经是北京时间（+08:00）**，直接引用，不要再换成 UTC。
7. title 是卡片标题用的短标题：不超过 20 个字，写清"几台什么设备怎么了"
   （例：3 台 master 节点 NotReady / 1 台节点 exporter 掉线），不要带工单号、不要写时间。

输出 JSON schema：
{
  "title": "≤20 字的故障标题",
  "summary": "一句话描述这个故障",
  "suspected_root_cause": "疑似根因",
  "confidence": 0.0-1.0,
  "evidence": [{"type": "metric|kubernetes|topology|alert", "description": "..."}],
  "impact": {"nodes": 0, "gpus": 0, "pods": 0},
  "recommended_checks": ["..."],
  "recommended_action": "...",
  "risk": "low|medium|high"
}"""

_IP_RE = re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}\b")
_HOST_RE = re.compile(r"\b(?:gpu|node|cpu|ascend|worker|master)[\w-]*\b", re.IGNORECASE)
# 自由文本里的主机名兜底：多段连字符 + 含数字（bm-example-zone1-d-a100-40g-2-176、p-kt-tianshu150-07）。
# 加“含数字”这个条件是为了不误伤组件名（node-exporter-metrics、kube-prometheus-stack 都没有数字）。
_HOSTLIKE_RE = re.compile(r"\b(?=[a-z0-9-]*\d)[a-z0-9]+(?:-[a-z0-9]+){2,}\b", re.IGNORECASE)
# ISO 时间戳前缀：多段连字符规则会误伤 2026-09-11T07:00:00，放行
_ISO_PREFIX_RE = re.compile(r"\d{4}-\d{2}-\d{2}")

_REQUIRED_KEYS = ("summary", "suspected_root_cause", "confidence", "evidence")


@dataclass
class DiagnosisResult:
    data: dict[str, Any] = field(default_factory=dict)
    ok: bool = False
    mocked: bool = False
    error: str | None = None
    duration_ms: int = 0


_ENTITY_TYPE_TOKENS = {
    "node",
    "pod",
    "gpu",
    "job",
    "disk",
    "container",
    "interface",
    "cluster",
    "service",
    "instance",
    "topology",
    "alert",
}

# 这些键的值是资产标识（IP / 主机名 / 实例），必须掩
_MASK_ASSET_KEYS = {
    "instance",
    "ip",
    "internal_ip",
    "pod_ip",
    "host_ip",
    "hostname",
    "node",
    "entity",
    "entity_id",
    "root_entity",
    "root_entity_id",
    "target_ident",
}

# 这些键的值是“组件名 / 指标名 / 规则名 / 类型名”，不是资产信息。
# 实测踩过：把 alertname 掩成 <host-1>（NodeDiskIoBusy 以 Node 开头）、
# 把 job 掩成 <host-2>（node-exporter）、把 pod 名掩掉，
# 结果 prompt 里出现 200 处 <host->，模型直接说「指标名被掩码，无法判断是 io_time 还是其它计数器」。
_MASK_SKIP_KEYS = {
    "__name__",
    "alertname",
    "root_alertname",
    "rule_name",
    "job",
    "container",
    "service",
    "endpoint",
    "namespace",
    "device",
    "pod",
    "entity_type",
    "type",
    "status",
    "severity",
    "engine",
    "source",
    "channel",
    "kind",
    # 业务标识（工单号/告警号/事件号）：不是资产，掩成 <host-N> 会让模型无法引用具体对象，
    # 而且它们形如 INC-20260911-001 / ALT-<hex>-<ts>，正好命中「多段连字符+含数字」规则。
    "id",
    "alert_id",
    "incident_id",
    "event_id",
    "root_alert_id",
    "fingerprint",
    "merged_into",
}


def _mask(obj: Any, mapping: dict[str, str], counters: dict[str, int]) -> Any:
    """把资产标识（IP / 主机名）替换为稳定占位符，同一实体映射到同一占位符。

    分两类处理，理由是实测踩过两头：
      · 按**字段**掩资产：真实主机名是 bm-example-… / p-example-… 这种，不在关键词表里，
        靠关键词匹配等于没掩到（而组件名 node-exporter / 规则名 NodeDiskIoBusy 反而被误掩）。
      · 按**模式**掩自由文本：summary / annotations 里的 IP 与主机名靠正则兜底。
    """

    def replace(kind: str, value: str) -> str:
        if value not in mapping:
            counters[kind] = counters.get(kind, 0) + 1
            mapping[value] = f"<{kind}-{counters[kind]}>"
        return mapping[value]

    def mask_asset(value: str) -> str:
        """掩资产标识：保留实体类型前缀（node:xxx）与端口，只掩主机名/IP。"""
        prefix, sep, rest = value.partition(":")
        if sep and prefix.lower() in _ENTITY_TYPE_TOKENS:
            return f"{prefix}:{mask_asset(rest)}"
        masked = _IP_RE.sub(lambda m: replace("ip", m.group(0)), value)
        if masked != value:
            return masked  # 含 IP：只掩 IP，端口保留（否则模型看不出是哪个 exporter）
        return replace("host", value)

    def mask_host(match: re.Match) -> str:
        token = match.group(0)
        # 实体类型前缀（如 node:192.0.2.250 里的 "node"）不是敏感信息
        if token.lower().rstrip(":") in _ENTITY_TYPE_TOKENS:
            return token
        # 带下划线的是指标名/指标口径（node_disk_io_time_seconds_total 之类），不是主机名
        if "_" in token:
            return token
        # ISO 时间戳（2026-09-11T07）会被多段连字符规则误伤，放行
        if _ISO_PREFIX_RE.match(token):
            return token
        # 不含数字的多段词是组件名（node-exporter、kube-prometheus-stack），不是主机名。
        # 真实主机名都带数字：bm-example-zone1-d-a100-40g-2-176、p-kt-tianshu150-07、gpu-node-021。
        if not any(ch.isdigit() for ch in token):
            return token
        return replace("host", token)

    if isinstance(obj, dict):
        result: dict = {}
        for key, value in obj.items():
            if key in _MASK_SKIP_KEYS:
                result[key] = value
            elif key in _MASK_ASSET_KEYS and isinstance(value, str):
                result[key] = mask_asset(value)
            else:
                result[key] = _mask(value, mapping, counters)
        return result
    if isinstance(obj, list):
        return [_mask(item, mapping, counters) for item in obj]
    if isinstance(obj, str):
        text = _IP_RE.sub(lambda m: replace("ip", m.group(0)), obj)
        text = _HOST_RE.sub(mask_host, text)
        return _HOSTLIKE_RE.sub(mask_host, text)
    return obj


def _unmask(text: str | None, mapping: dict[str, str]) -> str | None:
    """把模型输出里的占位符还原成真实资产名。

    必须按占位符长度倒序替换：否则 <host-1> 会先把 <host-10> 的前缀吃掉，
    还原出错误的资产名。
    """
    if not text or not mapping:
        return text
    for original, placeholder in sorted(mapping.items(), key=lambda item: len(item[1]), reverse=True):
        text = text.replace(placeholder, original)
    return text


def _clip(context: dict, max_chars: int = 24000) -> dict:
    """控制 prompt 体积，超长时裁剪指标序列（保留最关键的部分）。

    一次算出要删几条再批量删：早期写法每弹一条就把整个 context 重新 dumps 一遍，
    series 上千条时是 O(n²) 的纯 CPU 空转。
    """
    serialized = json.dumps(context, ensure_ascii=False)
    if len(serialized) <= max_chars:
        return context
    trimmed = json.loads(serialized)
    prometheus = trimmed.get("prometheus") or {}
    series = prometheus.get("series") or []
    if not series:
        return trimmed

    per_item = len(serialized) / len(series)
    over = len(serialized) - max_chars
    drop = min(len(series), max(1, int(over / per_item) + 1))
    prometheus["series"] = series[:-drop]
    trimmed["prometheus"] = prometheus
    trimmed["truncated"] = True
    return trimmed


class DiagnosisClient:
    def __init__(self) -> None:
        self.api_key = settings.deepseek_api_key
        self.base_url = settings.deepseek_base_url.rstrip("/")
        self.model = settings.deepseek_model

    @property
    def available(self) -> bool:
        return bool(self.api_key)

    # ------------------------------------------------------------------
    def _call_model(self, prompt: str) -> tuple[str | None, str | None]:
        if not self.api_key:
            return None, "no_api_key"
        body = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.1,
            "response_format": {"type": "json_object"},
            "stream": False,
        }
        last_error: str | None = None
        for attempt in range(settings.llm_max_retries + 1):
            try:
                response = httpx.post(
                    f"{self.base_url}/chat/completions",
                    headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
                    json=body,
                    timeout=settings.llm_timeout,
                )
                response.raise_for_status()
                payload = response.json()
                return payload["choices"][0]["message"]["content"], None
            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                log(logger, logging.WARNING, "llm_call_failed", attempt=attempt, error=last_error)
                time.sleep(min(2**attempt, 5))
        return None, last_error

    # ------------------------------------------------------------------
    def diagnose(self, session: Session, incident_id: str, context: dict) -> DiagnosisResult:
        started = time.time()
        masked = False
        payload = context
        mapping: dict[str, str] = {}
        if settings.mask_assets:
            # mapping 必须留着：调用返回后要用它把占位符还原成真实资产，
            # 否则卡片上会显示 <host-1> 这种占位符（脱敏只应作用于出网的那一份）。
            # 顺序：先裁剪再脱敏 —— 脱敏是递归遍历 + 3 条正则，对未裁剪的大 context 做纯浪费。
            payload = _mask(_clip(context), mapping, {})
            masked = True
        if not masked:
            payload = _clip(payload)
        prompt = json.dumps(payload, ensure_ascii=False)

        if self.available:
            content, error = self._call_model(prompt)
            duration_ms = int((time.time() - started) * 1000)
            if content is None:
                result = DiagnosisResult(ok=False, error=error, duration_ms=duration_ms)
                self._record(session, incident_id, prompt, None, None, False, error, duration_ms, masked)
                log(logger, logging.ERROR, "llm_unavailable_fallback_rules", incident_id=incident_id, error=error)
                fallback = self._rule_based(context)
                fallback.error = f"llm_failed:{error}"
                return fallback
            content = _unmask(content, mapping)
            parsed, parse_error = self._parse(content)
            self._record(
                session, incident_id, prompt, content, parsed, parsed is not None, parse_error, duration_ms, masked
            )
            if parsed is None:
                fallback = self._rule_based(context)
                fallback.error = f"llm_bad_output:{parse_error}"
                return fallback
            return DiagnosisResult(data=parsed, ok=True, duration_ms=duration_ms)

        # 未配置 Key：走规则 stub，并明确标注
        duration_ms = int((time.time() - started) * 1000)
        fallback = self._rule_based(context)
        self._record(session, incident_id, prompt, None, fallback.data, False, "no_api_key", duration_ms, masked)
        return fallback

    # ------------------------------------------------------------------
    def _parse(self, content: str) -> tuple[dict | None, str | None]:
        try:
            data = json.loads(content)
        except ValueError as exc:
            return None, f"invalid_json:{exc}"
        if not isinstance(data, dict):
            return None, "not_an_object"
        for key in _REQUIRED_KEYS:
            if key not in data:
                return None, f"missing_key:{key}"
        evidence = data.get("evidence") or []
        if not isinstance(evidence, list) or not evidence:
            # 没有证据：压低置信度，但不丢弃结论
            data["confidence"] = min(float(data.get("confidence") or 0), 0.3)
            data["evidence_gap"] = "模型未给出证据，置信度已自动下调"
        try:
            confidence = float(data.get("confidence") or 0)
        except (TypeError, ValueError):
            confidence = 0.0
        data["confidence"] = max(0.0, min(1.0, confidence))
        data.setdefault("evidence", [])
        data.setdefault("recommended_checks", [])
        data.setdefault("impact", {})
        # 短标题：卡片标题只用它（早期版本把整段 summary 当标题，228 字的标题刷屏）
        short_title = str(data.get("title") or "").strip().replace("\n", " ")
        if short_title:
            data["title"] = short_title[:40]
        else:
            data.pop("title", None)
        data["engine"] = f"llm:{self.model}"
        return data, None

    def _rule_based(self, context: dict) -> DiagnosisResult:
        """规则兜底：不接入模型时也能给出可解释结论，绝不编造。"""
        incident = context.get("incident", {})
        alerts = context.get("alerts", [])
        prometheus = context.get("prometheus", {})
        kubernetes = context.get("kubernetes", {})
        names = [alert.get("alertname") for alert in alerts]
        root_entity = incident.get("root_entity")

        evidence: list[dict] = [
            {"type": "alert", "description": f"聚合 {len(alerts)} 条告警：{', '.join(sorted(set(filter(None, names))))}"}
        ]
        if any(name in ("NodeExporterDown", "DCGMExporterDown", "KubeletDown") for name in names):
            evidence.append({"type": "metric", "description": "节点侧 exporter 失联，指向节点或网络级中断"})
        if "NodeNotReady" in names:
            evidence.append({"type": "kubernetes", "description": "存在 NodeNotReady 告警"})
        if prometheus.get("series"):
            evidence.append({"type": "metric", "description": f"Prometheus 回溯窗口内取到 {len(prometheus['series'])} 条指标摘要"})
        else:
            evidence.append({"type": "metric", "description": "未取得指标上下文（Prometheus 未配置或查询为空）"})
        if not kubernetes.get("nodes"):
            evidence.append({"type": "kubernetes", "description": "未取得 Kubernetes 事实（未配置或查询失败）"})

        confidence = 0.15 + 0.05 * len(evidence)
        if not prometheus.get("series"):
            confidence = min(confidence, 0.35)
        return DiagnosisResult(
            data={
                # 不要在摘要里塞「已聚合 N 条告警」这类会变的数字：
                # 摘要会变成工单标题，一旦嵌了计数，后面新增关联告警时标题就是错的。
                # 关联告警数在飞书卡片里是单独的字段。
                "summary": f"{root_entity} 疑似 {incident.get('root_alertname') or (names[0] if names else '故障')}",
                "suspected_root_cause": f"以 {root_entity} 为中心的故障，根因需人工确认（未接入模型，本结论由规则生成）",
                "confidence": round(min(confidence, 0.6), 2),
                "evidence": evidence,
                "impact": context.get("impact") or {},
                "recommended_checks": [
                    "检查节点网络连通性（bond/NIC 状态）",
                    "检查 kubelet 与 exporter 进程状态",
                    "检查内核日志 dmesg / 系统日志",
                    "确认是否有变更或批量任务同时发生",
                ],
                "recommended_action": "人工登录节点进一步确认",
                "risk": "medium",
                "engine": "rule-stub",
            },
            ok=False,
            mocked=True,
            error=None,
        )

    def _record(
        self,
        session: Session,
        incident_id: str | None,
        prompt: str,
        response: str | None,
        parsed: dict | None,
        ok: bool,
        error: str | None,
        duration_ms: int,
        masked: bool,
    ) -> None:
        session.add(
            LLMCall(
                incident_id=incident_id,
                provider="deepseek",
                model=self.model if self.available else "rule-stub",
                masked=masked,
                prompt=prompt,
                response=response,
                parsed=parsed,
                ok=ok,
                error=error,
                duration_ms=duration_ms,
            )
        )
