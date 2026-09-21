"""飞书推送（双通道）。

事件通道（AIOPS_FEISHU_EVENT_WEBHOOK）：粒度 = Alert 创建 / 恢复；
工单通道（AIOPS_FEISHU_INCIDENT_WEBHOOK）：粒度 = 工单创建 / 根因变化 / 关闭。
夜莺原始告警通道不归本服务管，保持现状作为对照。

卡片版式：标题只放级别+故障对象+告警名，标量进双列字段区，分隔线切块；
关联告警按告警名聚合；时间线只留关键事件放页脚；三种工单卡片用表头色区分。
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

import httpx
from sqlalchemy.orm import Session

from app.config import settings
from app.db.models import FeishuMessage
from app.logging_setup import get_logger, log
from app.metrics import inc
from app.timeutil import local_display, local_hm, to_local, utcnow

logger = get_logger("app.integrations.feishu")

CHANNEL_EVENT = "event"
CHANNEL_INCIDENT = "incident"

_SEVERITY_LABEL = {"P0": "P0 严重", "P1": "P1 高", "P2": "P2 中", "P3": "P3 低"}
_SEVERITY_TEMPLATE = {"P0": "red", "P1": "red", "P2": "orange", "P3": "blue"}

_EVENT_LABEL = {
    "INCIDENT_CREATED": "工单创建",
    "ALERT_ATTACHED": "关联告警",
    "CONTEXT_COLLECTION_STARTED": "开始采集上下文",
    "AI_ANALYSIS_STARTED": "开始 AI 分析",
    "AI_ANALYSIS_FINISHED": "AI 分析完成",
    "FEISHU_SENT": "已推送飞书",
    "USER_ACKNOWLEDGED": "人工认领",
    "INCIDENT_ROOT_CHANGED": "根因更新",
    "INCIDENT_REOPENED": "故障复发",
    "RECOVERY_OBSERVING": "进入恢复观察",
    "RECOVERY_NOT_CONFIRMED": "恢复未确认",
    "INCIDENT_RESOLVED": "工单关闭",
    "ALERT_STALE_EXPIRED": "告警超时过期",
    "ALERT_DETACHED": "人工摘除告警",
    "INCIDENT_MERGED_IN": "并入本单",
    "INCIDENT_MERGED_OUT": "已并入其它单",
}

_REASON_LABEL = {
    "force_link": "强制关联",
    "causal_rule": "因果规则",
    "same_entity": "同一实体",
    "exact_entity": "同一实体",
    "same_node": "同一节点",
    "topology_d1": "拓扑相邻",
    "topology_d2": "拓扑二跳",
    "same_cluster": "同集群",
    "same_namespace": "同命名空间",
    "same_project": "同项目",
    "within_1m": "1分钟内",
    "within_3m": "3分钟内",
    "within_5m": "5分钟内",
}

# 时间线只在页脚展示这么多条
_TIMELINE_TAIL = 8
# 汇总卡最多列这么多工单，超出的靠链接看：飞书卡片过长会被折叠，反而更难扫
_DIGEST_MAX_ITEMS = 20

# 例行动作不进时间线：每关联一条告警都会刷一遍这些，会把真正关键的事件挤出视野
_TIMELINE_SKIP = {
    "CONTEXT_COLLECTION_STARTED",
    "AI_ANALYSIS_STARTED",
    "FEISHU_SENT",
}


# ----------------------------------------------------------------------
# 卡片原子
# ----------------------------------------------------------------------
def _div(content: str) -> dict:
    return {"tag": "div", "text": {"tag": "lark_md", "content": content}}


def _field(label: str, value: Any) -> dict:
    return {"is_short": True, "text": {"tag": "lark_md", "content": f"**{label}**\n{value}"}}


def _fields(pairs: list[tuple[str, Any]]) -> dict:
    visible = [(key, value) for key, value in pairs if value not in (None, "", [], {})]
    return {"tag": "div", "fields": [_field(key, value) for key, value in visible]}


def _hr() -> dict:
    return {"tag": "hr"}


def _note(content: str) -> dict:
    return {"tag": "note", "elements": [{"tag": "lark_md", "content": content}]}


def _shorten(value: Any, limit: int = 140) -> str:
    text = str(value or "").replace("\n", " ").strip()
    return text if len(text) <= limit else text[: limit - 1] + "…"


def incident_url(incident_id: str) -> str | None:
    """工单详情页的对外链接。未配 AIOPS_PUBLIC_BASE_URL 时返回 None（不渲染）。"""
    if not settings.public_base_url:
        return None
    return f"{settings.public_base_url}/ui/incidents/{quote(incident_id, safe='')}"


def incident_list_url() -> str | None:
    """未恢复工单列表页的对外链接。"""
    if not settings.public_base_url:
        return None
    return f"{settings.public_base_url}/ui/incidents"


def _link_elements(incident_id: str) -> list[dict]:
    """卡片底部的「查看详情」按钮 + 未恢复工单列表入口。

    群里只放摘要，细节（时间线、全部告警、上下文）一律点链接看，
    这样卡片不用越长越全，也就不必为了信息完整而多推几张。
    """
    detail = incident_url(incident_id)
    if not detail:
        return []
    listing = incident_list_url()
    actions: list[dict] = [
        {
            "tag": "button",
            "text": {"tag": "plain_text", "content": "查看工单详情"},
            "url": detail,
            "type": "primary",
        }
    ]
    if listing:
        actions.append(
            {
                "tag": "button",
                "text": {"tag": "plain_text", "content": "未恢复工单列表"},
                "url": listing,
                "type": "default",
            }
        )
    return [_hr(), {"tag": "action", "actions": actions}]


def format_duration(seconds: float | None) -> str | None:
    if seconds is None or seconds < 0:
        return None
    total = int(seconds)
    if total < 60:
        return f"{total} 秒"
    if total < 3600:
        return f"{total // 60} 分钟"
    hours, remainder = divmod(total, 3600)
    return f"{hours} 小时 {remainder // 60} 分钟" if remainder >= 60 else f"{hours} 小时"


def _impact_text(incident, diagnosis: dict, alerts: list | None = None) -> str | None:
    impact = dict((diagnosis or {}).get("impact") or (incident.context or {}).get("impact") or {})
    # 节点数用「当前关联告警涉及的去重节点数」，与卡片上的「节点详情」保持一致
    # （之前节点数取自采集时刻的 context，会和节点详情对不上：影响 3 节点 vs 详情 4 台）。
    nodes = {getattr(alert, "node", None) for alert in (alerts or []) if getattr(alert, "node", None)}
    if nodes:
        impact["nodes"] = len(nodes)
    unit = {"nodes": "节点", "gpus": "GPU", "pods": "Pod"}
    parts = [f"{value} {unit.get(key, key)}" for key, value in impact.items() if value]
    return " · ".join(parts) if parts else None


def _alert_groups(alerts: list, max_classes: int = 8) -> tuple[str, int]:
    """按告警名聚合，避免 30 个 Pod 刷 30 行。返回 (markdown, 类数)。"""
    groups: dict[str, dict] = {}
    for alert in alerts:
        entry = groups.setdefault(alert.alertname, {"count": 0, "samples": [], "occurrence": 0})
        entry["count"] += 1
        entry["occurrence"] += alert.occurrence_count or 1
        if len(entry["samples"]) < 2:
            entry["samples"].append(alert.entity_id)

    ordered = sorted(groups.items(), key=lambda item: (-item[1]["count"], item[0]))
    lines: list[str] = []
    for name, entry in ordered[:max_classes]:
        samples = "、".join(str(item) for item in entry["samples"] if item)
        suffix = f"（{samples}…）" if entry["count"] > len(entry["samples"]) else (f"（{samples}）" if samples else "")
        lines.append(f"· **{name}** ×{entry['count']}{suffix}")
    if len(ordered) > max_classes:
        rest = len(ordered) - max_classes
        lines.append(f"· _还有 {rest} 类告警，详见工单上下文_")
    return "\n".join(lines), len(ordered)


def _reason_text(reasons: list[str] | None) -> str | None:
    """把关联原因翻译成运维能读的话：说明「这些告警算同一个故障」。"""
    if not reasons:
        return None
    seen: list[str] = []
    for reason in reasons:
        head, _, tail = reason.partition(":")
        label = _REASON_LABEL.get(head, head)
        if head == "causal_rule" and tail:
            rule_id = tail.split(":")[0]
            label = f"因果规则 {rule_id}"
        if label not in seen:
            seen.append(label)
    return " · ".join(seen[:6]) if seen else None


def _timeline_text(events: list) -> str | None:
    """页脚时间线：聚合重复事件、保留 AI 分析结论，不再输出「事件 N 条／有效 N 条」这类元信息。"""
    if not events:
        return None

    attach_total = sum(1 for event in events if event.event_type == "ALERT_ATTACHED")
    merge_total = sum(1 for event in events if event.event_type == "INCIDENT_MERGED_IN")
    lines: list[str] = []
    attach_done = merge_done = False
    for event in events:
        if event.event_type in _TIMELINE_SKIP:
            continue
        stamp = local_hm(event.created_at)
        if event.event_type == "ALERT_ATTACHED":
            if attach_done:
                continue
            attach_done = True
            if attach_total > 1:
                lines.append(f"{stamp} 聚合告警 ×{attach_total}")
                continue
        if event.event_type == "INCIDENT_MERGED_IN":
            if merge_done:
                continue
            merge_done = True
            lines.append(f"{stamp} 同类工单并入 ×{merge_total}")
            continue
        if event.event_type == "AI_ANALYSIS_FINISHED":
            content = event.content or {}
            engine = str(content.get("engine") or "AI")
            confidence = content.get("confidence")
            text = f"AI 分析完成（{engine}）"
            try:
                if confidence is not None:
                    text += f"，置信度 {round(float(confidence) * 100)}%"
            except (TypeError, ValueError):
                pass
            lines.append(f"{stamp} {text}")
            continue
        lines.append(f"{stamp} {_EVENT_LABEL.get(event.event_type, event.event_type)}")

    return "\n".join(lines[-_TIMELINE_TAIL:])


# ----------------------------------------------------------------------
# 发送
# ----------------------------------------------------------------------
def _webhook(channel: str) -> str | None:
    if channel == CHANNEL_EVENT:
        return settings.feishu_event_webhook
    return settings.feishu_incident_webhook


def _post(channel: str, payload: dict[str, Any], session: Session, incident_id=None, alert_id=None, kind="text") -> bool:
    url = _webhook(channel)
    ok = False
    error: str | None = None
    dry_run = url is None
    if url:
        try:
            response = httpx.post(url, json=payload, timeout=settings.feishu_timeout)
            response.raise_for_status()
            body: dict = {}
            text = ""
            if response.content:
                text = response.text[:300]
                try:
                    body = response.json()
                except ValueError:
                    body = {}
            if isinstance(body, dict) and body.get("code") not in (None, 0):
                error = f"feishu_api_error:{text}"
                ok = False
            else:
                ok = True
            # 把飞书返回原文打进日志，排查「到底发出去了没有」时这是最直接的证据
            log(
                logger,
                logging.INFO if ok else logging.WARNING,
                "feishu_sent",
                channel=channel,
                kind=kind,
                status=response.status_code,
                response=text,
            )
        except Exception as exc:
            error = str(exc)
            inc("aiops_feishu_send_failed_total", {"channel": channel})
            log(logger, logging.ERROR, "feishu_send_failed", channel=channel, error=error)
    else:
        settings.outbox_dir.mkdir(parents=True, exist_ok=True)
        path = settings.outbox_dir / f"{channel}.jsonl"
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps({"at": datetime.now(timezone.utc).isoformat(), "payload": payload}, ensure_ascii=False) + "\n")
        ok = True
        log(logger, logging.INFO, "feishu_dry_run", channel=channel, kind=kind)

    session.add(
        FeishuMessage(
            channel=channel,
            incident_id=incident_id,
            alert_id=alert_id,
            kind=kind,
            payload=payload,
            ok=ok,
            dry_run=dry_run,
            error=error,
        )
    )
    # dry_run 单独计数：早期版本干跑也计入 sent_total，导致监控显示「事件通道已发 31 条」，
    # 实际一条都没发出去（2026-09-14 排查时被这个口径误导过）。
    if dry_run:
        inc("aiops_feishu_dry_run_total", {"channel": channel})
    else:
        inc("aiops_feishu_sent_total", {"channel": channel, "ok": str(ok).lower()})
    return ok


def _card(template: str, title: str, elements: list[dict]) -> dict:
    return {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True},
            "header": {"template": template, "title": {"tag": "plain_text", "content": title}},
            "elements": elements,
        },
    }


# ----------------------------------------------------------------------
# 事件通道卡片（Alert 粒度）
# ----------------------------------------------------------------------
def notify_alert_created(session: Session, alert) -> None:
    severity = _SEVERITY_LABEL.get(alert.severity or "", alert.severity)
    elements = [
        _fields(
            [
                ("对象", f"{alert.entity_type}:{alert.entity_id}"),
                ("级别", severity),
                ("节点", alert.node),
                ("集群", alert.cluster),
                ("首次时间", local_hm(alert.first_seen)),
                ("累计触发", alert.occurrence_count),
            ]
        )
    ]
    if alert.summary:
        elements += [_hr(), _div(f"**摘要**\n{_shorten(alert.summary, 200)}")]
    if alert.enrichment_status and alert.enrichment_status != "FULL":
        elements.append(_note(f"富化状态：{alert.enrichment_status}"))
    elements.append(_note(f"Alert ID：{alert.alert_id}"))

    payload = _card(
        _SEVERITY_TEMPLATE.get(alert.severity or "", "orange"),
        f"🔔 新告警 · {alert.alertname}",
        elements,
    )
    _post(CHANNEL_EVENT, payload, session, alert_id=alert.alert_id, kind="alert_created")


def notify_alert_resolved(session: Session, alert) -> None:
    elements = [
        _fields(
            [
                ("对象", f"{alert.entity_type}:{alert.entity_id}"),
                ("恢复时间", local_hm(alert.resolved_at)),
                ("恢复方式", {"resolved": "夜莺下发恢复", "stale": "超时自动过期"}.get(alert.resolution_reason or "", alert.resolution_reason)),
                ("累计触发", alert.occurrence_count),
                ("节点", alert.node),
                ("集群", alert.cluster),
            ]
        ),
        _note(f"Alert ID：{alert.alert_id}"),
    ]
    payload = _card("green", f"✅ 告警恢复 · {alert.alertname}", elements)
    _post(CHANNEL_EVENT, payload, session, alert_id=alert.alert_id, kind="alert_resolved")


# ----------------------------------------------------------------------
# 工单通道卡片（Incident 粒度：创建 / 根因变化 / 关闭）
# ----------------------------------------------------------------------
def build_incident_card(
    incident,
    alerts: list,
    diagnosis: dict | None = None,
    events: list | None = None,
    kind: str = "incident_created",
    reasons: list[str] | None = None,
    resolution: dict | None = None,
) -> dict:
    diagnosis = diagnosis or {}
    resolved = kind == "incident_resolved"
    root_changed = kind == "incident_root_changed"

    name = {"incident_created": "🚨 新工单", "incident_root_changed": "🔄 工单根因更新", "incident_resolved": "✅ 工单恢复"}.get(
        kind, "🚨 新工单"
    )
    icon, label = name.split(" ", 1)
    # 标题 = 告警等级 + 故障标题（群里扫一眼就知道多严重、是什么故障，
    # 多台机器合并的工单也一样，不用点开才知道等级）
    title = f"{icon} {incident.severity or '未分级'} · {incident.title}"
    template = "green" if resolved else _SEVERITY_TEMPLATE.get(incident.severity or "", "orange")

    # 合并工单要把每台机器的信息写全（见下方「节点详情」段）
    node_details = _node_details(alerts or [], incident.root_entity_id)

    duration = None
    if resolved and incident.resolved_at and incident.first_seen:
        duration = (to_local(incident.resolved_at) - to_local(incident.first_seen)).total_seconds()  # type: ignore[operator]

    elements: list[dict] = [
        _fields(
            [
                ("工单号", incident.incident_id),
                ("卡片", f"{icon} {label}"),
                ("级别", _SEVERITY_LABEL.get(incident.severity or "", incident.severity)),
                ("状态", incident.status),
                ("故障对象", f"{incident.root_entity_type}:{incident.root_entity_id}"),
                # 合并工单的「节点」只显示根因那台；全部机器见下方「节点详情」段
                ("节点", f"{incident.node}（共 {node_details[1]} 台）" if node_details and incident.node else incident.node),
                ("集群", incident.cluster),
                ("关联告警", f"{len(alerts) if alerts else incident.alert_count} 条"),
                ("影响", _impact_text(incident, diagnosis, alerts)),
                ("首次异常", local_display(incident.first_seen)),
                ("恢复时间", local_display(incident.resolved_at) if resolved else None),
                ("持续时长", format_duration(duration)),
                ("最近异常", local_display(incident.last_seen) if not resolved else None),
            ]
        )
    ]

    if root_changed and events:
        changes = [event for event in events if event.event_type == "INCIDENT_ROOT_CHANGED"]
        if changes:
            content = changes[-1].content or {}
            old_root = (content.get("old_root") or {}).get("entity")
            new_root = (content.get("new_root") or {}).get("entity")
            if new_root:
                elements += [_hr(), _div(f"**根因提升**\n{old_root or '—'} ➜ **{new_root}**")]

    reason_text = _reason_text(reasons)
    if reason_text:
        elements += [_hr(), _div(f"**关联依据**\n{reason_text}")]

    if resolved:
        elements += [
            _hr(),
            _div(
                "**处理结果**\n"
                + "\n".join(
                    filter(
                        None,
                        [
                            f"· 累计关联告警：**{incident.alert_count}** 条",
                            f"· 故障持续：**{format_duration(duration) or '未知'}**",
                            f"· 根因判断：{_shorten(incident.suspected_root_cause, 200) or '—'}",
                        ],
                    )
                )
            ),
        ]
    else:
        confidence = diagnosis.get("confidence")
        if confidence is not None:
            engine = diagnosis.get("engine") or "unknown"
            used_model = not str(engine).startswith("rule")
            confidence_text = f"{round(float(confidence) * 100)}%（{'模型' if used_model else '规则兜底，未接入模型'}）"
        else:
            confidence_text = "无（AI 不可用）"

        elements += [
            _hr(),
            _div(
                f"**初步判断**\n{_shorten(diagnosis.get('suspected_root_cause'), 300) or '（AI 不可用，未给出判断）'}\n"
                f"置信度：{confidence_text}"
            ),
        ]

        evidence = diagnosis.get("evidence") or []
        if evidence:
            lines = "\n".join(
                f"{index}. {_shorten(item.get('description'), 160)}"
                for index, item in enumerate(evidence[:6], start=1)
            )
            elements += [_hr(), _div(f"**关键证据**\n{lines}")]

        checks = diagnosis.get("recommended_checks") or []
        if checks:
            lines = "\n".join(f"{index}. {_shorten(item, 120)}" for index, item in enumerate(checks[:5], start=1))
            elements += [_hr(), _div(f"**建议排查**\n{lines}")]

    if alerts:
        groups_text, class_count = _alert_groups(alerts)
        # 用实际关联条数（incident.alert_count 可能滞后于合并操作，导致卡片里 2 条 vs ×4 自相矛盾）
        header = f"**关联告警**（{len(alerts)} 条 / {class_count} 类）"
        elements += [_hr(), _div(f"{header}\n{groups_text}")]

    if node_details:
        detail_text, node_count = node_details
        elements += [_hr(), _div(f"**节点详情**（{node_count} 台）\n{detail_text}")]

    timeline = _timeline_text(events or [])
    if timeline:
        elements += [_hr(), _div("**处理时间线**"), _note(timeline)]

    # 恢复卡必须写清「依据」：之前卡片只写"已恢复"，看不出是靠什么判的，
    # 结果一台几周不可达的节点被"恢复"了也没人发现（2026-09-14 修正）。
    if resolved:
        elements.append(_fields([("恢复依据", _recovery_basis(resolution))]))

    elements += _link_elements(incident.incident_id)

    footer = [f"工单 {incident.incident_id}", f"根因归属 {incident.root_entity_type}:{incident.root_entity_id}"]
    if diagnosis.get("engine"):
        footer.append(f"分析引擎 {diagnosis['engine']}")
    elements.append(_note(" · ".join(footer)))

    return _card(template, title, elements)


def notify_incident(
    session: Session,
    incident,
    alerts: list,
    diagnosis: dict | None = None,
    events: list | None = None,
    kind: str = "incident_created",
    reasons: list[str] | None = None,
    resolution: dict | None = None,
) -> bool:
    payload = build_incident_card(incident, alerts, diagnosis, events, kind, reasons, resolution)
    return _post(CHANNEL_INCIDENT, payload, session, incident_id=incident.incident_id, kind=kind)


def build_digest_card(incidents: list, now: datetime | None = None) -> dict:
    """每日 09:00 的「未恢复工单」汇总卡。

    一张卡片列全部未恢复工单，而不是每单一张：早高峰刷 20 张卡就是新的噪声。
    单个工单的细节走卡片里的链接看。
    """
    stamp = local_display(now or utcnow())
    if not incidents:
        return _card(
            "green",
            "☀️ 每日故障汇总 · 无未恢复工单",
            [_div(f"截至 **{stamp}**，没有未恢复的故障工单。"), _note("每日汇总 · 未恢复工单")]
            + ([_hr(), *_link_elements_list()] if incident_list_url() else []),
        )

    by_severity: dict[str, int] = {}
    for incident in incidents:
        key = str(incident.severity or "未分级")
        by_severity[key] = by_severity.get(key, 0) + 1
    order = ["P0", "P1", "P2", "P3", "未分级"]
    counts = "　".join(
        f"{key} **{by_severity[key]}**" for key in order if key in by_severity
    )
    worst = next((key for key in order if key in by_severity), "未分级")

    elements: list[dict] = [
        _fields(
            [
                ("统计时间", stamp),
                ("未恢复工单", f"{len(incidents)} 个"),
                ("级别分布", counts),
            ]
        )
    ]

    lines: list[str] = []
    for index, incident in enumerate(incidents[:_DIGEST_MAX_ITEMS], start=1):
        bits = [f"**{incident.severity or '未分级'}**", _shorten(incident.title, 60)]
        if incident.node:
            bits.append(str(incident.node))
        # 带上当前疑似根因 + 置信度：卡片被"每天最多一张"压掉时，这是群里唯一能看到最新判断的地方
        if incident.suspected_root_cause:
            root = _shorten(incident.suspected_root_cause, 40)
            confidence = incident.root_cause_confidence
            try:
                suffix = f"（AI {round(float(confidence) * 100)}%）" if confidence is not None else ""
            except (TypeError, ValueError):
                suffix = ""
            bits.append(f"疑似根因：{root}{suffix}")
        age = None
        if incident.first_seen:
            age = format_duration((to_local(utcnow()) - to_local(incident.first_seen)).total_seconds())
        if age:
            bits.append(f"已持续 {age}")
        detail = incident_url(incident.incident_id)
        head = f"[{incident.incident_id}]({detail})" if detail else incident.incident_id
        lines.append(f"{index}. {head}　" + " · ".join(bits))
    elements += [_hr(), _div("**未恢复工单**\n" + "\n".join(lines))]
    if len(incidents) > _DIGEST_MAX_ITEMS:
        elements.append(_note(f"另有 {len(incidents) - _DIGEST_MAX_ITEMS} 个未列出，点下方链接查看全部。"))

    listing = incident_list_url()
    if listing:
        elements += [
            _hr(),
            {
                "tag": "action",
                "actions": [
                    {
                        "tag": "button",
                        "text": {"tag": "plain_text", "content": "查看未恢复工单"},
                        "url": listing,
                        "type": "primary",
                    }
                ],
            },
        ]
    elements.append(_note("每日汇总 · 未恢复工单"))
    return _card(_SEVERITY_TEMPLATE.get(worst, "orange"), f"☀️ 每日故障汇总 · {len(incidents)} 个未恢复", elements)


def _link_elements_list() -> list[dict]:
    listing = incident_list_url()
    if not listing:
        return []
    return [
        {
            "tag": "action",
            "actions": [
                {
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": "查看未恢复工单"},
                    "url": listing,
                    "type": "default",
                }
            ],
        }
    ]


def notify_digest(session: Session, incidents: list, now: datetime | None = None) -> bool:
    payload = build_digest_card(incidents, now)
    return _post(CHANNEL_INCIDENT, payload, session, kind="daily_digest")


def _node_details(alerts: list, root_entity_id: str | None = None) -> tuple[str, int] | None:
    """合并工单的「节点详情」：把每台机器的信息写全。

    用途：一场"4 台 master 同时 NotReady"的故障会合并成一个工单，
    但卡片上的「节点」字段只显示根因那台，看卡的人会以为只挂了一台。
    只有一台机器时返回 None（上面的「节点」字段已经够了，避免重复）。
    """
    rows: dict[str, dict] = {}
    for alert in alerts:
        key = str(getattr(alert, "node", None) or getattr(alert, "ip", None) or getattr(alert, "entity_id", "") or "").strip()
        if not key:
            continue
        item = rows.setdefault(key, {"ip": None, "alerts": set(), "model": None, "zone": None})
        item["ip"] = item["ip"] or getattr(alert, "ip", None)
        item["model"] = item["model"] or getattr(alert, "accelerator_model", None)
        item["zone"] = item["zone"] or getattr(alert, "zone", None)
        if getattr(alert, "alertname", None):
            item["alerts"].add(alert.alertname)
    if len(rows) <= 1:
        return None

    lines: list[str] = []
    for index, (node, item) in enumerate(sorted(rows.items()), start=1):
        bits: list[str] = []
        if item["ip"]:
            bits.append(str(item["ip"]))
        if item["model"]:
            bits.append(str(item["model"]))
        if item["zone"]:
            bits.append(str(item["zone"]))
        if item["alerts"]:
            names = sorted(item["alerts"])
            shown = "、".join(names[:3]) + ("…" if len(names) > 3 else "")
            bits.append(f"告警 {len(item['alerts'])} 类（{shown}）")
        if root_entity_id and node == root_entity_id:
            bits.append("**根因**")
        lines.append(f"{index}. {node}" + (f"（{' · '.join(bits)}）" if bits else ""))
    return "\n".join(lines), len(rows)


def _recovery_basis(resolution: dict | None) -> str:
    """把恢复验证的 detail 说成人话（卡片上要能一眼看出凭什么判恢复）。"""
    detail = resolution or {}
    verified_by = detail.get("verified_by")
    if verified_by == "kube_state_metrics":
        basis = "K8s 节点 Ready 条件已恢复（kube_node_status_condition）"
    elif verified_by == "prometheus":
        basis = "Prometheus 指标已恢复（up{instance=^IP:} 全部为 1）"
    elif verified_by == "alert_state_only":
        basis = "仅按告警状态判定（未配置指标源，未做独立验证）"
    else:
        basis = str(detail.get("reason") or "未记录")
    extra: list[str] = []
    if detail.get("nodes"):
        extra.append(f"节点 {'/'.join(str(n) for n in detail['nodes'][:3])}")
    if detail.get("ips"):
        extra.append(f"IP {'/'.join(str(i) for i in detail['ips'][:3])}")
    if detail.get("node_ready"):
        ready = detail["node_ready"]
        extra.append(f"Ready={sum(1 for ok in ready.values() if ok)}/{len(ready)}")
    return basis + ("（" + "，".join(extra) + "）" if extra else "")
