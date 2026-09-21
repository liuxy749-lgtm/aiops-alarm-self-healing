"""工单 Web 页面（只读、无鉴权 —— 私有网络打开即可看）。

- `GET /ui/incidents`        未恢复工单列表；每条是 `<details>` 折叠块，点一下就地展开详情
- `GET /ui/incidents/{id}`   同一页面并自动展开指定工单（飞书卡片里的「查看工单详情」按钮指这里）

零依赖的原因：私有网络离线环境，不引前端框架、不引 CDN；折叠用原生 `<details>/<summary>`，
不需要一行 JS。卡片点开要立刻能看到内容，页面必须在任何浏览器/无网环境下都能渲染。
"""
from __future__ import annotations

import html
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db import queries
from app.db.models import Incident
from app.timeutil import local_display, local_hm, to_local, utcnow

_SEVERITY_CLASS = {"P0": "sev-p0", "P1": "sev-p1", "P2": "sev-p2", "P3": "sev-p3"}
_STATUS_LABEL = {
    "OPEN": "未恢复",
    "ACKNOWLEDGED": "处理中",
    "RECOVERING": "恢复观察中",
    "RESOLVED": "已恢复",
    "MERGED": "已并入其它工单",
}

_CSS = """
:root { color-scheme: light dark; }
* { box-sizing: border-box; }
body { margin: 0; padding: 24px; font: 14px/1.6 -apple-system, "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif;
       background: #f5f6f8; color: #1c1f23; }
h1 { font-size: 20px; margin: 0 0 4px; }
.hint { color: #6b7280; font-size: 12px; margin-bottom: 16px; }
.wrap { max-width: 1180px; margin: 0 auto; }
a { color: #1d4ed8; }
details.card { background: #fff; border: 1px solid #e3e6ea; border-left: 4px solid #9aa4b2;
               border-radius: 8px; margin-bottom: 10px; overflow: hidden; }
details.card[open] { box-shadow: 0 2px 10px rgba(16,24,40,.08); }
details.card.sev-p0 { border-left-color: #b91c1c; } details.card.sev-p1 { border-left-color: #dc2626; }
details.card.sev-p2 { border-left-color: #ea8a00; } details.card.sev-p3 { border-left-color: #2563eb; }
summary { cursor: pointer; padding: 12px 14px; display: flex; flex-wrap: wrap; gap: 8px 14px; align-items: baseline; }
summary::-webkit-details-marker { display: none; }
sev { font-weight: 700; }
.sev-badge { font-weight: 700; padding: 1px 8px; border-radius: 999px; font-size: 12px;
             background: #eef1f5; color: #111; }
.sev-p0 .sev-badge { background: #7f1d1d; color: #fff; } .sev-p1 .sev-badge { background: #dc2626; color: #fff; }
.sev-p2 .sev-badge { background: #f59e0b; color: #231303; } .sev-p3 .sev-badge { background: #2563eb; color: #fff; }
.title { font-weight: 600; flex: 1 1 380px; }
.meta { color: #6b7280; font-size: 12px; }
.body { border-top: 1px solid #eef1f5; padding: 4px 14px 16px; }
.fields { display: grid; grid-template-columns: repeat(auto-fit, minmax(210px, 1fr)); gap: 6px 18px; margin: 10px 0; }
.fields div span { color: #6b7280; margin-right: 6px; }
h3 { font-size: 14px; margin: 16px 0 6px; border-left: 3px solid #cbd5e1; padding-left: 8px; }
table { border-collapse: collapse; width: 100%; font-size: 13px; margin-top: 4px; }
th, td { border: 1px solid #e5e7eb; padding: 5px 8px; text-align: left; vertical-align: top; }
th { background: #f8fafc; font-weight: 600; color: #475569; }
code, .mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12px; }
ul { margin: 4px 0 0 18px; padding: 0; } li { margin: 2px 0; }
.timeline { font-family: ui-monospace, Menlo, monospace; font-size: 12px; color: #374151; white-space: pre-wrap; }
.tag { display: inline-block; font-size: 11px; padding: 1px 6px; border-radius: 4px; background: #eef1f5; color: #374151; margin-left: 6px; }
.tag.firing { background: #fee2e2; color: #991b1b; } .tag.resolved { background: #dcfce7; color: #166534; }
.empty { background: #fff; border: 1px solid #e3e6ea; border-radius: 8px; padding: 28px; text-align: center; color: #6b7280; }
footer { color: #9aa4b2; font-size: 12px; margin-top: 20px; }
"""


def _esc(value) -> str:
    return html.escape(str(value if value is not None else "-"))


def _duration(first_seen) -> str:
    start = to_local(first_seen)
    if start is None:
        return "-"
    seconds = (to_local(utcnow()) - start).total_seconds()  # type: ignore[operator]
    return _human_duration(seconds)


def _human_duration(seconds: float) -> str:
    if seconds < 0:
        return "-"
    total = int(seconds)
    if total < 60:
        return f"{total} 秒"
    if total < 3600:
        return f"{total // 60} 分钟"
    hours, remainder = divmod(total, 3600)
    if hours < 48:
        return f"{hours} 小时 {remainder // 60} 分钟" if remainder >= 60 else f"{hours} 小时"
    return f"{hours // 24} 天 {hours % 24} 小时"


def _node_rows(alerts: list) -> list[dict]:
    rows: dict[str, dict] = {}
    for alert in alerts:
        key = str(getattr(alert, "node", None) or getattr(alert, "ip", None) or alert.entity_id or "").strip()
        if not key:
            continue
        item = rows.setdefault(key, {"ip": None, "names": set(), "count": 0, "model": None})
        item["ip"] = item["ip"] or getattr(alert, "ip", None)
        item["model"] = item["model"] or getattr(alert, "accelerator_model", None)
        item["attrs"] = None
        item["names"].add(alert.alertname)
        item["count"] += 1
    return [{"node": key, **value} for key, value in sorted(rows.items())]


def _incident_block(session: Session, incident: Incident, *, expand: bool) -> str:
    alerts = queries.attached_alerts(session, incident.incident_id)
    events = queries.timeline(session, incident.incident_id)
    diagnosis = incident.ai_diagnosis or {}
    severity = incident.severity or "未分级"
    open_attr = " open" if expand else ""

    head = (
        f'<details class="card {_SEVERITY_CLASS.get(severity, "")}" id="{_esc(incident.incident_id)}"{open_attr}>'
        f'<summary><span class="sev-badge">{_esc(severity)}</span>'
        f'<span class="title">{_esc(incident.title)}</span>'
        f'<span class="meta">{_esc(_STATUS_LABEL.get(incident.status, incident.status))} · '
        f'{_esc(incident.node or incident.root_entity_id)} · {_esc(incident.cluster)} · '
        f'告警 {len(alerts)} 条 · 已持续 {_esc(_duration(incident.first_seen))} · '
        f'最近 {_esc(local_display(incident.last_seen))}</span></summary>'
        f'<div class="body">'
    )

    fields = "".join(
        f"<div><span>{_esc(label)}</span>{_esc(value)}</div>"
        for label, value in (
            ("工单号", incident.incident_id),
            ("状态", _STATUS_LABEL.get(incident.status, incident.status)),
            ("故障对象", f"{incident.root_entity_type}:{incident.root_entity_id}"),
            ("节点", incident.node),
            ("集群", incident.cluster),
            ("关联告警", f"{len(alerts)} 条"),
            ("首次异常", local_display(incident.first_seen)),
            ("最近异常", local_display(incident.last_seen)),
            ("恢复时间", local_display(incident.resolved_at)),
            ("AI 引擎", diagnosis.get("engine")),
        )
        if value not in (None, "", [])
    )
    body = [f'<div class="fields">{fields}</div>']

    if diagnosis.get("summary") or diagnosis.get("suspected_root_cause"):
        items = []
        if diagnosis.get("summary"):
            items.append(f"<li><b>摘要</b>：{_esc(diagnosis['summary'])}</li>")
        if diagnosis.get("suspected_root_cause"):
            confidence = incident.root_cause_confidence
            suffix = f"（置信度 {round(confidence * 100)}%）" if confidence is not None else ""
            items.append(f"<li><b>疑似根因</b>：{_esc(diagnosis['suspected_root_cause'])}{_esc(suffix)}</li>")
        if diagnosis.get("impact"):
            impact = "、".join(f"{value} {key}" for key, value in diagnosis["impact"].items() if value)
            if impact:
                items.append(f"<li><b>影响</b>：{_esc(impact)}</li>")
        if diagnosis.get("recommended_action"):
            items.append(f"<li><b>建议动作</b>：{_esc(diagnosis['recommended_action'])}</li>")
        for evidence in (diagnosis.get("evidence") or [])[:8]:
            kind = evidence.get("type") if isinstance(evidence, dict) else ""
            text = evidence.get("description") if isinstance(evidence, dict) else evidence
            items.append(f'<li><span class="tag">{_esc(kind)}</span> {_esc(text)}</li>')
        body.append("<h3>AI 判断与证据</h3><ul>" + "".join(items) + "</ul>")
        checks = diagnosis.get("recommended_checks") or []
        if checks:
            body.append(
                "<h3>建议排查</h3><ul>"
                + "".join(f"<li>{_esc(item)}</li>" for item in checks[:8])
                + "</ul>"
            )

    nodes = _node_rows(alerts)
    if nodes:
        rows = "".join(
            f"<tr><td class='mono'>{_esc(item['node'])}</td><td>{_esc(item['ip'])}</td>"
            f"<td>{_esc(item['model'])}</td><td>{len(item['names'])} 类</td>"
            f"<td>{_esc('、'.join(sorted(item['names'])))}</td><td>{item['count']}</td></tr>"
            for item in nodes
        )
        body.append(
            "<h3>节点详情</h3><table><tr><th>节点</th><th>IP</th><th>加速卡</th><th>告警类数</th>"
            f"<th>告警类型</th><th>告警条数</th></tr>{rows}</table>"
        )

    if alerts:
        rows = "".join(
            f"<tr><td>{_esc(alert.alertname)}</td><td class='mono'>{_esc(alert.entity_id)}</td>"
            f"<td>{_esc(alert.status)}"
            f"{'<span class=&quot;tag firing&quot;>FIRING</span>' if alert.status == 'FIRING' else ''}</td>"
            f"<td>{alert.occurrence_count}</td><td>{_esc(local_display(alert.first_seen))}</td>"
            f"<td>{_esc(local_display(alert.last_seen))}</td>"
            f"<td>{_esc(alert.resolution_reason or '-')}</td></tr>"
            for alert in alerts[:60]
        )
        body.append(
            "<h3>关联告警</h3><table><tr><th>告警名</th><th>实体</th><th>状态</th><th>次数</th>"
            f"<th>首次</th><th>最近</th><th>结束原因</th></tr>{rows}</table>"
        )
        if len(alerts) > 60:
            body.append(f"<p class='hint'>（仅显示前 60 条，共 {len(alerts)} 条）</p>")

    if events:
        lines = "".join(
            f'{_esc(local_hm(event.created_at))}  {_esc(event.event_type)}'
            + (f"  {_esc(str(event.content)[:180])}" if event.content else "")
            + "\n"
            for event in events
        )
        body.append(f'<h3>时间线</h3><div class="timeline">{lines}</div>')

    body.append(
        f'<p class="hint">原始 JSON：<a href="/api/v1/incidents/{_esc(incident.incident_id)}">'
        f"/api/v1/incidents/{_esc(incident.incident_id)}</a></p>"
    )
    return head + "".join(body) + "</div></details>"


def render_incidents_page(
    session: Session, *, scope: str = "open", expand: str | None = None, limit: int = 200
) -> str:
    """渲染工单页面。scope=open 只列未恢复，scope=all 列最近全部（含已恢复）。"""
    if scope == "all":
        incidents = list(
            session.scalars(select(Incident).where(Incident.merged_into.is_(None)).order_by(Incident.last_seen.desc()).limit(limit)).all()
        )
    else:
        incidents = queries.unresolved_incidents(session, limit=limit)

    if expand and all(incident.incident_id != expand for incident in incidents):
        # 卡片链接指过来的工单可能已恢复 → 也要能打开
        single = queries.get_incident(session, expand)
        if single is not None:
            incidents = [single, *incidents]

    open_count = len([item for item in incidents if item.status in ("OPEN", "ACKNOWLEDGED", "RECOVERING")])
    blocks = "".join(_incident_block(session, incident, expand=incident.incident_id == expand) for incident in incidents)
    if not blocks:
        blocks = '<div class="empty">当前没有未恢复的故障工单 🎉</div>'

    tabs = (
        f'<a href="/ui/incidents">未恢复（{open_count}）</a>'
        if scope == "all"
        else f'未恢复（{open_count}） · <a href="/ui/incidents?scope=all">查看全部</a>'
    )
    return f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>故障工单 · AIOps</title><style>{_CSS}</style></head>
<body><div class="wrap">
<h1>故障工单</h1>
<div class="hint">{tabs} ｜ 点标题行展开详情 ｜ 生成于 {_esc(local_display(utcnow()))}</div>
{blocks}
<footer>AIOps 告警事件中心 · 每张工单每天最多一张飞书卡片，未恢复的每天 09:00 汇总一次</footer>
</div></body></html>"""
