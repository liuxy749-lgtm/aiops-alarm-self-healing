"""一期验收测试：设计文档 §39 的 Case 1~5，加上幂等、合并、恢复、人工纠错。

这些用例不依赖任何外部系统（Prometheus / K8s / DeepSeek / 飞书都未配置），
因此同时验证了「外部依赖不可用时告警链仍完整」这条降级要求。
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

BASE = datetime(2026, 9, 11, 10, 0, 0, tzinfo=timezone(timedelta(hours=8)))
CLUSTER = "h800-prod"


def payload(
    alertname: str,
    entity_type: str,
    entity_id: str,
    *,
    seconds: int = 0,
    status: str = "firing",
    node: str | None = None,
    namespace: str | None = None,
    cluster: str = CLUSTER,
    severity: str = "P1",
    labels: dict | None = None,
) -> dict:
    body: dict = {
        "schema_version": "1",
        "source": "nightingale",
        "alertname": alertname,
        "status": status,
        "severity": severity,
        "occurred_at": (BASE + timedelta(seconds=seconds)).isoformat(),
        "entity": {"type": entity_type, "id": entity_id},
        "scope": {"cluster": cluster},
        "labels": labels or {},
    }
    if node:
        body["scope"]["node"] = node
        body["entity"]["node"] = node
    if namespace:
        body["scope"]["namespace"] = namespace
        body["entity"]["namespace"] = namespace
    return body


def send(client, *payloads: dict) -> list[dict]:
    results = []
    for item in payloads:
        response = client.post("/api/v1/events/nightingale", json=item)
        assert response.status_code == 200, response.text
        results.append(response.json())
    return results


def get_incidents(client, status: str | None = None) -> list[dict]:
    params = {"status": status} if status else None
    response = client.get("/api/v1/incidents", params=params)
    assert response.status_code == 200, response.text
    return response.json()["items"]


def stat(client, key: str):
    response = client.get("/api/v1/stats")
    assert response.status_code == 200
    return response.json()[key]


def incident_cards(session) -> int:
    """已发到飞书「工单通道」的卡片数（工单创建/根因变化/关闭各算一张）。"""
    from sqlalchemy import func, select

    from app.db.models import FeishuMessage

    session.rollback()  # 结束只读事务，取最新已提交快照
    return int(
        session.scalar(select(func.count()).select_from(FeishuMessage).where(FeishuMessage.channel == "incident")) or 0
    )


def event_messages(session) -> int:
    """「事件通道」的消息数（Alert 粒度，未配 webhook 时是干跑）。"""
    from sqlalchemy import func, select

    from app.db.models import FeishuMessage

    session.rollback()
    return int(
        session.scalar(select(func.count()).select_from(FeishuMessage).where(FeishuMessage.channel == "event")) or 0
    )


# ----------------------------------------------------------------------
# Case 1：重复告警去重
# ----------------------------------------------------------------------
def test_case1_100_duplicate_alerts_collapse(client):
    for second in range(100):
        send(client, payload("NodeNotReady", "node", "node01", seconds=second, node="node01"))

    assert stat(client, "raw_events") == 100
    assert stat(client, "alerts") == 1
    assert stat(client, "incidents") == 1

    alerts = client.get("/api/v1/alerts").json()["items"]
    assert len(alerts) == 1
    assert alerts[0]["occurrence_count"] == 100
    assert alerts[0]["first_seen"].startswith("2026-09-11T10:00:00")
    assert alerts[0]["last_seen"].startswith("2026-09-11T10:01:39")


def test_same_payload_twice_is_idempotent(client):
    """夜莺 webhook 重试：同一份 payload 不能产生两条 raw event。"""
    body = payload("NodeNotReady", "node", "node01", node="node01")
    first = client.post("/api/v1/events/nightingale", json=body).json()
    second = client.post("/api/v1/events/nightingale", json=body).json()

    assert first["processed"] == 1
    assert second["duplicated"] == 1
    assert stat(client, "raw_events") == 1
    assert stat(client, "alerts") == 1


# ----------------------------------------------------------------------
# Case 2：节点故障风暴
# ----------------------------------------------------------------------
def test_case2_node_failure_storm_becomes_one_incident(client):
    send(client, payload("NodeNotReady", "node", "node01", seconds=0, node="node01"))
    send(client, payload("KubeletDown", "node", "node01", seconds=12, node="node01"))
    send(client, payload("NodeExporterDown", "node", "node01", seconds=32, node="node01"))
    send(client, payload("DCGMExporterDown", "node", "node01", seconds=40, node="node01"))
    for index in range(30):
        send(
            client,
            payload(
                "PodNotReady",
                "pod",
                f"training-worker-{index}",
                seconds=60 + index,
                node="node01",
                namespace="training",
            ),
        )

    incidents = get_incidents(client)
    assert len(incidents) == 1, incidents
    assert incidents[0]["alert_count"] == 34
    assert incidents[0]["root_entity"] == "node:node01"

    detail = client.get(f"/api/v1/incidents/{incidents[0]['incident_id']}").json()
    relations = {item["alertname"]: item["relation_type"] for item in detail["alerts"]}
    assert relations["NodeNotReady"] == "ROOT"
    assert relations["KubeletDown"] == "SYMPTOM"
    assert relations["PodNotReady"] == "SYMPTOM"

    forced = [item for item in detail["alerts"] if item["alertname"] == "KubeletDown"][0]
    assert any(reason.startswith("force_link") for reason in forced["correlation_reason"]["reasons"])

    causal_pod = [item for item in detail["alerts"] if item["alertname"] == "PodNotReady"][0]
    assert any(reason.startswith("causal_rule") for reason in causal_pod["correlation_reason"]["reasons"])

    # 7 条独立告警 → 1 个 Incident + 1 条完整时间线
    assert {"INCIDENT_CREATED", "ALERT_ATTACHED", "AI_ANALYSIS_FINISHED", "FEISHU_SENT"} <= {
        event["event_type"] for event in detail["timeline"]
    }


# ----------------------------------------------------------------------
# Case 3：两个节点同时故障
# ----------------------------------------------------------------------
def test_case3_two_nodes_stay_separate(client):
    send(client, payload("NodeNotReady", "node", "node01", seconds=0, node="node01"))
    send(client, payload("NodeNotReady", "node", "node02", seconds=5, node="node02"))

    incidents = get_incidents(client)
    assert len(incidents) == 2, incidents
    assert {item["root_entity"] for item in incidents} == {"node:node01", "node:node02"}


# ----------------------------------------------------------------------
# Case 4：Pod 与 Node 关联（拓扑 + 因果）
# ----------------------------------------------------------------------
def test_case3b_same_alert_chain_on_different_nodes_stays_separate(client):
    """真实数据实测踩到的坑：NodeNotReady(node01) 不得吞掉 KubeletDown(node02)。

    force_link 与因果规则都声明了 same_node 语义，跨节点必须拒绝 ——
    否则同集群里任何一台节点的 KubeletDown 都会被并进别人那条链。
    """
    send(client, payload("NodeNotReady", "node", "node01", seconds=0, node="node01"))
    send(client, payload("KubeletDown", "node", "node02", seconds=10, node="node02"))

    incidents = get_incidents(client)
    assert len(incidents) == 2, incidents
    assert {item["root_entity"] for item in incidents} == {"node:node01", "node:node02"}


def test_case4_pod_belongs_to_failing_node(client):
    # 通过拓扑接口登记 pod-a → node01（K8s 未接入时由夜莺/巡检写入）
    client.post(
        "/api/v1/topology/relations",
        json={"source_type": "pod", "source_id": "pod-a", "relation": "RUNS_ON", "target_type": "node", "target_id": "node01"},
    )
    send(client, payload("NodeNotReady", "node", "node01", seconds=0, node="node01"))
    send(client, payload("PodNotReady", "pod", "pod-a", seconds=20, namespace="training"))

    incidents = get_incidents(client)
    assert len(incidents) == 1, incidents
    assert incidents[0]["root_entity"] == "node:node01"


def test_case4b_reverse_order_promotes_root_to_node(client):
    """症状先到、根因后到：根因必须被提升，而不是永远当症状。"""
    send(client, payload("PodNotReady", "pod", "pod-a", seconds=0, node="node01", namespace="training"))
    first = get_incidents(client)
    assert len(first) == 1
    assert first[0]["root_entity"] == "pod:pod-a"

    send(client, payload("NodeNotReady", "node", "node01", seconds=30, node="node01"))

    incidents = get_incidents(client)
    assert len(incidents) == 1
    assert incidents[0]["root_entity"] == "node:node01"

    detail = client.get(f"/api/v1/incidents/{incidents[0]['incident_id']}").json()
    relations = {item["alertname"]: item["relation_type"] for item in detail["alerts"]}
    assert relations["NodeNotReady"] == "ROOT"
    assert relations["PodNotReady"] == "SYMPTOM"
    assert "INCIDENT_ROOT_CHANGED" in {event["event_type"] for event in detail["timeline"]}


# ----------------------------------------------------------------------
# Case 5：无关告警不得因为同节点被合并
# ----------------------------------------------------------------------
def test_case5_never_link_blocks_unrelated_alerts(client):
    send(client, payload("DiskUsageHigh", "node", "node01", seconds=0, node="node01"))
    send(client, payload("GPUHighTemperature", "node", "node01", seconds=5, node="node01"))

    incidents = get_incidents(client)
    assert len(incidents) == 2, incidents


def test_same_node_without_causal_evidence_does_not_merge(client):
    """验证对设计文档 §16 评分表的修正。

    两条告警落在同一节点、不同实体（两个 Pod），除「同节点+同集群+1分钟内」外
    没有别的证据 —— 恰好 70 分。按文档会被合并，但那正是 Case 5 要防的过度关联。
    所以必须靠强关联信号门槛拦住（same_node 不是强信号）。
    """
    send(client, payload("FooAlert", "pod", "pod-a", seconds=0, node="node01"))
    send(client, payload("BarAlert", "pod", "pod-b", seconds=10, node="node01"))

    incidents = get_incidents(client)
    assert len(incidents) == 2, incidents


def test_same_entity_merges(client):
    """同实体是强信号：同节点上同一个 GPU 的 Xid 与掉卡，应当合并。"""
    send(client, payload("GPUXidError", "gpu", "node01:gpu3", seconds=0, node="node01"))
    send(client, payload("GPUMissing", "gpu", "node01:gpu3", seconds=10, node="node01"))

    incidents = get_incidents(client)
    assert len(incidents) == 1, incidents
    assert incidents[0]["alert_count"] == 2


# ----------------------------------------------------------------------
# 自动合并：症状先建两个工单，根因到达后应收敛成一个
# ----------------------------------------------------------------------
def test_incidents_merge_after_root_arrives(client):
    send(client, payload("PodNotReady", "pod", "pod-a", seconds=0, node="node01", namespace="training"))
    send(client, payload("PodNotReady", "pod", "pod-b", seconds=5, node="node01", namespace="training"))
    assert len(get_incidents(client)) == 2  # 无强证据时按设计分开

    send(client, payload("NodeNotReady", "node", "node01", seconds=60, node="node01"))

    incidents = get_incidents(client)
    assert len(incidents) == 1, incidents
    detail = client.get(f"/api/v1/incidents/{incidents[0]['incident_id']}").json()
    assert detail["alert_count"] == 3
    assert "INCIDENT_MERGED_IN" in {event["event_type"] for event in detail["timeline"]}
    merged_events = [event for event in detail["timeline"] if event["event_type"] == "INCIDENT_MERGED_IN"]
    assert merged_events[0]["content"]["reason"]


# ----------------------------------------------------------------------
# Alert 过期 + Incident 恢复观察
# ----------------------------------------------------------------------
def test_stale_alert_does_not_auto_resolve_incident(client, session):
    """告警「不再来」不等于恢复：stale 过期后工单保持 OPEN、不推恢复卡（2026-09-14 修正）。

    修正前的行为是：stale → RECOVERING → 观察期 → 用 up{} 判「已恢复」并推卡，
    实测把几周不可达的僵尸节点每小时误判一次恢复。现在只有夜莺显式恢复信号才进恢复流程。
    """
    from app.services.sweeper import run_sweep

    send(client, payload("NodeNotReady", "node", "node01", seconds=0, node="node01"))
    assert get_incidents(client, status="OPEN")

    base_utc = BASE.astimezone(timezone.utc)
    run_sweep(session, now=base_utc + timedelta(seconds=1000))  # 超过 900s 无事件 → Alert 过期
    session.commit()

    alerts = client.get("/api/v1/alerts").json()["items"]
    assert alerts[0]["status"] == "RESOLVED"
    assert alerts[0]["resolution_reason"] == "stale"

    # 关键断言：不进 RECOVERING、不关单
    assert not get_incidents(client, status="RECOVERING")
    assert get_incidents(client, status="OPEN")
    detail = client.get(f"/api/v1/incidents/{get_incidents(client)[0]['incident_id']}").json()
    assert any(event["event_type"] == "RECOVERY_UNCONFIRMED" for event in detail["timeline"])

    run_sweep(session, now=base_utc + timedelta(seconds=1400))  # 再等多久也不会自动关
    session.commit()
    assert get_incidents(client, status="OPEN")
    assert incident_cards(session) == 1, "只应有创建卡，不该有恢复卡"


def test_explicit_nightingale_recovery_resolves_incident(client, session):
    """夜莺显式恢复（is_recovered=true）才进恢复观察期，验证通过后关单并推恢复卡。"""
    from app.services.sweeper import run_sweep
    from app.timeutil import utcnow

    send(client, payload("NodeNotReady", "node", "node01", seconds=0, node="node01"))
    recovered = payload("NodeNotReady", "node", "node01", seconds=600, node="node01")
    recovered["status"] = "RESOLVED"          # 夜莺恢复通知
    recovered["is_recovered"] = True
    send(client, recovered)
    assert get_incidents(client, status="RECOVERING")

    # 恢复观察期是从「收到恢复信号」那一刻起算的（真实 now），所以这里要跨过它
    run_sweep(session, now=utcnow() + timedelta(seconds=600))
    session.commit()

    incidents = get_incidents(client)
    assert incidents[0]["status"] == "RESOLVED"
    assert incident_cards(session) == 2  # 工单创建 1 张 + 工单关闭 1 张


# ----------------------------------------------------------------------
# 推送噪音控制：风暴只能产生一张工单卡片
# ----------------------------------------------------------------------
def test_storm_sends_exactly_one_incident_card(client, session):
    """8 条告警 → 工单通道只发 1 张卡片；事件通道按 Alert 粒度发 8 条。

    实测踩过：每次关联告警都推一张卡片 → 节点故障风暴往群里刷 8 张，
    比原来的夜莺告警还吵。工单卡片只在「创建/根因变化/关闭」三个时机发。
    """
    send(client, payload("NodeNotReady", "node", "node01", seconds=0, node="node01"))
    send(client, payload("KubeletDown", "node", "node01", seconds=12, node="node01"))
    send(client, payload("NodeExporterDown", "node", "node01", seconds=32, node="node01"))
    send(client, payload("DCGMExporterDown", "node", "node01", seconds=40, node="node01"))
    for index in range(4):
        send(client, payload("PodNotReady", "pod", f"worker-{index}", seconds=60 + index, node="node01", namespace="training"))

    assert len(get_incidents(client)) == 1
    assert incident_cards(session) == 1
    assert event_messages(session) == 8


def test_root_promotion_sends_one_extra_card_only(client, session, monkeypatch):
    """根因提升补一张；之后普通关联不再刷群。

    这是关掉每日封顶时的行为。打开封顶后根因卡当天会被压掉（见下一个用例），
    所以这里显式关掉，测的是「根因提升」本身的推卡时机。
    """
    from app.config import settings

    monkeypatch.setattr(settings, "daily_card_cap", False)

    send(client, payload("PodNotReady", "pod", "pod-a", seconds=0, node="node01", namespace="training"))
    assert incident_cards(session) == 1

    send(client, payload("NodeNotReady", "node", "node01", seconds=30, node="node01"))
    assert incident_cards(session) == 2  # 根因从 pod 提升到 node

    send(client, payload("NodeExporterDown", "node", "node01", seconds=40, node="node01"))
    assert incident_cards(session) == 2  # 同一故障的更多证据，不重复推


def test_daily_cap_suppresses_root_promotion_card_same_day(client, session):
    """打开封顶后，根因提升当天不再补卡 —— 未恢复的故障靠每日汇总重新提醒。"""
    send(client, payload("PodNotReady", "pod", "pod-a", seconds=0, node="node01", namespace="training"))
    assert incident_cards(session) == 1

    send(client, payload("NodeNotReady", "node", "node01", seconds=30, node="node01"))
    assert incident_cards(session) == 1


def test_resolve_event_closes_alert_and_observes(client):
    send(client, payload("NodeNotReady", "node", "node01", seconds=0, node="node01"))
    send(client, payload("NodeNotReady", "node", "node01", seconds=60, status="resolved", node="node01"))

    alerts = client.get("/api/v1/alerts").json()["items"]
    assert alerts[0]["status"] == "RESOLVED"
    assert alerts[0]["resolution_reason"] == "resolved"
    # 不直接关单，先进观察期
    assert get_incidents(client, status="RECOVERING")


def test_firing_again_reopens_incident(client):
    send(client, payload("NodeNotReady", "node", "node01", seconds=0, node="node01"))
    send(client, payload("NodeNotReady", "node", "node01", seconds=60, status="resolved", node="node01"))
    send(client, payload("NodeNotReady", "node", "node01", seconds=120, node="node01"))

    assert get_incidents(client, status="OPEN")
    assert stat(client, "alerts") == 2  # 新的一次 ALERT 周期


# ----------------------------------------------------------------------
# 人工闭环与纠错
# ----------------------------------------------------------------------
def test_ack_resolve_and_detach(client):
    client.post(
        "/api/v1/topology/relations",
        json={"source_type": "pod", "source_id": "pod-a", "relation": "RUNS_ON", "target_type": "node", "target_id": "node01"},
    )
    send(client, payload("NodeNotReady", "node", "node01", seconds=0, node="node01"))
    send(client, payload("PodNotReady", "pod", "pod-a", seconds=20, namespace="training"))

    incident = get_incidents(client)[0]
    incident_id = incident["incident_id"]

    ack = client.post(f"/api/v1/incidents/{incident_id}/ack", params={"actor": "刘星雨"})
    assert ack.status_code == 200 and ack.json()["status"] == "ACKNOWLEDGED"

    detail = client.get(f"/api/v1/incidents/{incident_id}").json()
    pod_alert = [item for item in detail["alerts"] if item["alertname"] == "PodNotReady"][0]
    detach = client.post(f"/api/v1/incidents/{incident_id}/alerts/{pod_alert['alert_id']}/detach")
    assert detach.status_code == 200 and detach.json()["alert_count"] == 1

    resolved = client.post(f"/api/v1/incidents/{incident_id}/resolve", params={"reason": "人工确认已恢复"})
    assert resolved.status_code == 200 and resolved.json()["status"] == "RESOLVED"


def test_reanalyze_and_rule_stub(client):
    """未配置 DeepSeek 时必须走规则兜底，并且明确标注不是模型结论。"""
    send(client, payload("NodeNotReady", "node", "node01", seconds=0, node="node01"))
    incident_id = get_incidents(client)[0]["incident_id"]

    response = client.post(f"/api/v1/incidents/{incident_id}/analyze")
    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is False and body["mocked"] is True
    assert body["diagnosis"]["engine"] == "rule-stub"
    assert body["diagnosis"]["evidence"]
    assert body["diagnosis"]["confidence"] <= 0.6


# ----------------------------------------------------------------------
# 接入层校验与降级
# ----------------------------------------------------------------------
def test_bad_payload_is_rejected_not_silently_stored(client):
    response = client.post("/api/v1/events/nightingale", json={"status": "firing"})
    assert response.status_code == 422
    assert stat(client, "raw_events") == 1
    assert stat(client, "raw_events_failed") == 1
    assert stat(client, "alerts") == 0


def test_enrichment_degradation_keeps_alert(client):
    send(client, payload("NodeNotReady", "node", "node01", seconds=0, node="node01"))
    alert = client.get("/api/v1/alerts").json()["items"][0]
    assert alert["enrichment_status"] in ("PARTIAL", "FAILED")
    assert stat(client, "incidents") == 1


def test_raw_event_archive_roundtrip(client):
    body = payload("NodeNotReady", "node", "node01", seconds=0, node="node01")
    send(client, body)
    event_id = client.get("/api/v1/raw-events").json()["items"][0]["event_id"]

    detail = client.get(f"/api/v1/raw-events/{event_id}").json()
    assert detail["archive_match"] is True
    assert detail["payload_archive"]["alertname"] == "NodeNotReady"
    assert detail["normalized"]["fingerprint"]


def test_rule_based_compression_view(client):
    """文档的核心价值：52 条原始事件 → 少量告警 → 1 个工单。"""
    for index in range(20):
        send(client, payload("NodeNotReady", "node", "node01", seconds=index, node="node01"))
    send(client, payload("KubeletDown", "node", "node01", seconds=30, node="node01"))
    send(client, payload("NodeExporterDown", "node", "node01", seconds=31, node="node01"))
    for index in range(30):
        send(client, payload("PodNotReady", "pod", f"worker-{index}", seconds=40 + index, node="node01", namespace="training"))

    assert stat(client, "raw_events") == 52
    assert stat(client, "alerts") == 33
    assert stat(client, "incidents") == 1
    assert stat(client, "compression_ratio") > 0.9


def test_health_and_metrics_endpoints(client):
    send(client, payload("NodeNotReady", "node", "node01", seconds=0, node="node01"))

    ready = client.get("/readyz").json()
    assert ready["status"] == "ready"
    assert ready["storage"]["type"] == "sqlite"
    assert ready["llm"] == "rule_stub_only"
    assert ready["feishu"]["incident"] == "dry_run"

    metrics = client.get("/metrics").text
    assert "aiops_events_received_total" in metrics
    assert "aiops_alerts_created_total" in metrics
    assert "aiops_incidents_open" in metrics


# ----------------------------------------------------------------------
# 夜莺原生 Webhook 实测报文回归（n9e-host-01 真实推送的形状）
# ----------------------------------------------------------------------
def n9e_native(*, recovered: bool = False, trigger_time: int = 1789000000) -> dict:
    """照抄夜莺 `{{ jsonMarshal $event }}` 实际发出来的结构。

    两个关键点（线上就是这么 422 的）：
      - `tags` 是字符串数组 ["k=v", ...]（源码 TagsJSON []string），不是 dict
      - `status` 是 int（源码 Status int，内部瞬时字段），**不是告警状态**
    """
    return {
        "id": 12345,
        "cate": "prometheus",
        "cluster": "dx0-calc1",
        "datasource_id": 3,
        "group_id": 1,
        "group_name": "中心监控集群",
        "hash": "a1b2c3d4e5f6a7b8",
        "rule_id": 42,
        "rule_name": "NodeNetworkErrorOrDrop",
        "rule_note": "",
        "rule_prod": "metric",
        "rule_algo": "",
        "severity": 1,
        "prom_for_duration": 60,
        "prom_ql": "up == 0",
        "prom_eval_interval": 15,
        "callbacks": "",
        "runbook_url": "",
        "notify_recovered": 1,
        "target_ident": "192.0.2.21:9100",
        "target_note": "",
        "trigger_time": trigger_time,
        "trigger_value": "1",
        "trigger_values": "1",
        "tags": ["__prom_env__=dx0-calc1", "node=192.0.2.21", "instance=192.0.2.21:9100"],
        "tags_map": {"__prom_env__": "dx0-calc1", "node": "192.0.2.21", "instance": "192.0.2.21:9100"},
        "original_tags": ["node=192.0.2.21"],
        "annotations": {"summary": "节点网络异常"},
        "is_recovered": recovered,
        "notify_users_obj": [],
        "last_eval_time": trigger_time,
        "last_sent_time": trigger_time,
        "first_eval_time": trigger_time,
        "notify_cur_number": 1,
        "first_trigger_time": trigger_time - 60,
        "extra_config": None,
        "status": 0,
        "claimant": "",
        "sub_rule_id": 0,
        "extra_info": [],
        "target": {},
        "recover_config": {},
        "rule_hash": "deadbeef",
        "extra_info_map": {},
        "notify_rule_ids": [],
        "notify_rule_id": 0,
        "notify_rule_name": "",
        "notify_version": "",
        "recover_time": 0,
    }


def test_nightingale_native_event_parses(client):
    response = client.post("/api/v1/events/nightingale", json=n9e_native())
    assert response.status_code == 200, response.text
    assert response.json()["processed"] == 1

    alert = client.get("/api/v1/alerts").json()["items"][0]
    assert alert["alertname"] == "NodeNetworkErrorOrDrop"
    assert alert["status"] == "FIRING"
    assert alert["severity"] == "P1"
    # target_ident 是带端口的 instance，实体 id 必须剥成纯 IP
    assert alert["entity"] == "node:192.0.2.21"
    assert alert["node"] == "192.0.2.21"

    event_id = client.get("/api/v1/raw-events").json()["items"][0]["event_id"]
    normalized = client.get(f"/api/v1/raw-events/{event_id}").json()["normalized"]
    assert normalized["status"] == "FIRING"
    # tags(数组) 与 tags_map 都要进 labels
    assert normalized["labels"]["instance"] == "192.0.2.21:9100"
    assert normalized["labels"]["__prom_env__"] == "dx0-calc1"


def test_nightingale_native_recovery_closes_alert(client):
    client.post("/api/v1/events/nightingale", json=n9e_native())
    response = client.post("/api/v1/events/nightingale", json=n9e_native(recovered=True, trigger_time=1789000060))
    assert response.status_code == 200, response.text
    assert response.json()["processed"] == 1  # trigger_time 变了 → 不是重试，是新事件

    assert client.get("/api/v1/alerts").json()["items"][0]["status"] == "RESOLVED"
    assert get_incidents(client, status="RECOVERING")


def test_webhook_accepts_bare_array_and_no_content_type(client):
    """夜莺 HTTP 媒介默认不带 Content-Type；模板写成 $events 时 body 是数组。"""
    body = json.dumps([n9e_native()]).encode()
    response = client.post("/api/v1/events/nightingale", content=body, headers={"Content-Type": ""})
    assert response.status_code == 200, response.text
    assert response.json()["processed"] == 1


def test_webhook_accepts_events_wrapper(client):
    """夜莺原生批量：{"events": [...]} 也要能拆开处理。"""
    body = {"source": "nightingale", "events": [n9e_native()]}
    response = client.post("/api/v1/events/nightingale", json=body)
    assert response.status_code == 200, response.text
    assert response.json()["processed"] == 1


def test_webhook_empty_body_gives_actionable_error(client):
    response = client.post("/api/v1/events/nightingale", content=b"")
    assert response.status_code == 422
    assert "请求体为空" in response.text
    assert "jsonMarshal" in response.text


def test_mask_unmask_roundtrip_keeps_card_readable():
    """脱敏只作用于出网的那一份；模型输出必须还原成真实资产。

    不还原的话，飞书卡片上会显示「<host-1> 疑似 NodeNotReady」这种占位符。
    """
    from app.integrations.deepseek import _mask, _unmask

    context = {
        "incident": {"root_entity": "node:gpu-node-021", "node": "gpu-node-021", "cluster": "h800-prod"},
        "alerts": [{"entity": "192.0.2.21:9100", "summary": "gpu-node-021 not ready"}],
    }
    mapping: dict[str, str] = {}
    masked = _mask(context, mapping, {})
    prompt = json.dumps(masked, ensure_ascii=False)

    # 出网的那一份必须不含真实资产信息
    assert "192.0.2.21" not in prompt
    assert "gpu-node-021" not in prompt
    assert mapping, "必须保留映射关系用于还原"

    host_token = mapping["gpu-node-021"]
    ip_token = mapping["192.0.2.21"]
    reply = json.dumps(
        {
            "summary": f"节点 {host_token} 疑似宕机",
            "suspected_root_cause": f"{ip_token} 失联",
            "confidence": 0.5,
            "evidence": [{"type": "metric", "description": f"{ip_token} 掉线"}],
        },
        ensure_ascii=False,
    )

    restored = _unmask(reply, mapping)
    assert "gpu-node-021" in restored
    assert "192.0.2.21" in restored
    assert host_token not in restored and ip_token not in restored


def test_mask_keeps_entity_type_prefix():
    """实体类型前缀不是敏感信息，掩掉它会让模型以为实体名是占位符。

    实测踩过：模型在建议里写「entity 中 node 为占位形式，需人工确认真实节点名」，
    因为出网 prompt 里写成了 <host-1>:<host-2>。
    """
    from app.integrations.deepseek import _mask

    masked = _mask("node:192.0.2.21", {}, {})
    assert masked.startswith("node:")
    assert "192.0.2.21" not in masked

    # 真实主机名照旧要掩
    hostname_masked = _mask("gpu-node-021 not ready", {}, {})
    assert "gpu-node-021" not in hostname_masked


def test_context_substitution_falls_back_to_hostname():
    """告警实体是主机名（无 IP）时，查询模板也必须能跑，不能整条跳过。"""
    from app.services.context_collector import _substitute

    class FakeAlert:
        ip = None
        hostname = None
        entity_id = "bm-example-zone1-d-3090-24g-26-209"
        node = None
        namespace = None

    query = _substitute('up{instance=~"$target.*"}', FakeAlert())
    assert query == 'up{instance=~"bm-example-zone1-d-3090-24g-26-209.*"}'
    # 变量确实缺失时仍然跳过
    assert _substitute('kube_pod_status_ready{namespace="$namespace"}', FakeAlert()) is None


def test_instance_regex_covers_hostname_and_ip():
    """节点名与 IP 两种 target 注册形态都要能命中（实测 cluster02 两种都有）。"""
    import re as _re

    from app.services.context_collector import _instance_regex, _substitute

    class FakeAlert:
        ip = None
        hostname = None
        entity_id = "p-phy-klx-calc-node-001"
        node = "p-phy-klx-calc-node-001"
        namespace = None
        enrichment = None

    class FakeClient:
        configured = True
        calls = 0

        def instant(self, query):
            FakeClient.calls += 1
            return [{"metric": {"internal_ip": "192.0.2.12"}}]

    pattern = _instance_regex(FakeClient(), FakeAlert())
    assert _re.match(pattern + ".*", "p-phy-klx-calc-node-001:9100")
    assert _re.match(pattern + ".*", "192.0.2.12:9100")
    assert not _re.match(pattern + ".*", "other-node-999:9100")
    assert FakeClient.calls == 1, "富化结果里没有 internal_ip 时才应该去查 Prometheus"

    # 拼进 PromQL 前必须把反斜杠再转义一层，否则 `\.` 是非法转义、查询直接报错返回空
    query = _substitute('up{instance=~"$target.*"}', FakeAlert(), pattern)
    assert "\\\\." in query or "\\\\-" in query
    assert 'up{instance=~"' in query


def test_instance_regex_uses_enrichment_and_cache():
    """internal_ip 优先取自富化结果 / 本次采集缓存，避免同节点重复查 Prometheus。

    一场风暴里同一个节点会被 N 条告警问到，早期实现每条都查一次 kube_node_info。
    """
    import re as _re

    from app.services.context_collector import _instance_regex

    class FakeAlert:
        ip = None
        hostname = None
        entity_id = "node-a"
        node = "node-a"
        namespace = None
        enrichment = {"kube_node": {"internal_ip": "192.0.2.9"}}

    class BoomClient:
        configured = True

        def instant(self, query):  # pragma: no cover - 不该被调用
            raise AssertionError("富化结果里已有 internal_ip 时不该再查 Prometheus")

    pattern = _instance_regex(BoomClient(), FakeAlert())
    assert _re.match(pattern + ".*", "192.0.2.9:9100")

    # 富化结果缺失时查一次，并写进缓存；第二条同节点告警走缓存
    class CountingClient:
        configured = True
        calls = 0

        def instant(self, query):
            CountingClient.calls += 1
            return [{"metric": {"internal_ip": "192.0.2.10"}}]

    class BareAlert(FakeAlert):
        enrichment = None

    cache: dict[str, str] = {}
    _instance_regex(CountingClient(), BareAlert(), cache)
    _instance_regex(CountingClient(), BareAlert(), cache)
    assert CountingClient.calls == 1, "同节点的第二次解析必须命中缓存"


def test_context_collection_failure_is_recorded(client):
    """采集失败必须写回 incident.context，否则接口与卡片上看不出「拿不到数据」。"""
    send(client, payload("NodeNotReady", "node", "node01", seconds=0, node="node01"))
    incident_id = get_incidents(client)[0]["incident_id"]
    detail = client.get(f"/api/v1/incidents/{incident_id}").json()
    # 未配置 Prometheus 时返回的是正常降级结构，不是错误
    context = detail["context"] or {}
    assert "error" not in context
    assert (context.get("prometheus") or {}).get("status") == "NOT_CONFIGURED"


def test_mask_keeps_metric_and_component_names():
    """指标名/组件名/规则名不是资产，不能被掩。

    实测踩过：NodeDiskIoBusy 被掩成 <host-1>、node-exporter 被掩成 <host-2>、
    整个 prompt 出现 200 处 <host->，模型只能写「指标名被掩码，无法确认是哪个计数器」。
    """
    from app.integrations.deepseek import _mask

    payload = {
        "alertname": "NodeDiskIoBusy",
        "root_alertname": "NodeDiskIoBusy",
        "job": "node-exporter-metrics",
        "container": "node-exporter",
        "namespace": "monitoring",
        "device": "nvme0n1",
        "instance": "192.0.2.176:9100",
        "node": "bm-example-zone1-d-a100-40g-2-176",
        "query": 'rate(node_disk_io_time_seconds_total{instance=~"192.0.2.176:9100"}[5m])',
    }
    mapping: dict[str, str] = {}
    masked = _mask(payload, mapping, {})

    # 组件名 / 指标名 / 规则名必须原样保留
    assert masked["alertname"] == "NodeDiskIoBusy"
    assert masked["root_alertname"] == "NodeDiskIoBusy"
    assert masked["job"] == "node-exporter-metrics"
    assert masked["container"] == "node-exporter"
    assert masked["namespace"] == "monitoring"
    assert masked["device"] == "nvme0n1"
    # 指标名保留，但查询语句里的真实实例仍要掩掉
    assert "node_disk_io_time_seconds_total" in masked["query"]
    assert "192.0.2.176" not in masked["query"]
    # 资产标识照旧掩掉
    assert "192.0.2.176" not in masked["instance"]
    assert "bm-example-zone1-d-a100-40g-2-176" not in masked["node"]

    # 自由文本里的主机名也要掩（annotation/summary 里会带）
    text = _mask("节点 bm-example-zone1-d-a100-40g-2-176 磁盘 192.0.2.176 繁忙", {}, {})
    assert "bm-example-zone1-d-a100-40g-2-176" not in text
    assert "192.0.2.176" not in text
    # 但不能误伤组件名
    assert _mask("job=node-exporter-metrics", {}, {}) == "job=node-exporter-metrics"
    # 时间戳也不能被多段连字符规则误伤
    assert _mask("first_seen=2026-09-11T07:00:00+00:00", {}, {}) == "first_seen=2026-09-11T07:00:00+00:00"


def test_topology_upsert_is_idempotent_within_session(session):
    """同一 session 内重复登记同一关系不能重复插入。

    实测踩过：注入 K8s API 后，kube-state-metrics 富化与 K8s API 富化都会登记
    Pod RUNS_ON Node / OWNED_BY，而 autoflush=False 让 upsert 的前置 SELECT
    看不到尚未 flush 的那条 → 重复 add → 下一次 flush 撞 resource_relations
    的唯一约束 → 整个事件处理失败。
    """
    from sqlalchemy import func, select

    from app.correlation.topology import TopologyService
    from app.db.models import ResourceRelation

    TopologyService(session).upsert("pod", "p1", "RUNS_ON", "node", "n1")
    TopologyService(session).upsert("pod", "p1", "RUNS_ON", "node", "n1")  # 另一个实例，同一 session
    session.flush()
    assert session.scalar(select(func.count()).select_from(ResourceRelation)) == 1


def test_mask_keeps_business_ids():
    """工单号/告警号不是资产，不能被掩成 <host-N>。

    实测踩过：INC-20260911-001 / ALT-<hex>-<ts> 正好命中「多段连字符 + 含数字」规则，
    被当成主机名掩掉，模型无法引用具体对象、还占用了 host 计数。
    """
    from app.integrations.deepseek import _mask

    masked = _mask(
        {
            "incident_id": "INC-20260911-001",
            "alert_id": "ALT-dd2e4391-20260911084251024706",
            "entity": "node:bm-example-zone1-d-a100-40g-2-176",
        },
        {},
        {},
    )
    assert masked["incident_id"] == "INC-20260911-001"
    assert masked["alert_id"] == "ALT-dd2e4391-20260911084251024706"
    # 资产照旧要掩（保留类型前缀）
    assert masked["entity"] == "node:<host-1>"


def test_incident_card_is_sent_once(client):
    """同一工单同一 kind 的卡片只发一次（后台重试/重启补偿不得重复推）。"""
    from sqlalchemy import func, select

    from app.db.models import FeishuMessage
    from app.db.session import SessionLocal
    from app.db import queries
    from app.services import pipeline

    send(client, payload("NodeNotReady", "node", "node01", seconds=0, node="node01"))
    incident_id = get_incidents(client)[0]["incident_id"]

    session = SessionLocal()
    try:
        incident = queries.get_incident(session, incident_id)
        session.add(FeishuMessage(channel="incident", incident_id=incident_id, kind="incident_created", ok=True))
        session.flush()
        before = session.scalar(select(func.count()).select_from(FeishuMessage))
        pipeline.send_incident_card(session, incident, "incident_created")
        session.flush()
        assert session.scalar(select(func.count()).select_from(FeishuMessage)) == before
    finally:
        session.close()


def test_worker_coalesces_requests_into_latest_kind(monkeypatch):
    """同一工单在跑期间来的新请求不能被静默丢弃：合并成「最新 kind」，跑完补一次。

    实测踩过：分析还在跑时人工点 /analyze，请求被直接丢掉（接口却返回 202），
    工单状态随后被写成 DONE，sweeper 也不会重排 → 人工重跑永久失效。
    """
    from app.services import worker

    worker._INFLIGHT.clear()
    worker._PENDING.clear()
    submitted: list[tuple] = []
    monkeypatch.setattr(worker, "_submit", lambda fn, *args: submitted.append((fn, args)) or True)

    worker.request_analysis("INC-1", "incident_created")
    worker.request_analysis("INC-1", "manual_reanalyze")  # 运行中 → 记成待跑
    assert len(submitted) == 1, "同一工单同时只跑一个"
    assert worker._PENDING["INC-1"] == "manual_reanalyze"

    submitted.clear()
    worker._drain_pending("INC-1", "incident_created")
    assert submitted == [(worker._analyze_job, ("INC-1", "manual_reanalyze"))]


def test_transient_db_lock_keeps_analysis_retryable():
    """database is locked 不该定格成 FAILED（FAILED 不自动重试，诊断会永久缺失）。"""
    from sqlalchemy.exc import OperationalError

    from app.services.pipeline import _is_transient

    assert _is_transient(OperationalError("select 1", {}, Exception("database is locked")))
    assert _is_transient(TimeoutError("operation timed out"))
    assert not _is_transient(ValueError("模型输出不是 JSON"))


def test_clip_truncates_metric_series():
    """超长 context 要裁剪指标序列并标记 truncated（批量裁剪，不做 O(n²) 重复序列化）。"""
    from app.integrations.deepseek import _clip

    context = {
        "prometheus": {
            "series": [
                {"metric": {"__name__": "node_load5", "i": index}, "query": "x" * 200, "min": 1, "max": 2}
                for index in range(400)
            ]
        }
    }
    clipped = _clip(context, max_chars=6000)
    assert clipped["truncated"] is True
    assert 0 < len(clipped["prometheus"]["series"]) < 400
    assert _clip({"a": "b"}, max_chars=6000) == {"a": "b"}


def test_locked_event_retry_rolls_back_and_recovers(client, monkeypatch):
    """database is locked 必须「回滚最外层事务再重试」才有效。

    实测背景（2026-09-14 丢了一条告警）：savepoint 内层重试不会换快照，
    同一请求 4 次重试全败 → 事件永久丢失。这里模拟第一次抛 locked、第二次成功。
    """
    from sqlalchemy.exc import OperationalError

    from app.metrics import get_counter
    from app.services import pipeline

    calls = {"n": 0}
    real = pipeline.process_one

    def flaky(session, item, raw_cache=None):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OperationalError(
                "INSERT INTO raw_events ...", {}, Exception("database is locked")
            )
        return real(session, item, raw_cache=raw_cache)

    monkeypatch.setattr(pipeline, "process_one", flaky)
    before = get_counter("aiops_events_locked_retry_total")

    body = send(client, payload("NodeNotReady", "node", "node01", seconds=0, node="node01"))[0]

    assert calls["n"] == 2, "第一次 locked 后必须重试"
    assert body["processed"] == 1 and body["failed"] == 0
    assert get_incidents(client), "重试成功后工单要建出来"
    assert get_counter("aiops_events_locked_retry_total") == before + 1


def test_locked_event_gives_up_after_retries(client, monkeypatch):
    """一直 locked 时：返回 FAILED、计失败数、且不留下半截数据。"""
    from sqlalchemy.exc import OperationalError

    from app.metrics import get_counter
    from app.services import pipeline

    def always_locked(session, item, raw_cache=None):
        raise OperationalError("INSERT INTO raw_events ...", {}, Exception("database is locked"))

    monkeypatch.setattr(pipeline, "process_one", always_locked)
    before = get_counter("aiops_events_failed_total", {"reason": "processing"})

    # 全部失败时入口返回 422（便于夜莺侧看到明确失败），不是 200
    response = client.post(
        "/api/v1/events/nightingale", json=payload("NodeNotReady", "node", "node01", seconds=0, node="node01")
    )
    assert response.status_code == 422, response.text
    assert response.json()["detail"]["failed"] == 1
    assert get_counter("aiops_events_failed_total", {"reason": "processing"}) == before + 1
    assert not get_incidents(client), "失败的事件不该留下工单"


def test_items_are_committed_individually(session):
    """每条事件独立提交：第一条成功后第二条失败，第一条不能跟着回滚。"""
    from sqlalchemy import func, select
    from sqlalchemy.exc import OperationalError

    from app.db.models import RawEvent
    from app.db.session import SessionLocal
    from app.services import pipeline

    first = payload("NodeNotReady", "node", "node01", seconds=0, node="node01")
    second = payload("NodeNotReady", "node", "node02", seconds=0, node="node02")

    real = pipeline.process_one
    seen = {"n": 0}

    def flaky(session_, item, raw_cache=None):
        seen["n"] += 1
        if seen["n"] == 2:
            raise RuntimeError("boom")
        return real(session_, item, raw_cache=raw_cache)

    original = pipeline.process_one
    pipeline.process_one = flaky
    try:
        results = pipeline.process_items(session, [first, second])
    finally:
        pipeline.process_one = original

    assert [r.status for r in results] == ["PROCESSED", "FAILED"]

    fresh = SessionLocal()
    try:
        kept = fresh.scalar(select(func.count()).select_from(RawEvent))
    finally:
        fresh.close()
    assert kept == 1, "第一条应已提交（独立事务），不因第二条失败而回滚"


def test_raw_archive_has_no_duplicate_lines_on_retry(client, monkeypatch):
    """重试期间归档只写一次：早期版本一次失败会在 JSONL 里留 4 行重复。"""
    from sqlalchemy.exc import OperationalError

    from app.config import settings
    from app.services import pipeline

    real = pipeline.process_one
    calls = {"n": 0}

    def flaky(session, item, raw_cache=None):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OperationalError("INSERT ...", {}, Exception("database is locked"))
        return real(session, item, raw_cache=raw_cache)

    monkeypatch.setattr(pipeline, "process_one", flaky)

    day_file = next(iter(sorted(settings.raw_dir.glob("*.jsonl"))), None)
    before = set(day_file.read_text().splitlines()) if day_file else set()

    send(client, payload("NodeNotReady", "node", "node01", seconds=0, node="node01"))

    day_file = next(iter(sorted(settings.raw_dir.glob("*.jsonl"))), None)
    assert day_file is not None
    new_lines = [line for line in day_file.read_text().splitlines() if line.strip() and line not in before]
    assert len(new_lines) == 1, f"重试不该产生重复归档行，实际新增 {len(new_lines)} 行"


def test_dry_run_not_counted_as_sent(session):
    """未配置 webhook 时走干跑，只能计 dry_run，不能计 sent（否则监控会骗人）。"""
    from app.integrations import feishu
    from app.metrics import get_counter

    dry_before = get_counter("aiops_feishu_dry_run_total", {"channel": "event"})
    sent_before = get_counter("aiops_feishu_sent_total", {"channel": "event", "ok": "true"})

    feishu._post("event", {"msg_type": "text", "content": {"text": "x"}}, session, kind="text")

    assert get_counter("aiops_feishu_dry_run_total", {"channel": "event"}) == dry_before + 1
    assert get_counter("aiops_feishu_sent_total", {"channel": "event", "ok": "true"}) == sent_before


def test_node_ip_is_corrected_to_internal_ip():
    """节点 IP 必须以 kube_node_info.internal_ip 为准，覆盖采集端地址。

    实测（2026-09-14）：节点告警来自 kube-state-metrics，告警里的 ip 是 198.51.100.210
    （4 台节点共用），如果不纠正，恢复验证会拿这个永远 up=1 的地址判「已恢复」。
    """
    from app.db.session import SessionLocal, init_db
    from app.models.schemas import Entity, NormalizedEvent, Scope
    from app.services.enricher import _k8s_metric_facts

    class FakePrometheus:
        configured = True

        def instant(self, query):
            if "kube_node_info" in query and "node=" in query:
                return [{"metric": {"node": "master-39", "internal_ip": "192.0.2.39",
                                    "kubelet_version": "v1.31.14", "__prom_env__": "dx0-calc1-dev"}}]
            return []

    init_db()
    session = SessionLocal()
    try:
        norm = NormalizedEvent(
            event_id="e", source="nightingale", event_type="alert",
            alertname="center-k8s-master-node-not-ready",
            entity=Entity(type="node", id="master-39", node="master-39", ip="198.51.100.210"),
            scope=Scope(), occurred_at=datetime.now(timezone.utc), status="FIRING",
        )
        facts = _k8s_metric_facts(session, norm, FakePrometheus())
        assert facts, "应能取到 kube_node_info"
        assert norm.entity.ip == "192.0.2.39", "节点 IP 必须被纠正为 internal_ip"
        assert facts["kube_node"]["ip_corrected_from"] == "198.51.100.210", "要留下纠正痕迹便于排查"
    finally:
        session.close()


def test_verify_recovery_requires_real_evidence(monkeypatch, session):
    """恢复判定三态：节点未就绪=not_recovered、Ready 回来了=recovered、查不到序列=unverified。

    重点：查不到证据时**绝不能**当「已恢复」（之前用采集端地址的 up 永远为 1，导致假恢复）。
    """
    from app.db.models import Alert, Incident, IncidentAlert
    from app.services import sweeper
    from app.timeutil import utcnow

    def make_incident() -> Incident:
        incident = Incident(
            incident_id="INC-T-1",
            title="t",
            status="RECOVERING",
            severity="P2",
            root_entity_type="node",
            root_entity_id="master-39",
            node="master-39",
            alert_count=1,
            first_seen=utcnow(),
            last_seen=utcnow(),
            recovery_deadline=utcnow(),
        )
        session.add(incident)
        session.add(
            Alert(
                alert_id="ALT-T-1",
                fingerprint="f-verify",
                alertname="center-k8s-master-node-not-ready",
                status="RESOLVED",
                resolution_reason="resolved",
                entity_type="node",
                entity_id="master-39",
                node="master-39",
                ip="198.51.100.210",
                first_seen=utcnow(),
                last_seen=utcnow(),
                incident_id="INC-T-1",
            )
        )
        # attached_alerts 走的是 incident_alerts 关联表，必须一起补上
        session.add(IncidentAlert(incident_id="INC-T-1", alert_id="ALT-T-1", relation_type="ROOT"))
        session.flush()
        return incident

    class FakePrometheus:
        def __init__(self, ready=None, up_rows=None):
            self.configured = True
            self._ready = ready
            self._up = up_rows if up_rows is not None else []

        def instant(self, query):
            if "kube_node_status_condition" in query:
                return [] if self._ready is None else [{"metric": {}, "value": [0, self._ready]}]
            if query.startswith("up{"):
                return self._up
            return []

    # ① 节点还没 Ready → 不恢复
    incident = make_incident()
    monkeypatch.setattr(sweeper, "get_prometheus_client", lambda: FakePrometheus(ready="0"))
    state, detail = sweeper.verify_recovery(session, incident)
    assert state == "not_recovered" and detail["reason"] == "node_not_ready"

    # ② 节点 Ready 回来了 → 恢复（判据来自 K8s 指标，不是那个采集端地址）
    monkeypatch.setattr(sweeper, "get_prometheus_client", lambda: FakePrometheus(ready="1"))
    state, detail = sweeper.verify_recovery(session, incident)
    assert state == "recovered" and detail["verified_by"] == "kube_state_metrics"

    # ③ 什么都查不到 → unverified（不是 recovered！）
    monkeypatch.setattr(sweeper, "get_prometheus_client", lambda: FakePrometheus())
    state, detail = sweeper.verify_recovery(session, incident)
    assert state == "unverified", "查不到证据时不能判恢复"


def test_incident_card_title_and_node_details():
    """合并工单的卡片：标题=告警等级+故障标题；节点详情写全所有机器（用户 2026-09-14 要求）。"""
    import json as _json
    from types import SimpleNamespace

    from app.integrations.feishu import build_incident_card

    def make_alert(index: int, node: str, ip: str, alertname: str):
        return SimpleNamespace(
            alert_id=f"ALT-{index}", alertname=alertname, node=node, ip=ip, cluster="center",
            namespace="kube-system", zone="zone1", accelerator_model="A100-40G", severity="P2",
            status="FIRING", occurrence_count=1, enrichment_status="FULL", summary=None,
            entity_type="node", entity_id=node, first_seen=None, last_seen=None,
        )

    incident = SimpleNamespace(
        incident_id="INC-20260914-099",
        title="中心集群 4 台 A100 master 节点同一时刻 NotReady",
        severity="P2", status="OPEN", alert_count=4,
        root_entity_type="node", root_entity_id="master-39", node="master-39", cluster="center",
        first_seen=None, last_seen=None, resolved_at=None, suspected_root_cause=None,
        context=None, region=None, zone=None, projset=None, project=None, ai_diagnosis=None,
    )
    alerts = [
        make_alert(1, "master-39", "192.0.2.39", "center-k8s-master-node-not-ready"),
        make_alert(2, "master-27", "192.0.2.27", "center-k8s-master-node-not-ready"),
        make_alert(3, "master-28", "192.0.2.28", "center-k8s-master-node-not-ready"),
        make_alert(4, "master-123", "192.0.2.123", "KubeletDown"),
    ]
    card = build_incident_card(incident, alerts, kind="incident_created")
    title = card["card"]["header"]["title"]["content"]
    text = _json.dumps(card, ensure_ascii=False)

    assert title.startswith("🚨 P2 · "), f"标题应为「等级 + 故障标题」，实际: {title}"
    assert "中心集群 4 台 A100 master 节点同一时刻 NotReady" in title
    assert "节点详情" in text and "（4 台）" in text
    # 跳转按钮：卡片要能一键打开 Web 工单页（用户 2026-09-15 要求）
    buttons = [
        action
        for element in card["card"]["elements"]
        if element.get("tag") == "action"
        for action in element.get("actions", [])
    ]
    assert buttons, "卡片必须带「查看工单详情」跳转按钮"
    labels = {button["text"]["content"]: button["url"] for button in buttons}
    assert labels.get("查看工单详情", "").endswith(f"/ui/incidents/{incident.incident_id}"), labels
    assert labels.get("未恢复工单列表", "").endswith("/ui/incidents"), labels
    for node, ip in (("master-39", "192.0.2.39"), ("master-27", "192.0.2.27"),
                     ("master-28", "192.0.2.28"), ("master-123", "192.0.2.123")):
        assert node in text and ip in text, f"节点详情必须写全：{node}/{ip} 缺失"
    assert "根因" in text  # 标出根因那台

    # 单机工单不重复出「节点详情」段
    single = build_incident_card(incident, alerts[:1], kind="incident_created")
    assert "节点详情" not in _json.dumps(single, ensure_ascii=False)


def test_ui_incidents_page_lists_and_expands(client):
    """Web 工单页（用户 2026-09-15 要求）：打开就是列表，点一下就地展开详情；单工单页自动展开。"""
    send(client, payload("NodeNotReady", "node", "node01", seconds=0, node="node01"))
    incident_id = get_incidents(client)[0]["incident_id"]

    page = client.get("/ui/incidents")
    assert page.status_code == 200, page.text
    assert "text/html" in page.headers["content-type"]
    text = page.text
    assert incident_id in text and "node01" in text
    for section in ("<details", "节点详情", "关联告警", "时间线"):
        assert section in text, f"页面缺少 {section}"

    detail = client.get(f"/ui/incidents/{incident_id}")
    assert detail.status_code == 200
    assert f'id="{incident_id}" open' in detail.text, "点卡片链接进来必须自动展开这张工单"

    assert client.get("/ui").status_code == 200  # /ui → 列表
    assert client.get("/ui/incidents?scope=all").status_code == 200


def test_ui_page_is_read_only(client):
    """页面只读：不带鉴权也不接受写操作（只注册 GET）。"""
    from app.main import app

    ui_methods = {
        getattr(route, "methods", set())
        for route in app.router.routes
        for _ in [0]
        if getattr(route, "path", "").startswith("/ui")
    }
    for methods in ui_methods:
        assert methods <= {"GET", "HEAD"}, f"/ui 只应有 GET/HEAD，实际 {methods}"


def test_incident_id_never_reuses_a_notified_number(client, session):
    """工单号必须单调递增：删掉工单后也不能复用号段，否则撞上飞书留档会让新卡被静默压掉。

    2026-09-15 实测：清理测试单后号段复用 → already_notified 误判 → 用户手动触发的
    GPU XID 告警建单时卡片没发出去。
    """
    from sqlalchemy import delete as sql_delete
    from sqlalchemy import select

    from app.db.models import FeishuMessage, Incident, IncidentEvent
    from app.db.session import SessionLocal
    from app.services import incident_service
    from app.timeutil import utcnow

    moment = utcnow()
    first = incident_service.next_incident_id(session, moment)
    session.add(
        Incident(incident_id=first, title="t", status="OPEN", severity="P2", first_seen=moment, last_seen=moment)
    )
    session.flush()
    session.add(FeishuMessage(channel="incident", incident_id=first, kind="incident_created", ok=True))
    session.flush()
    assert incident_service.next_incident_id(session, moment) != first

    # 删掉工单（但飞书留档还在）→ 下一个号必须是新号，不能复用 first
    session.execute(sql_delete(IncidentEvent).where(IncidentEvent.incident_id == first))
    session.execute(sql_delete(Incident).where(Incident.incident_id == first))
    session.flush()
    assert (
        incident_service.next_incident_id(session, moment) != first
    ), "号段复用了会撞上飞书留档，新卡会被 already_notified 静默压掉"
    # 飞书留档仍在（append-only），正是它把号段"记住"了
    assert session.scalar(select(FeishuMessage.id).where(FeishuMessage.incident_id == first)) is not None


def test_recovery_up_query_matches_real_instance(monkeypatch, session):
    """up{} 兜底判据必须能匹配真实 instance（PromQL 的 =~ 两端自动锚定，写成 "^IP:" 永远查不到）。

    2026-09-16 实测踩到：GPU XID 工单因这条查询永远返回 0 条序列 → 一直 unverified → 永不自动关单。
    """
    from app.db.models import Alert, Incident, IncidentAlert
    from app.services import sweeper
    from app.timeutil import utcnow

    seen: list[str] = []

    class FakePrometheus:
        configured = True

        def instant(self, query: str):
            seen.append(query)
            if query.startswith("up{"):
                # 只有真正能匹配 "198.51.100.9:9400" 的写法才返回数据：
                # 尾部带 (:.*)? 才算对（"^IP:" 这种写法在 PromQL 里永远匹配不到）
                matched = "(:.*)?" in query
                return [{"metric": {}, "value": [0, "1"]}] if matched else []
            return []

    incident = Incident(
        incident_id="INC-UP-1", title="t", status="RECOVERING", severity="P2",
        root_entity_type="gpu", root_entity_id="198.51.100.9:9400:gpunvidia6",
        alert_count=1, first_seen=utcnow(), last_seen=utcnow(), recovery_deadline=utcnow(),
    )
    session.add(incident)
    session.add(
        Alert(
            alert_id="ALT-UP-1", fingerprint="f-up", alertname="nvidia-gpu-xid-error", status="RESOLVED",
            resolution_reason="resolved", entity_type="gpu", entity_id="198.51.100.9:9400:gpunvidia6",
            ip="198.51.100.9", first_seen=utcnow(), last_seen=utcnow(), incident_id="INC-UP-1",
        )
    )
    session.add(IncidentAlert(incident_id="INC-UP-1", alert_id="ALT-UP-1", relation_type="ROOT"))
    session.flush()

    monkeypatch.setattr(sweeper, "get_prometheus_client", lambda: FakePrometheus())
    state, detail = sweeper.verify_recovery(session, incident)

    up_queries = [query for query in seen if query.startswith("up{")]
    assert up_queries, "应该查过 up{}"
    assert any("(:.*)?" in query for query in up_queries), f"尾部必须带 (:.*)?，实际 {up_queries}"
    assert state == "recovered", f"exporter 在线就该判恢复，实际 {state} / {detail}"


def test_gpu_alert_lifts_node_from_dcgm_tags():
    """DCGM 类告警的 tags 里有 Hostname/kubernetes_node → 必须抬成 node。

    否则（2026-09-16 实测）：GPU XID 告警的 node 是空的 → 卡片「节点」列空白、
    无法按机器聚合，而且 target_ident 会被当成带端口的假节点名。
    """
    from app.services import normalizer

    labels = {
        "Hostname": "bm-example-zone1-d-a100-40g-2-99",
        "kubernetes_node": "bm-example-zone1-d-a100-40g-2-99",
        "instance": "198.51.100.9:9400",
        "job": "dcgm-exporter-metrics",
        "device": "nvidia6",
        "gpu": "6",
        "namespace": "airs",
        "severity": "P2",
    }
    raw = n9e_native()
    raw.update(
        {
            "rule_name": "nvidia-gpu-xid-error",
            "target_ident": "198.51.100.9:9400",
            "tags": [f"{key}={value}" for key, value in labels.items()],
            "tags_map": labels,
            "original_tags": [f"{key}={value}" for key, value in labels.items()],
        }
    )
    norm = normalizer.normalize(raw)

    assert norm.entity.type == "gpu"
    assert norm.entity.node == "bm-example-zone1-d-a100-40g-2-99", norm.entity.node
    assert norm.entity.hostname == "bm-example-zone1-d-a100-40g-2-99", norm.entity.hostname
    assert norm.entity.ip == "198.51.100.9"
    # 实体 id 仍是采集目标（target_ident 剥端口），但绝不能是"带端口的假节点名"
    assert ":" not in norm.entity.id, norm.entity.id


def test_non_node_entity_uses_exporter_up_not_node_ready(monkeypatch, session):
    """GPU 这类挂在节点上的实体：节点 Ready 不能代表它好了，判据要用采集端 up{}。

    反例：节点 Ready=true 但 dcgm-exporter 掉线（up=0）时若按节点判，会误判恢复。
    """
    from app.db.models import Alert, Incident, IncidentAlert
    from app.services import sweeper
    from app.timeutil import utcnow

    seen: list[str] = []

    class FakePrometheus:
        configured = True

        def instant(self, query: str):
            seen.append(query)
            if "kube_node_status_condition" in query:
                return [{"metric": {}, "value": [0, "1"]}]  # 节点本身是 Ready 的
            if query.startswith("up{"):
                return [{"metric": {}, "value": [0, "0"]}]  # 但采集端挂了
            return []

    node = "bm-example-zone1-d-a100-40g-2-99"
    incident = Incident(
        incident_id="INC-GPU-1", title="GPU", status="RECOVERING", severity="P2",
        root_entity_type="gpu", root_entity_id=f"{node}:gpunvidia6", node=node, alert_count=1,
        first_seen=utcnow(), last_seen=utcnow(), recovery_deadline=utcnow(),
    )
    session.add(incident)
    session.add(
        Alert(
            alert_id="ALT-GPU-1", fingerprint="f-gpu", alertname="nvidia-gpu-xid-error", status="RESOLVED",
            resolution_reason="resolved", entity_type="gpu", entity_id=f"{node}:gpunvidia6",
            ip="198.51.100.9", node=node, first_seen=utcnow(), last_seen=utcnow(), incident_id="INC-GPU-1",
        )
    )
    session.add(IncidentAlert(incident_id="INC-GPU-1", alert_id="ALT-GPU-1", relation_type="ROOT"))
    session.flush()

    monkeypatch.setattr(sweeper, "get_prometheus_client", lambda: FakePrometheus())
    state, detail = sweeper.verify_recovery(session, incident)

    assert any(query.startswith("up{") for query in seen), f"必须查过采集端 up{{}}，实际 {seen}"
    assert state == "not_recovered", f"采集端 up=0 就不能判恢复，实际 {state} / {detail}"


def test_k8s_facts_resolve_node_by_internal_ip():
    """告警实体是 IP 时，要用 internal_ip 反查节点名（实测 192.0.2.46 这种）。"""
    from app.db.session import SessionLocal, init_db
    from app.models.schemas import Entity, NormalizedEvent, Scope
    from app.services.enricher import _k8s_metric_facts

    class FakePrometheus:
        configured = True

        def instant(self, query):
            if 'node="10.01"' in query:
                return []
            if 'internal_ip="192.0.2.46"' in query:
                return [{"metric": {"node": "gpu-a100-node-046", "internal_ip": "192.0.2.46",
                                    "kubelet_version": "v1.31.14", "__prom_env__": "dx0-calc1-dev"}}]
            if "kube_node_labels" in query:
                return []
            return []

    init_db()
    session = SessionLocal()
    try:
        norm = NormalizedEvent(
            event_id="e", source="nightingale", event_type="alert", alertname="NodeNetworkErrorOrDrop",
            entity=Entity(type="node", id="192.0.2.46", ip="192.0.2.46"),
            scope=Scope(), occurred_at=datetime.now(timezone.utc), status="FIRING",
        )
        facts = _k8s_metric_facts(session, norm, FakePrometheus())
        assert facts, "IP 形态的节点告警必须能反查到 K8s 事实"
        assert norm.entity.node == "gpu-a100-node-046"
        assert norm.entity.hostname == "gpu-a100-node-046"
        assert norm.scope.cluster == "dx0-calc1-dev"
        assert facts["kube_node"]["kubelet_version"] == "v1.31.14"
    finally:
        session.close()


def test_async_webhook_defers_analysis(client, monkeypatch):
    """副作用出锁：webhook 只落 PENDING 并把分析排进后台，请求内不跑采集/诊断。

    这是「夜莺不再超时」的核心保证：请求里既没有 Prometheus/K8s 查询，也没有 LLM 调用。
    """
    from app.config import settings
    from app.services import worker

    monkeypatch.setattr(settings, "inline_analysis", False)
    queued: list[tuple[str, str]] = []
    monkeypatch.setattr(worker, "request_analysis", lambda iid, kind: queued.append((iid, kind)))
    monkeypatch.setattr(worker, "request_alert_notification", lambda aid, kind: None)

    send(client, payload("NodeNotReady", "node", "node01", seconds=0, node="node01"))
    assert len(queued) == 1, "必须把分析任务排进后台"
    incident_id, kind = queued[0]
    assert kind == "incident_created"

    detail = client.get(f"/api/v1/incidents/{incident_id}").json()
    assert detail["analysis_status"] == "PENDING"
    assert detail["ai_diagnosis"] is None, "请求内不应跑 AI 诊断"
    assert detail["context"] is None, "请求内不应跑上下文采集"


def test_async_worker_completes_analysis(client, monkeypatch):
    """后台任务跑完后：DONE + 有诊断 + attempts 递增。"""
    from app.config import settings
    from app.services import pipeline, worker

    monkeypatch.setattr(settings, "inline_analysis", False)
    monkeypatch.setattr(worker, "request_analysis", lambda iid, kind: None)
    monkeypatch.setattr(worker, "request_alert_notification", lambda aid, kind: None)

    send(client, payload("NodeNotReady", "node", "node01", seconds=0, node="node01"))
    incident_id = get_incidents(client)[0]["incident_id"]

    pipeline.analyze_incident_now(incident_id, "incident_created")

    detail = client.get(f"/api/v1/incidents/{incident_id}").json()
    assert detail["analysis_status"] == "DONE"
    assert detail["analysis_attempts"] == 1
    assert detail["analysis_error"] is None
    assert detail["ai_diagnosis"] is not None
    assert detail["context"] is not None


def test_requeue_pending_on_startup(client, monkeypatch):
    """重启补偿：停在 PENDING 的工单会被重新入队（否则永远没有诊断）。"""
    from app.config import settings
    from app.services import worker

    monkeypatch.setattr(settings, "inline_analysis", False)
    monkeypatch.setattr(worker, "request_analysis", lambda iid, kind: None)
    monkeypatch.setattr(worker, "request_alert_notification", lambda aid, kind: None)
    send(client, payload("NodeNotReady", "node", "node01", seconds=0, node="node01"))
    incident_id = get_incidents(client)[0]["incident_id"]

    seen: list[tuple[str, str]] = []
    monkeypatch.setattr(worker, "request_analysis", lambda iid, kind: seen.append((iid, kind)))
    assert worker.requeue_pending() == 1
    assert seen == [(incident_id, "incident_created")]


def test_sweeper_requeues_stuck_analysis(client, monkeypatch, session):
    """sweeper 兜底：卡住的分析会被重排；FAILED 不自动重试（避免重复推卡）。"""
    from datetime import timedelta

    from app.config import settings
    from app.services import sweeper, worker
    from app.timeutil import utcnow

    monkeypatch.setattr(settings, "inline_analysis", False)
    monkeypatch.setattr(worker, "request_analysis", lambda iid, kind: None)
    monkeypatch.setattr(worker, "request_alert_notification", lambda aid, kind: None)
    send(client, payload("NodeNotReady", "node", "node01", seconds=0, node="node01"))

    seen: list[tuple[str, str]] = []
    monkeypatch.setattr(worker, "request_analysis", lambda iid, kind: seen.append((iid, kind)))
    summary = sweeper.run_sweep(session, now=utcnow() + timedelta(seconds=3600))
    assert summary["analysis_requeued"] == 1
    assert seen and seen[0][1] == "incident_created"

    # 标成 FAILED 后不再自动重排
    session.execute(
        __import__("sqlalchemy").text("update incidents set analysis_status='FAILED'")
    )
    session.flush()
    seen.clear()
    summary = sweeper.run_sweep(session, now=utcnow() + timedelta(seconds=7200))
    assert summary["analysis_requeued"] == 0
    assert seen == []


def test_context_queries_default_fallback_is_reachable():
    """兜底模板必须真的能被读到。

    实测踩过：`default:` 写在 yaml 顶层，而 rules.py 只读 `queries:` 段，
    导致没有专属模板的告警类型 queries=0、证据里什么都没有。
    """
    from app.correlation.rules import get_rules

    rules = get_rules()
    assert rules.context_queries.get("default"), "default 兜底模板必须可读"
    assert rules.context_queries.get("NodeNotReady")
    # 未命中专属模板时按 default 取
    templates = rules.context_queries.get("某个不存在的告警名") or rules.context_queries.get("default", [])
    assert len(templates) >= 3


def test_debug_skip_dedup_allows_repeat(client, monkeypatch):
    """调试开关：同一份 body 可反复处理并新建工单；默认关闭时幂等生效。

    夜莺的「测试」按钮发的是固定事件，联调时需要反复触发整条链路，
    所以要有一个能显式绕开两层去重（raw 幂等 + Alert 指纹合并）的开关。
    """
    from app.config import settings

    body = payload("NodeNotReady", "node", "node01", seconds=0, node="node01")

    send(client, body)
    assert send(client, body)[0]["duplicated"] == 1  # 默认幂等生效
    assert len(get_incidents(client)) == 1

    monkeypatch.setattr(settings, "debug_skip_dedup", True)
    result = send(client, body)[0]["results"][0]
    # 调试模式跳过关联与合并，每次请求都产生一个新工单（走完整链路）
    assert result["action"] == "DEBUG_NEW_INCIDENT"
    assert len(get_incidents(client)) == 2
    monkeypatch.setattr(settings, "debug_skip_dedup", False)


def test_daily_cap_suppresses_second_card_same_day(client):
    """每天每单最多一张卡：当天已发过创建卡后，根因更新卡要被压掉。"""
    from sqlalchemy import func, select

    from app.db import queries
    from app.db.models import FeishuMessage
    from app.db.session import SessionLocal
    from app.services import pipeline

    send(client, payload("NodeNotReady", "node", "node01", seconds=0, node="node01"))
    incident_id = get_incidents(client)[0]["incident_id"]

    session = SessionLocal()
    try:
        incident = queries.get_incident(session, incident_id)
        # 当天已成功发过一张创建卡
        session.add(FeishuMessage(channel="incident", incident_id=incident_id, kind="incident_created", ok=True))
        session.flush()
        assert pipeline.sent_today(session, incident_id) is True

        before = session.scalar(select(func.count()).select_from(FeishuMessage))
        pipeline.send_incident_card(session, incident, "incident_root_cause_changed")
        session.flush()
        assert session.scalar(select(func.count()).select_from(FeishuMessage)) == before
    finally:
        session.close()


def test_daily_cap_never_suppresses_recovery_card(client):
    """恢复卡是终态、每单只可能一次，压掉会让人不知道故障已经好了 → 必须豁免。"""
    from sqlalchemy import select

    from app.db import queries
    from app.db.models import FeishuMessage
    from app.db.session import SessionLocal
    from app.services import pipeline

    send(client, payload("NodeNotReady", "node", "node01", seconds=0, node="node01"))
    incident_id = get_incidents(client)[0]["incident_id"]

    session = SessionLocal()
    try:
        session.add(FeishuMessage(channel="incident", incident_id=incident_id, kind="incident_created", ok=True))
        session.flush()
        incident = queries.get_incident(session, incident_id)
        pipeline.send_incident_card(session, incident, "incident_resolved")
        session.flush()
        kinds = list(session.scalars(select(FeishuMessage.kind)).all())
    finally:
        session.close()
    assert "incident_resolved" in kinds


def test_daily_cap_resets_next_day(client):
    """封顶按本地自然日算：昨天发过不影响今天。"""
    from sqlalchemy import select

    from app.db.models import FeishuMessage
    from app.db.session import SessionLocal
    from app.services import pipeline
    from app.timeutil import utcnow

    send(client, payload("NodeNotReady", "node", "node01", seconds=0, node="node01"))
    incident_id = get_incidents(client)[0]["incident_id"]

    session = SessionLocal()
    try:
        # 建单时已经推过一张卡（今天），把它整体挪到昨天 → 当天就该重新允许发卡
        yesterday = utcnow() - timedelta(days=1)
        rows = session.scalars(
            select(FeishuMessage).where(FeishuMessage.incident_id == incident_id)
        ).all()
        assert rows, "建单应至少推过一张工单卡，否则这个用例没测到东西"
        for row in rows:
            row.created_at = yesterday
        session.flush()
        assert pipeline.sent_today(session, incident_id) is False
    finally:
        session.close()


def _make_open_incident(session, incident_id: str = "INC-DIGEST-TEST"):
    """给汇总用例造一个未恢复工单（汇总默认"只在有事时发"）。"""
    from app.db.models import Incident
    from app.timeutil import utcnow

    incident = Incident(
        incident_id=incident_id,
        title="测试未恢复故障",
        status="OPEN",
        severity="P2",
        first_seen=utcnow(),
        last_seen=utcnow(),
    )
    session.add(incident)
    session.flush()
    return incident


def test_digest_is_due_once_per_day_after_configured_hour():
    """汇总的时机判定：到点前不发、到点后发、发过之后当天不再发。"""
    from app.db.models import FeishuMessage
    from app.db.session import SessionLocal
    from app.services import digest

    day = datetime(2026, 9, 15, tzinfo=timezone(timedelta(hours=8)))
    session = SessionLocal()
    try:
        assert digest.due(session, day.replace(hour=8, minute=59)) is False
        _make_open_incident(session)  # 汇总默认只在有未恢复工单时发
        assert digest.due(session, day.replace(hour=9, minute=0)) is True

        session.add(
            FeishuMessage(
                channel="incident",
                kind=digest.DIGEST_KIND,
                ok=True,
                created_at=day.replace(hour=9, minute=0).astimezone(timezone.utc).replace(tzinfo=None),
            )
        )
        session.flush()
        assert digest.due(session, day.replace(hour=9, minute=1)) is False
        # 次日重新到点
        assert digest.due(session, (day + timedelta(days=1)).replace(hour=9)) is True
    finally:
        session.close()


def test_digest_not_due_when_nothing_unresolved():
    """没有未恢复工单就不发（避免每天一张"平安卡"变成新噪声）；打开 always 才固定报到。"""
    from app.config import settings
    from app.db.session import SessionLocal
    from app.services import digest

    moment = datetime(2026, 9, 15, 9, 30, tzinfo=timezone(timedelta(hours=8)))
    session = SessionLocal()
    try:
        assert digest.due(session, moment) is False
        settings.daily_digest_always = True
        try:
            assert digest.due(session, moment) is True
        finally:
            settings.daily_digest_always = False
    finally:
        session.close()


def test_digest_failure_is_retried_same_day():
    """发失败（ok=False）不算发过：汇总卡没有别的补偿路径，下次巡检要重试。"""
    from app.db.models import FeishuMessage
    from app.db.session import SessionLocal
    from app.services import digest

    moment = datetime(2026, 9, 15, 9, 30, tzinfo=timezone(timedelta(hours=8)))
    session = SessionLocal()
    try:
        _make_open_incident(session)
        session.add(
            FeishuMessage(
                channel="incident",
                kind=digest.DIGEST_KIND,
                ok=False,
                created_at=moment.astimezone(timezone.utc).replace(tzinfo=None),
            )
        )
        session.flush()
        assert digest.due(session, moment) is True
    finally:
        session.close()


# ----------------------------------------------------------------------
# 主动恢复探测：人工处理完之后由系统自己发现"好了没有"（用户 2026-09-15 要求）
# ----------------------------------------------------------------------
class _FakeProbePrometheus:
    """按需返回节点 Ready 条件与 up{} 结果，并记录被查了什么。"""

    configured = True

    def __init__(self, ready: str | None = "1", up_rows: list | None = None):
        self._ready = ready
        self._up = up_rows if up_rows is not None else []
        self.queries: list[str] = []

    def instant(self, query: str) -> list[dict]:
        self.queries.append(query)
        if "kube_node_status_condition" in query:
            return [] if self._ready is None else [{"metric": {}, "value": [0, self._ready]}]
        if query.startswith("up{"):
            return self._up
        return []


def test_probe_resolves_incident_and_notifies_group(client, session, monkeypatch):
    """告警静默后主动探测：节点 Ready 回来了 → 关单 + 群里通报恢复卡。"""
    from app.services import sweeper
    from app.services.sweeper import run_sweep

    send(client, payload("NodeNotReady", "node", "node01", seconds=0, node="node01"))
    assert get_incidents(client, status="OPEN")
    assert incident_cards(session) == 1  # 只有创建卡

    fake = _FakeProbePrometheus(ready="1")
    monkeypatch.setattr(sweeper, "get_prometheus_client", lambda: fake)

    base = BASE.astimezone(timezone.utc)
    run_sweep(session, now=base + timedelta(seconds=1000))  # 静默超过 stale 窗口 → 进入待探测
    session.commit()

    incidents = get_incidents(client)
    assert incidents[0]["status"] == "RESOLVED", "探测到恢复就该关单"
    assert incident_cards(session) == 2, "恢复后要在群里通报一张恢复卡"
    detail = client.get(f"/api/v1/incidents/{incidents[0]['incident_id']}").json()
    resolved_events = [event for event in detail["timeline"] if event["event_type"] == "INCIDENT_RESOLVED"]
    assert resolved_events and resolved_events[-1]["content"]["detected_by"] == "probe"
    assert "kube_node_status_condition" in " ".join(fake.queries), "节点类必须查 K8s 权威判据"


def test_probe_keeps_open_when_not_recovered(client, session, monkeypatch):
    """探测到"还没好"就保持 OPEN，不推恢复卡，等次日汇总。"""
    from sqlalchemy import select

    from app.db.models import Incident
    from app.services import sweeper
    from app.services.sweeper import run_sweep

    send(client, payload("NodeNotReady", "node", "node01", seconds=0, node="node01"))
    fake = _FakeProbePrometheus(ready="0")  # 节点仍未就绪
    monkeypatch.setattr(sweeper, "get_prometheus_client", lambda: fake)

    base = BASE.astimezone(timezone.utc)
    run_sweep(session, now=base + timedelta(seconds=1000))
    session.commit()

    assert get_incidents(client, status="OPEN")
    assert incident_cards(session) == 1, "没恢复就不该有恢复卡"
    assert session.get(Incident, 1) is not None  # ORM 可见
    stored = session.execute(
        select(Incident.last_probe_result).where(Incident.incident_id == get_incidents(client)[0]["incident_id"])
    ).scalar()
    assert stored == "not_recovered", "探测结论要留痕"

    # 限流：同一工单间隔内不再重复探测
    calls = len(fake.queries)
    run_sweep(session, now=base + timedelta(seconds=1010))
    session.commit()
    assert len(fake.queries) == calls, "间隔未到不该重复探测"


def test_probe_does_nothing_without_metric_source(client, session):
    """没配指标源就不探测（探测不出 ≠ 已恢复），工单保持 OPEN。"""
    from app.services.sweeper import run_sweep

    send(client, payload("NodeNotReady", "node", "node01", seconds=0, node="node01"))
    base = BASE.astimezone(timezone.utc)
    run_sweep(session, now=base + timedelta(seconds=1000))
    session.commit()
    assert get_incidents(client, status="OPEN")
    assert incident_cards(session) == 1


def test_digest_card_lists_unresolved_incidents(client):
    """汇总卡要列出未恢复工单，并且按级别统计。"""
    from app.db import queries
    from app.db.session import SessionLocal
    from app.integrations.feishu import build_digest_card

    send(
        client,
        payload("NodeNotReady", "node", "node01", seconds=0, node="node01", severity="P0"),
        payload("DiskWillFull", "node", "node02", seconds=10, node="node02", severity="P2"),
    )

    session = SessionLocal()
    try:
        incidents = queries.unresolved_incidents(session)
        card = build_digest_card(incidents, datetime(2026, 9, 15, 9, 0, tzinfo=timezone(timedelta(hours=8))))
    finally:
        session.close()

    assert len(incidents) == 2
    text = json.dumps(card, ensure_ascii=False)
    assert "每日故障汇总" in text
    for incident in incidents:
        assert incident.incident_id in text
    assert "P0" in text and "P2" in text


def test_digest_card_reports_all_clear_when_nothing_open():
    """没有未恢复工单也发一张「平安卡」：静默无法区分「没故障」和「汇总挂了」。"""
    from app.integrations.feishu import build_digest_card

    card = build_digest_card([], datetime(2026, 9, 15, 9, 0, tzinfo=timezone(timedelta(hours=8))))
    text = json.dumps(card, ensure_ascii=False)
    assert "无未恢复工单" in text
    assert card["card"]["header"]["template"] == "green"


def test_digest_send_records_message_row(session):
    """汇总发送要落 FeishuMessage（幂等判据全靠它），未配 webhook 时走干跑。"""
    from sqlalchemy import select

    from app.db.models import FeishuMessage
    from app.services import digest

    digest.send_digest(session, datetime(2026, 9, 15, 9, 0, tzinfo=timezone(timedelta(hours=8))))

    rows = list(session.scalars(select(FeishuMessage).where(FeishuMessage.kind == digest.DIGEST_KIND)).all())
    assert len(rows) == 1
    assert rows[0].channel == "incident"
