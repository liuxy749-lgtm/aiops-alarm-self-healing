# AIOps 第一期详细设计文档

## ------ 告警事件中心、关联引擎与 AI 辅助诊断 MVP

**文档版本：** v1.1（v1.0 为设计稿；v1.1 追加 §46–§62「实施篇」，记录实际落地规格）\
**日期：** 2026-09-11\
**阶段：** Phase 1 / MVP（已完成开发与实盘联调）\
**适用环境：** Kubernetes 集群、GPU/CPU
裸机节点、Prometheus、Nightingale（夜莺）、飞书\
**核心目标：** 将现有"Prometheus → 夜莺 → 飞书"的告警链路升级为"事件接入
→ 标准化 → 富化 → 去重 → 关联 → Incident → AI 分析 → 飞书播报 → 人工闭环
→ 复盘沉淀"的 AIOps 基础平台。

------------------------------------------------------------------------

# 1. 项目背景

当前监控告警链路为：

``` text
Kubernetes / Bare Metal
          │
          ▼
     Prometheus
          │
          ▼
     Nightingale
          │
          ▼
       Feishu
```

该体系已经解决：

-   Kubernetes 与裸机节点指标采集；
-   Prometheus 指标统一存储与查询；
-   夜莺 PromQL 告警规则；
-   飞书告警通知。

但当前仍属于传统 Monitoring / Alerting 模式，主要存在以下问题：

1.  每条 PromQL 告警彼此独立，没有故障上下文；
2.  同一故障可能产生大量重复和衍生告警；
3.  无法判断多个告警是否属于同一个 Incident；
4.  告警信息缺少资产、集群、项目、GPU、Pod 等上下文；
5.  运维人员收到告警后仍需手工查询 Prometheus、Kubernetes、日志；
6.  故障处理过程没有统一 Timeline；
7.  故障经验没有结构化沉淀；
8.  AI 无法直接获得足够可靠的故障上下文；
9.  暂不具备安全可控的自动修复能力。

因此第一期不直接建设"AI 自动修复"，而是优先建设 AIOps
的数据和事件基础设施。

------------------------------------------------------------------------

# 2. 第一期建设目标

第一期重点解决：

> **一条告警是什么、属于哪个资源、与哪些告警相关、是否属于已有故障，以及
> AI 分析需要什么上下文。**

第一期完成以下闭环：

``` text
Nightingale Alert
        │
        ▼
AIOps Event Gateway
        │
        ├── Raw Event 保存
        ├── Normalize 标准化
        ├── Enrichment 上下文富化
        └── Fingerprint
        │
        ▼
Alert Engine
        │
        ├── Dedup
        ├── State
        └── Alert Instance
        │
        ▼
Correlation Engine
        │
        ├── Entity
        ├── Time Window
        ├── Topology
        ├── Causal Rule
        └── Correlation Score
        │
        ▼
Incident Engine
        │
        ▼
Context Collector
        │
        ├── Prometheus
        ├── Kubernetes API
        ├── CMDB / machine_info
        └── 后续预留 Logs / Change
        │
        ▼
AI Diagnosis
        │
        ▼
Feishu Incident Broadcast
        │
        ▼
人工处理 / 关闭
        │
        ▼
Incident Timeline / Postmortem
```

## 2.1 第一期必须实现

-   夜莺 Webhook 统一接入；
-   原始事件永久保存；
-   告警标准化；
-   资源身份识别；
-   `machine_info` 信息富化；
-   Kubernetes 拓扑富化；
-   Alert Fingerprint；
-   重复告警合并；
-   Alert FIRING / RESOLVED 状态维护；
-   Incident 创建；
-   同资源告警关联；
-   时间窗口关联；
-   Kubernetes 拓扑关联；
-   基础因果规则；
-   关联评分；
-   Incident Timeline；
-   Prometheus 上下文自动查询；
-   Kubernetes 上下文自动查询；
-   AI 初步诊断；
-   飞书结构化 Incident 播报；
-   人工确认/关闭 Incident；
-   基础复盘数据沉淀。

## 2.2 第一期明确不做

以下能力放到后续阶段：

-   AI 任意执行 Shell；
-   AI SSH 登录服务器；
-   无审批自动重启服务器；
-   自动 Drain 生产节点；
-   自动修改 Kubernetes 配置；
-   自动修改 Prometheus 告警规则；
-   完整知识图谱平台；
-   复杂机器学习异常检测；
-   全量日志智能分析；
-   全自动 Root Cause Analysis；
-   L4 级无人值守自动修复。

------------------------------------------------------------------------

# 3. 核心设计原则

## 3.1 Alert 不等于 Incident

定义三个核心对象：

``` text
Raw Event
   │
   ▼
Alert
   │
   ▼
Incident
```

### Raw Event

夜莺每次 Webhook 请求均作为一个原始事件保存。

### Alert

同一个规则、同一个资源持续触发，合并成一个 Alert Instance。

例如：

``` text
10:00 NodeNotReady node01
10:01 NodeNotReady node01
10:02 NodeNotReady node01
```

对应一个 Alert：

``` text
Alert A001
first_seen = 10:00
last_seen  = 10:02
count      = 3
status     = FIRING
```

### Incident

多个具有共同故障原因的 Alert 聚合成一个 Incident。

``` text
INC-001
│
├── NodeNotReady
├── KubeletDown
├── NodeExporterDown
├── DCGMExporterDown
└── PodNotReady × 20
```

------------------------------------------------------------------------

# 4. 总体系统架构

``` text
                         ┌──────────────────┐
                         │ Kubernetes       │
                         │ Bare Metal       │
                         │ GPU Nodes        │
                         └────────┬─────────┘
                                  │
                                  ▼
                         ┌──────────────────┐
                         │ Prometheus       │
                         └────────┬─────────┘
                                  │
                                  ▼
                         ┌──────────────────┐
                         │ Nightingale      │
                         └────────┬─────────┘
                                  │ Webhook
                                  ▼
┌──────────────────────────────────────────────────────────┐
│                   AIOps Event Gateway                    │
│                                                          │
│  Receiver → Normalizer → Enricher → Fingerprint         │
└──────────────────────────┬───────────────────────────────┘
                           │
              ┌────────────▼────────────┐
              │ PostgreSQL             │
              │ raw_events / alerts    │
              └────────────┬────────────┘
                           │
                           ▼
              ┌─────────────────────────┐
              │ Correlation Engine      │
              │                         │
              │ Entity                  │
              │ Time                    │
              │ Topology                │
              │ Causal Rules            │
              │ Score                   │
              └────────────┬────────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │ Incident Engine │
                  └────────┬────────┘
                           │
                           ▼
              ┌─────────────────────────┐
              │ Context Collector       │
              ├─────────────────────────┤
              │ Prometheus API          │
              │ Kubernetes API          │
              │ machine_info / CMDB     │
              └────────────┬────────────┘
                           │
                           ▼
                   ┌───────────────┐
                   │ AI Diagnosis  │
                   └───────┬───────┘
                           │
                           ▼
                   ┌───────────────┐
                   │ Feishu        │
                   └───────────────┘
```

------------------------------------------------------------------------

# 5. 技术选型

第一期以简单、稳定、容易维护为目标。

  模块            推荐技术
  --------------- -------------------------------------------------
  Event Gateway   Python + FastAPI
  API Server      Uvicorn / Gunicorn
  数据库          PostgreSQL
  ORM             SQLAlchemy 2.x
  数据模型        Pydantic
  数据迁移        Alembic
  Prometheus      HTTP API
  Kubernetes      kubernetes-python-client
  配置            YAML
  AI Agent        第一版普通 Tool Calling；复杂后再考虑 LangGraph
  飞书            Bot Webhook / App API
  缓存            第一期可不引入，后续 Redis
  消息队列        第一期可同步/后台任务，后续 Kafka/NATS/RabbitMQ
  部署            Kubernetes Deployment
  可观测性        Prometheus Metrics + Structured Logs

原则：

> 第一期不要为了"架构先进"提前引入 Kafka、Neo4j、ElasticSearch、复杂
> Agent Framework。

先把 Incident 闭环跑通。

------------------------------------------------------------------------

# 6. Event Gateway 设计

## 6.1 职责

Gateway 只负责：

``` text
Receive
   ↓
Validate
   ↓
Normalize
   ↓
Enrich
   ↓
Fingerprint
   ↓
Persist
   ↓
Dispatch
```

Gateway 不直接负责：

-   AI 根因判断；
-   自动修复；
-   复杂故障推理。

## 6.2 API

建议：

``` http
POST /api/v1/events/nightingale
```

健康检查：

``` http
GET /healthz
GET /readyz
```

Incident API：

``` http
GET  /api/v1/incidents
GET  /api/v1/incidents/{id}
POST /api/v1/incidents/{id}/ack
POST /api/v1/incidents/{id}/resolve
```

------------------------------------------------------------------------

# 7. 标准事件模型

所有告警进入系统后转换成统一模型。

``` json
{
  "event_id": "evt_01JXXXXX",
  "source": "nightingale",
  "event_type": "NodeNotReady",
  "status": "firing",
  "severity": "P1",
  "occurred_at": "2026-09-11T10:00:00+08:00",

  "entity": {
    "type": "node",
    "id": "gpu-node-021",
    "ip": "192.0.2.21",
    "hostname": "gpu-node-021"
  },

  "scope": {
    "cluster": "h800-prod",
    "region": "beijing",
    "zone": "zone-a",
    "projset": "team-a",
    "project": "training-prod"
  },

  "hardware": {
    "accelerator_model": "NVIDIA H800"
  },

  "labels": {},
  "annotations": {},

  "fingerprint": "sha256..."
}
```

------------------------------------------------------------------------

# 8. Enrichment 上下文富化

夜莺只提供告警触发时的 Labels。

Gateway 必须负责把：

``` text
instance=192.0.2.21:9100
```

转换成：

``` text
IP
 ↓
Hostname
 ↓
Cluster
 ↓
Region / Zone
 ↓
Project / Projset
 ↓
Accelerator Model
 ↓
Kubernetes Node
 ↓
Pod / Workload
```

## 8.1 Prometheus machine_info

现有 `machine_info` 可作为第一版轻量 CMDB。

例如：

``` promql
machine_info{ip="192.0.2.21"}
```

获取：

``` text
ip
hostname
region
zone
projset
project
accelerator_model
```

## 8.2 Kubernetes Enrichment

对于 Pod Alert：

``` text
pod
namespace
cluster
```

查询 Kubernetes API：

``` text
Pod
 ↓
spec.nodeName
 ↓
Node
 ↓
Labels
 ↓
Deployment / StatefulSet / Job
```

例如：

``` text
Pod:
training-worker-12

RUNS_ON:
gpu-node-021

OWNED_BY:
training-worker

WORKLOAD:
Job
```

## 8.3 富化失败原则

Enrichment 失败不能导致告警丢失。

必须：

``` text
Raw Event 保存
Alert 正常创建
enrichment_status = PARTIAL / FAILED
```

并记录失败原因。

------------------------------------------------------------------------

# 9. Fingerprint 设计

Fingerprint 用于识别"是不是同一个 Alert"。

第一版建议：

``` text
fingerprint =
SHA256(
    source
  + alertname
  + cluster
  + entity_type
  + entity_id
  + selected_dimensions
)
```

例如 GPU：

``` text
GPUHighTemperature
+
cluster-a
+
node01
+
gpu3
```

Pod：

``` text
PodCrashLoop
+
cluster-a
+
namespace
+
pod
+
container
```

Node：

``` text
NodeNotReady
+
cluster-a
+
hostname
```

**不要把 value、timestamp 放入 Fingerprint。**

否则每次告警都会产生新的 Fingerprint。

------------------------------------------------------------------------

# 10. Alert 去重机制

处理流程：

``` text
New Event
    │
    ▼
Fingerprint
    │
    ▼
Active Alert Exists?
    │
 ┌──┴───┐
 YES    NO
 │       │
 ▼       ▼
Update  Create
Alert   Alert
```

存在：

``` text
last_seen = now
count += 1
```

不存在：

``` text
create alert
first_seen = now
last_seen = now
count = 1
```

Resolved Event：

``` text
status = RESOLVED
resolved_at = now
```

------------------------------------------------------------------------

# 11. Correlation Engine 设计

第一期核心。

关联判断分为：

``` text
1. Entity
2. Time
3. Topology
4. Causal Rule
5. Score
```

执行顺序：

``` text
New Alert
   │
   ▼
查询最近 OPEN Incident
   │
   ▼
强制规则匹配
   │
   ▼
Entity Match
   │
   ▼
Topology Match
   │
   ▼
Time Match
   │
   ▼
Causal Match
   │
   ▼
Calculate Score
   │
   ├── score >= threshold → Attach Incident
   │
   └── score < threshold  → New Incident
```

------------------------------------------------------------------------

# 12. Entity Correlation

第一版最重要的关联方式。

例如：

``` text
NodeNotReady node01
KubeletDown node01
NodeExporterDown node01
DCGMExporterDown node01
```

全部：

``` text
entity=node01
```

因此属于强关联候选。

建议评分：

``` text
same exact entity      +50
same physical node     +40
same cluster           +10
same namespace         +10
same project           +5
```

------------------------------------------------------------------------

# 13. Time Correlation

建议第一版默认：

``` text
default_window = 5 minutes
```

但支持按告警类型覆盖：

``` yaml
correlation_windows:

  NodeNotReady: 300
  KubeletDown: 300
  PodNotReady: 180
  GPUXidError: 600
  NCCLTimeout: 600
  RDMANetworkError: 600
```

时间评分：

``` text
<= 1 minute     +20
<= 3 minutes    +15
<= 5 minutes    +10
>  5 minutes      0
```

------------------------------------------------------------------------

# 14. Topology Correlation

第一期不用 Neo4j。

使用 PostgreSQL 保存资源关系。

统一关系模型：

``` text
SOURCE --RELATION--> TARGET
```

例如：

``` text
pod-a --RUNS_ON--> node01

gpu0 --BELONGS_TO--> node01

node01 --MEMBER_OF--> cluster-a

node01 --CONNECTED_TO--> switch01

job-a --USES--> pod-a
```

第一期重点维护 Kubernetes 拓扑：

``` text
Cluster
  │
  ├── Node
  │     │
  │     ├── Pod
  │     │
  │     └── GPU
  │
  └── Workload
```

后续扩展物理网络：

``` text
Node
 ↓
NIC
 ↓
Switch
 ↓
Rack
```

------------------------------------------------------------------------

# 15. Causal Rule 设计

确定性故障链由运维维护。

配置文件：

``` text
rules/causal_rules.yaml
```

示例：

``` yaml
rules:

  - id: CR-K8S-001
    name: node_not_ready_children

    cause:
      alertname: NodeNotReady

    symptoms:
      - KubeletDown
      - NodeExporterDown
      - DCGMExporterDown
      - PodNotReady

    match:
      same_node: true

    window:
      before_seconds: 60
      after_seconds: 300

    relation:
      type: SYMPTOM

    weight: 50
```

另一条：

``` yaml
  - id: CR-GPU-001
    name: gpu_xid_related

    cause:
      alertname: GPUXidError

    symptoms:
      - GPUMissing
      - GPUUtilizationZero
      - TrainingJobError

    match:
      same_node: true

    window:
      before_seconds: 120
      after_seconds: 600

    weight: 40
```

------------------------------------------------------------------------

# 16. Correlation Score

建议第一版：

  条件                      Score
  ----------------------- -------
  Exact Entity                +50
  Same Node                   +40
  Known Causal Rule           +50
  Topology Distance = 1       +30
  Topology Distance = 2       +15
  Same Cluster                +10
  Same Namespace              +10
  Same Project                 +5
  ≤ 1 min                     +20
  ≤ 3 min                     +15
  ≤ 5 min                     +10

建议：

``` text
score >= 70
```

自动关联。

``` text
50 <= score < 70
```

可记录为候选关联，但第一期不自动合并。

``` text
score < 50
```

新建 Incident。

------------------------------------------------------------------------

# 17. 强制关联 / 禁止关联

不能完全依赖 Score。

支持：

``` yaml
force_link:
  NodeNotReady:
    - KubeletDown
    - NodeExporterDown
    - DCGMExporterDown

never_link:
  DiskUsageHigh:
    - GPUHighTemperature
```

优先级：

``` text
Never Link
    ↓
Force Link
    ↓
Causal Rule
    ↓
Topology
    ↓
Score
```

------------------------------------------------------------------------

# 18. NodeNotReady 第一条标准故障链

第一期重点跑通：

``` text
Network / Host
       │
       ▼
 NodeNotReady
       │
 ┌─────┼───────────────┐
 ▼     ▼               ▼
Kubelet Exporter      DCGM
Down    Down           Down
       │
       ▼
   PodNotReady
       │
       ▼
 Training Job
```

例如收到：

``` text
10:00:01 NICError node01
10:00:12 NodeNotReady node01
10:00:30 KubeletDown node01
10:00:32 NodeExporterDown node01
10:00:40 DCGMExporterDown node01
10:01:00 PodNotReady pod-a
10:01:01 PodNotReady pod-b
```

最终：

``` text
INC-20260911-001

root_entity:
node01

alerts:
- NICError
- NodeNotReady
- KubeletDown
- NodeExporterDown
- DCGMExporterDown
- PodNotReady × 2
```

而不是发送 7 个独立故障。

------------------------------------------------------------------------

# 19. Incident 数据模型

Incident 建议字段：

``` text
id

title

status

severity

root_entity_type
root_entity_id

cluster

suspected_root_cause
root_cause_confidence

first_seen
last_seen
ack_at
resolved_at

alert_count

created_at
updated_at
```

状态机：

``` text
OPEN
  ↓
ACKNOWLEDGED
  ↓
INVESTIGATING
  ↓
MITIGATING
  ↓
RECOVERING
  ↓
RESOLVED
```

第一期可以简化为：

``` text
OPEN
ACKNOWLEDGED
RESOLVED
```

------------------------------------------------------------------------

# 20. 数据库设计

## 20.1 raw_events

保存 Nightingale 原始请求。

``` sql
CREATE TABLE raw_events (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(64) UNIQUE NOT NULL,
    source VARCHAR(32) NOT NULL,
    payload JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processing_status VARCHAR(32),
    processing_error TEXT
);
```

## 20.2 alerts

``` sql
CREATE TABLE alerts (
    id BIGSERIAL PRIMARY KEY,

    alert_id VARCHAR(64) UNIQUE NOT NULL,
    fingerprint VARCHAR(128) NOT NULL,

    alertname VARCHAR(255) NOT NULL,
    status VARCHAR(32) NOT NULL,
    severity VARCHAR(16),

    entity_type VARCHAR(64),
    entity_id VARCHAR(255),

    ip INET,
    hostname VARCHAR(255),

    cluster VARCHAR(255),
    region VARCHAR(255),
    zone VARCHAR(255),
    projset VARCHAR(255),
    project VARCHAR(255),

    first_seen TIMESTAMPTZ NOT NULL,
    last_seen TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ,

    occurrence_count BIGINT NOT NULL DEFAULT 1,

    labels JSONB,
    annotations JSONB,
    enrichment JSONB,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_alerts_fingerprint_status
ON alerts(fingerprint, status);

CREATE INDEX idx_alerts_entity
ON alerts(entity_type, entity_id);

CREATE INDEX idx_alerts_last_seen
ON alerts(last_seen);
```

## 20.3 incidents

``` sql
CREATE TABLE incidents (
    id BIGSERIAL PRIMARY KEY,

    incident_id VARCHAR(64) UNIQUE NOT NULL,

    title VARCHAR(512) NOT NULL,

    status VARCHAR(32) NOT NULL,
    severity VARCHAR(16),

    root_entity_type VARCHAR(64),
    root_entity_id VARCHAR(255),

    cluster VARCHAR(255),

    suspected_root_cause TEXT,
    root_cause_confidence NUMERIC(5,4),

    first_seen TIMESTAMPTZ NOT NULL,
    last_seen TIMESTAMPTZ NOT NULL,

    acknowledged_at TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ,

    alert_count INTEGER NOT NULL DEFAULT 0,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

## 20.4 incident_alerts

``` sql
CREATE TABLE incident_alerts (
    id BIGSERIAL PRIMARY KEY,

    incident_id VARCHAR(64) NOT NULL,
    alert_id VARCHAR(64) NOT NULL,

    relation_type VARCHAR(32),

    correlation_score INTEGER,

    correlation_reason JSONB,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE(incident_id, alert_id)
);
```

relation_type：

``` text
ROOT
SYMPTOM
DEPENDENCY
SAME_RESOURCE
TOPOLOGY
UNKNOWN
```

## 20.5 incident_events

保存 Timeline。

``` sql
CREATE TABLE incident_events (
    id BIGSERIAL PRIMARY KEY,

    incident_id VARCHAR(64) NOT NULL,

    event_type VARCHAR(64) NOT NULL,

    actor VARCHAR(128),

    content JSONB,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

例如：

``` text
INCIDENT_CREATED
ALERT_ATTACHED
AI_ANALYSIS_STARTED
AI_ANALYSIS_FINISHED
FEISHU_SENT
USER_ACKNOWLEDGED
INCIDENT_RESOLVED
```

## 20.6 resource_relations

``` sql
CREATE TABLE resource_relations (
    id BIGSERIAL PRIMARY KEY,

    source_type VARCHAR(64) NOT NULL,
    source_id VARCHAR(255) NOT NULL,

    relation VARCHAR(64) NOT NULL,

    target_type VARCHAR(64) NOT NULL,
    target_id VARCHAR(255) NOT NULL,

    metadata JSONB,

    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

------------------------------------------------------------------------

# 21. Correlation Engine 伪代码

``` python
def process_event(event):

    raw_event = save_raw_event(event)

    normalized = normalize(event)

    enriched = enrich(normalized)

    fingerprint = generate_fingerprint(enriched)

    alert = find_active_alert(fingerprint)

    if alert:
        update_alert(alert, enriched)
        return alert

    alert = create_alert(enriched)

    candidates = find_open_incidents(
        cluster=alert.cluster,
        since=alert.first_seen - DEFAULT_WINDOW
    )

    best_incident = None
    best_score = 0

    for incident in candidates:

        if never_link(alert, incident):
            continue

        if force_link(alert, incident):
            attach(alert, incident, relation="SYMPTOM")
            return alert

        score = 0
        reasons = []

        if same_entity(alert, incident):
            score += 50
            reasons.append("same_entity")

        if same_physical_node(alert, incident):
            score += 40
            reasons.append("same_node")

        topology_score = calculate_topology(alert, incident)
        score += topology_score

        causal_score = match_causal_rules(alert, incident)
        score += causal_score

        time_score = calculate_time_score(alert, incident)
        score += time_score

        if score > best_score:
            best_score = score
            best_incident = incident

    if best_score >= 70:
        attach(
            alert,
            best_incident,
            score=best_score
        )
    else:
        create_incident(alert)

    return alert
```

------------------------------------------------------------------------

# 22. Context Collector

Incident 创建或发生重要变化后触发 Context Collector。

注意：

> Context Collector 与 Correlation Engine 分开。

Correlation 的任务：

``` text
这些告警是不是同一个故障？
```

Context Collector 的任务：

``` text
这个故障发生时系统到底是什么状态？
```

------------------------------------------------------------------------

# 23. Prometheus Context

以 NodeNotReady 为例。

自动查询：

``` promql
up{instance=~"192.0.2.21.*"}
```

以及：

``` text
CPU
Memory
Load
Disk
Network
GPU
Exporter
```

建议保存：

``` json
{
  "query": "...",
  "timestamp": "...",
  "result": "...",
  "summary": "..."
}
```

AI 最终引用的是这些证据，而不是凭空推断。

------------------------------------------------------------------------

# 24. Kubernetes Context

Node Incident 自动收集：

``` bash
kubectl get node
kubectl describe node
kubectl get pods --all-namespaces --field-selector spec.nodeName=<node>
kubectl get events
```

实际代码使用 Kubernetes API，而不是直接执行 kubectl。

重点获取：

``` text
Node Conditions
Node Labels
Node Taints
Pod Count
NotReady Pods
Evicted Pods
Kubernetes Events
Workload Owner
```

------------------------------------------------------------------------

# 25. AI Diagnosis 设计

第一期 AI 不参与基础去重。

AI 输入对象必须是：

``` text
Incident
+
Alerts
+
Topology
+
Prometheus Evidence
+
Kubernetes Evidence
+
Historical Context（后续）
```

而不是只输入一条夜莺告警。

------------------------------------------------------------------------

# 26. AI 输出 Schema

强制结构化输出：

``` json
{
  "summary": "GPU节点 node01 出现不可用",

  "suspected_root_cause": "节点网络或主机级故障",

  "confidence": 0.86,

  "evidence": [
    {
      "type": "metric",
      "description": "Node Exporter 与 DCGM Exporter 同时失联"
    },
    {
      "type": "kubernetes",
      "description": "Node Ready condition=False"
    }
  ],

  "impact": {
    "nodes": 1,
    "gpus": 8,
    "pods": 17
  },

  "recommended_checks": [
    "检查节点网络连通性",
    "检查 bond/NIC 状态",
    "检查 kubelet 状态",
    "检查内核日志"
  ],

  "recommended_action": "人工登录节点进一步确认",

  "risk": "medium"
}
```

要求：

-   必须给出 Evidence；
-   必须给出 Confidence；
-   没有证据不能给高置信度；
-   不允许虚构查询结果；
-   不允许声称执行了未执行的操作。

------------------------------------------------------------------------

# 27. 飞书告警播报

原有：

``` text
[P1] NodeNotReady
instance=192.0.2.21
```

第一期目标：

``` text
【P1 Incident】

事件：GPU节点异常
Incident：INC-20260911-001

节点：
gpu-node-021

集群：
h800-prod

区域：
beijing / zone-a

项目：
training-prod

影响：
1 Node
8 GPU
17 Pods

关联告警：
23

首次异常：
10:00:12

AI初步判断：
节点网络或主机级故障

置信度：
86%

关键证据：
1. Node Ready=False
2. Node Exporter失联
3. DCGM Exporter同时失联
4. 17个Pod在相同节点出现异常

建议：
1. 检查节点网络
2. 检查 bond/NIC
3. 检查 kubelet
4. 检查 dmesg

状态：
OPEN
```

后续可以增加：

``` text
[确认]
[查看详情]
[标记解决]
```

------------------------------------------------------------------------

# 28. Incident Timeline

所有动作进入 Timeline：

``` text
10:00:12 NodeNotReady firing
10:00:30 KubeletDown attached
10:00:32 NodeExporterDown attached
10:00:40 DCGMExporterDown attached
10:01:00 17 Pod alerts correlated
10:01:05 Incident created
10:01:06 Context collection started
10:01:10 AI analysis completed
10:01:11 Feishu notification sent
10:03:20 Operator acknowledged
10:15:40 Node recovered
10:20:40 Incident resolved
```

Timeline 是后续 AI 自动复盘的核心数据。

------------------------------------------------------------------------

# 29. Incident 恢复判断

不能因为某一条 Alert RESOLVED 就关闭 Incident。

例如：

``` text
NodeNotReady RESOLVED
```

系统进入：

``` text
RECOVERING
```

然后观察：

``` text
5 minutes
```

检查：

``` text
Node Ready
Kubelet Up
Exporter Up
Pods Ready
关键指标恢复
```

全部满足：

``` text
Incident → RESOLVED
```

否则：

``` text
Incident → OPEN
```

------------------------------------------------------------------------

# 30. 第一期故障场景范围

建议第一期只覆盖以下场景。

## P0：NodeNotReady

必须完整跑通。

## P1：ExporterDown

包括：

``` text
NodeExporterDown
DCGMExporterDown
KubeStateMetricsDown
```

## P2：Pod异常

``` text
PodNotReady
CrashLoopBackOff
OOMKilled
```

## P3：GPU异常

``` text
GPU Xid
GPU Missing
GPU Temperature
GPU Memory
```

## P4：磁盘

``` text
DiskUsageHigh
DiskIOError
FilesystemReadOnly
```

## P5：NCCL / RDMA

第一期先做告警聚合和 AI 分析，不做自动修复。

------------------------------------------------------------------------

# 31. 第一批因果规则建议

至少建立：

``` text
NodeNotReady
 ├─ KubeletDown
 ├─ NodeExporterDown
 ├─ DCGMExporterDown
 └─ PodNotReady
```

``` text
DiskFull
 ├─ PodEvicted
 ├─ ContainerWriteError
 └─ ImagePullFailure
```

``` text
OOM
 ├─ ProcessKilled
 ├─ PodRestart
 └─ ApplicationUnavailable
```

``` text
GPUXid
 ├─ GPUMissing
 ├─ GPUUtilizationZero
 └─ TrainingJobError
```

``` text
RDMANetworkError
 ├─ NCCLTimeout
 ├─ TrainingThroughputDrop
 └─ DistributedJobFailure
```

规则应根据实际环境逐步校准，而不是一次性编写大量规则。

------------------------------------------------------------------------

# 32. 项目目录

建议：

``` text
aiops-platform/
│
├── app/
│   ├── main.py
│   │
│   ├── api/
│   │   ├── webhook.py
│   │   ├── incidents.py
│   │   └── health.py
│   │
│   ├── models/
│   │   ├── event.py
│   │   ├── alert.py
│   │   └── incident.py
│   │
│   ├── services/
│   │   ├── normalizer.py
│   │   ├── enricher.py
│   │   ├── fingerprint.py
│   │   ├── deduplicator.py
│   │   ├── incident_service.py
│   │   └── context_collector.py
│   │
│   ├── correlation/
│   │   ├── engine.py
│   │   ├── entity.py
│   │   ├── topology.py
│   │   ├── causal.py
│   │   ├── time_window.py
│   │   └── score.py
│   │
│   ├── integrations/
│   │   ├── prometheus.py
│   │   ├── kubernetes.py
│   │   ├── nightingale.py
│   │   ├── llm.py
│   │   └── feishu.py
│   │
│   ├── repositories/
│   │   ├── alert.py
│   │   ├── incident.py
│   │   └── topology.py
│   │
│   └── db/
│       ├── base.py
│       ├── session.py
│       └── models.py
│
├── rules/
│   ├── causal_rules.yaml
│   └── correlation_windows.yaml
│
├── migrations/
│
├── tests/
│   ├── test_dedup.py
│   ├── test_correlation.py
│   └── test_node_not_ready.py
│
├── deploy/
│   ├── deployment.yaml
│   ├── service.yaml
│   ├── configmap.yaml
│   └── secret.yaml
│
├── Dockerfile
├── requirements.txt
└── README.md
```

------------------------------------------------------------------------

# 33. 安全设计

第一期 AI：

``` text
只读
```

允许：

``` text
Prometheus Query
Kubernetes GET/LIST/WATCH
读取 Incident
读取资产信息
```

禁止：

``` text
kubectl delete
kubectl patch
kubectl drain
kubectl rollout restart
SSH
Ansible执行
Shell执行
```

Kubernetes ServiceAccount 使用最小权限 RBAC。

AI 不直接持有：

``` text
root password
SSH private key
Kubernetes cluster-admin
```

------------------------------------------------------------------------

# 34. AIOps 自身可观测性

AIOps 平台自己必须暴露指标：

``` text
aiops_events_received_total

aiops_events_failed_total

aiops_alerts_active

aiops_incidents_open

aiops_correlation_total

aiops_correlation_failed_total

aiops_ai_analysis_total

aiops_ai_analysis_failed_total

aiops_ai_analysis_duration_seconds

aiops_feishu_send_failed_total
```

建议额外统计：

``` text
raw_alert_count
deduplicated_alert_count
incident_count
```

从而计算：

``` text
告警压缩率 =
1 - incident_count / raw_alert_count
```

------------------------------------------------------------------------

# 35. 日志设计

全部使用 JSON Structured Logging。

例如：

``` json
{
  "timestamp": "2026-09-11T10:01:05+08:00",
  "level": "INFO",
  "module": "correlation",
  "event_id": "evt_xxx",
  "alert_id": "ALT_xxx",
  "incident_id": "INC_xxx",
  "action": "alert_attached",
  "score": 110,
  "reasons": [
    "same_node",
    "causal_rule",
    "within_1m"
  ]
}
```

这样后续平台自身问题也容易排查。

------------------------------------------------------------------------

# 36. 故障保护

任何 AI 或外部系统异常，都不能阻断告警。

降级路径：

``` text
AI正常：

Alert
 ↓
Incident
 ↓
AI Analysis
 ↓
Feishu
```

AI异常：

``` text
Alert
 ↓
Incident
 ↓
AI unavailable
 ↓
Feishu 原始 Incident
```

Prometheus enrichment 异常：

``` text
Alert
 ↓
PARTIAL context
 ↓
Incident
 ↓
Feishu
```

核心原则：

> AI 是增强能力，不是告警可靠性的单点依赖。

------------------------------------------------------------------------

# 37. 第一阶段实施计划

## Week 1：Gateway + 数据模型

完成：

-   FastAPI 项目；
-   Nightingale Webhook；
-   PostgreSQL；
-   raw_events；
-   alerts；
-   incidents；
-   incident_alerts；
-   incident_events；
-   原始事件保存；
-   Event Normalizer。

验收：

``` text
夜莺告警
→ Gateway
→ PostgreSQL
```

成功率 ≥ 99.9%。

------------------------------------------------------------------------

## Week 2：Enrichment + Dedup

完成：

-   machine_info 查询；
-   Kubernetes API；
-   Node/Pod 资源识别；
-   Fingerprint；
-   FIRING/RESOLVED；
-   Alert Dedup。

验收：

100 次相同 NodeNotReady：

``` text
raw_events = 100
alerts = 1
occurrence_count = 100
```

------------------------------------------------------------------------

## Week 3：Correlation + Incident

完成：

-   Entity Correlation；
-   Time Correlation；
-   Topology Correlation；
-   causal_rules.yaml；
-   Score；
-   Incident 创建/关联；
-   NodeNotReady 标准故障链。

验收：

``` text
NodeNotReady
KubeletDown
NodeExporterDown
DCGMExporterDown
PodNotReady × N
```

能够归并为一个 Incident。

------------------------------------------------------------------------

## Week 4：Context + AI + Feishu

完成：

-   Prometheus Context；
-   Kubernetes Context；
-   AI Structured Output；
-   Evidence；
-   Confidence；
-   飞书 Incident 卡片；
-   Incident Timeline。

最终验收链路：

``` text
制造 NodeNotReady
      ↓
Nightingale
      ↓
Gateway
      ↓
Alert
      ↓
Correlation
      ↓
Incident
      ↓
Context Collector
      ↓
AI Diagnosis
      ↓
Feishu
```

------------------------------------------------------------------------

# 38. 第一期验收指标

建议至少定义以下 KPI。

  指标                                        目标
  ------------------------------- ----------------
  Webhook 接收成功率                       ≥ 99.9%
  原始事件保存率                              100%
  重复 Alert 去重准确率                      ≥ 99%
  NodeNotReady 故障链关联准确率              ≥ 95%
  告警压缩率                        实测并持续优化
  AI 分析成功率                              ≥ 95%
  AI 分析有证据输出比例                       100%
  AI 不可用时告警送达率                       100%
  Incident Timeline 完整率                   ≥ 99%

第一期 AI Root Cause 准确率不建议直接作为硬 KPI。

第一期应该先衡量：

``` text
上下文是否完整
证据是否真实
关联是否准确
AI是否给出了可验证判断
```

------------------------------------------------------------------------

# 39. 第一期测试案例

必须建立自动测试数据。

### Case 1：重复告警

输入：

``` text
NodeNotReady × 100
```

预期：

``` text
raw_event = 100
alert = 1
incident = 1
```

### Case 2：节点故障风暴

输入：

``` text
NodeNotReady
KubeletDown
NodeExporterDown
DCGMExporterDown
PodNotReady × 30
```

预期：

``` text
incident = 1
```

### Case 3：两个节点同时故障

输入：

``` text
NodeNotReady node01
NodeNotReady node02
```

无共同上游拓扑时：

``` text
incident = 2
```

### Case 4：Pod 与 Node

输入：

``` text
NodeNotReady node01
PodNotReady pod-a
```

Kubernetes：

``` text
pod-a → node01
```

预期：

``` text
incident = 1
```

### Case 5：无关告警

输入：

``` text
DiskUsageHigh node01
GPUHighTemperature node01
```

如果没有因果规则：

``` text
不得仅因 same_node 就错误合并
```

该 Case 非常重要，用于防止"过度关联"。

------------------------------------------------------------------------

# 40. 防止错误关联

Correlation Engine 最危险的问题不是漏关联，而是：

> 把两个独立故障错误合并。

因此必须保存：

``` text
correlation_score
correlation_reason
correlation_rule_id
```

例如：

``` json
{
  "score": 110,
  "reasons": [
    "same_node:+40",
    "causal_rule:CR-K8S-001:+50",
    "within_1m:+20"
  ]
}
```

运维人员必须能够回答：

> 为什么这两条告警被系统认为是同一个故障？

------------------------------------------------------------------------

# 41. 后续第二期演进

第一期稳定后进入：

``` text
Logs
 ↓
Change Events
 ↓
Historical Incidents
 ↓
RAG
 ↓
Advanced RCA
```

然后：

``` text
AI Diagnosis
 ↓
Runbook Selection
 ↓
Policy Engine
 ↓
Human Approval
 ↓
Execution
 ↓
Verification
```

第二期再建设：

-   Loki / Elasticsearch 日志上下文；
-   变更事件关联；
-   历史 Incident 检索；
-   故障知识库；
-   AI 自动 Postmortem；
-   Runbook；
-   人工审批修复；
-   Ansible / Kubernetes Action Executor。

------------------------------------------------------------------------

# 42. 长期目标架构

最终形成：

``` text
Observe
   │
   ▼
Detect
   │
   ▼
Normalize
   │
   ▼
Enrich
   │
   ▼
Correlate
   │
   ▼
Incident
   │
   ▼
Diagnose
   │
   ▼
Decide
   │
   ▼
Remediate
   │
   ▼
Verify
   │
   ▼
Postmortem
   │
   ▼
Knowledge
   │
   └──────────────→ Correlation / Diagnosis
```

最终 AIOps 平台的核心资产不是单独某一个大模型，而是：

``` text
统一事件模型
+
实时资源拓扑
+
故障因果规则
+
Incident 历史
+
Evidence
+
Runbook
+
修复结果
+
运维反馈
```

AI 位于这些工程能力之上。

------------------------------------------------------------------------

# 43. 第一期最终交付物

第一期结束至少应交付：

1.  `aiops-gateway` 服务；
2.  PostgreSQL Event / Alert / Incident 数据库；
3.  Nightingale Webhook；
4.  Event Normalizer；
5.  Context Enricher；
6.  Alert Fingerprint / Dedup；
7.  Correlation Engine；
8.  Kubernetes Topology；
9.  `causal_rules.yaml`；
10. Incident Engine；
11. Incident Timeline；
12. Prometheus Context Collector；
13. Kubernetes Context Collector；
14. AI Diagnosis；
15. Feishu Incident Bot；
16. NodeNotReady 标准故障场景；
17. 自动化测试；
18. 平台自身 Prometheus Metrics；
19. 部署 YAML；
20. 运维 README / SOP。

------------------------------------------------------------------------

# 44. 建议的第一条开发主线

不要并行开发所有故障类型。

只围绕一个场景：

``` text
NodeNotReady
```

完成端到端：

``` text
Step 1
夜莺触发 NodeNotReady

Step 2
Gateway 接收

Step 3
保存 Raw Event

Step 4
Normalize

Step 5
查询 machine_info

Step 6
查询 Kubernetes Node

Step 7
生成 Fingerprint

Step 8
创建 Alert

Step 9
查询最近 Incident

Step 10
执行 Correlation

Step 11
创建/加入 Incident

Step 12
关联 Kubelet / Exporter / Pod 告警

Step 13
查询 Prometheus

Step 14
查询 Kubernetes Event

Step 15
形成 Evidence

Step 16
调用 AI

Step 17
生成结构化 Diagnosis

Step 18
发送飞书 Incident

Step 19
人工 ACK

Step 20
恢复后自动/人工关闭

Step 21
生成 Timeline

Step 22
保存复盘基础数据
```

这条链跑通以后，再复制到：

``` text
GPU Xid
DiskFull
OOM
NCCL
RDMA
```

------------------------------------------------------------------------

# 45. 一期成功标准

第一期真正成功的标志不是：

> "接入了大模型。"

而是当一台 GPU
节点故障时，原本飞书出现几十条独立告警，系统最终只产生一个可解释的
Incident：

``` text
INC-001

故障对象：
gpu-node-021

影响：
8 GPU / 17 Pods / 2 Jobs

原始事件：
52

有效 Alert：
8

Incident：
1

疑似根因：
节点网络异常

Confidence：
86%

Evidence：
- Node Ready=False
- Kubelet heartbeat lost
- NodeExporter unreachable
- DCGMExporter unreachable
- 17 Pods located on same node

Timeline：
完整

处理状态：
OPEN
```

运维人员看到的不再是"52 条 PromQL 告警"，而是：

> **1 个故障 + 影响范围 + 证据 + 初步原因 + 推荐排查路径。**

这就是第一期 AIOps 平台最重要的交付价值。

------------------------------------------------------------------------

# 46. 实施篇说明：本篇的性质

§1–§45 是**设计稿**（开工前的方案）。§46 之后是**实施篇**，记录一期实际交付的规格，
包括：真实数据模型、接口契约、算法实现细节、配置项、部署运维方式、
与设计稿的偏差清单、实测验收证据，以及二期的可执行切分。

阅读原则：

- 两者冲突时，**以实施篇为准**（§59 逐条列出偏差与原因）；
- 设计稿中的意图、场景（如 §39 五个测试案例）继续有效；
- 二期规划以 §62 为准，§41 只保留方向性描述。

本篇所有结论都有对应的**可复现命令或测试用例**，没有推测。

------------------------------------------------------------------------

# 47. 一期实施结果总览

## 47.1 交付清单

| 项 | 状态 | 说明 |
| --- | --- | --- |
| Event Gateway | 已交付 | 夜莺 Webhook 接入、原始留档、标准化、富化、指纹 |
| Alert Engine | 已交付 | 幂等 upsert、状态机、超时过期兜底 |
| Correlation Engine | 已交付 | Never → Force → Causal → Topology → Score，决策留档 |
| Incident Engine | 已交付 | 创建/关联/根因提升/自动合并/时间线/恢复观察/人工闭环 |
| Context Collector | 已交付 | Prometheus range 回溯 + K8s 事实 + 拓扑，带查询预算 |
| AI Diagnosis | 已交付 | 结构化输出 + 证据/置信度约束 + 脱敏 + 完整留档 + 规则兜底 |
| 飞书播报 | 已交付 | 双通道（事件/工单）三卡片，噪音可控，支持干跑 |
| 平台自身可观测 | 已交付 | /metrics、结构化 JSON 日志、状态计数、保留策略 |
| 单元测试 | 已交付 | 28 个用例，覆盖 §39 Case1–5 + 幂等 + 关联 + 合并 + 恢复 + 噪音控制 |
| 实盘联调 | 已完成 | 夜莺真实推送 → 工单落库 → 飞书卡片送达 |

**未交付（有意留到二期）**：多副本部署、副作用出锁的后台队列、
日志/变更事件接入、历史工单相似检索（RAG）、Runbook 与执行审批。

## 47.2 代码规模

| 目录 | 文件数 | 行数 | 内容 |
| --- | --- | --- | --- |
| `app/` | 33 | 4704 | 业务代码（含包 `__init__`） |
| `tests/` | 2 | 648 | conftest + 28 个用例 |
| `rules/` | 3 | 249 | 关联权重 / 因果规则 / 上下文查询模板 |

## 47.3 实况数据流（与 §2 设计图的对应关系）

``` text
夜莺 n9e v8.5.1 (n9e-host-01 192.0.2.250)
    │  Callback 媒介（request_type=http, POST, body={{ jsonMarshal $event }}）
    ▼
POST http://192.0.2.115:8701/api/v1/events/nightingale
    │
    ├─ 0. 手工解析 body（不依赖 Content-Type），兼容 单对象 / 数组 / {"events":[...]}
    │
    ▼
process_items()  ← 进程内全局锁串行化；逐条 savepoint 隔离失败
    │
    ├─ 1. 判重 → 落盘：raw_events 表 + data/raw_events/raw-events-YYYY-MM-DD.jsonl
    ├─ 2. normalize()：契约校验（失败即 422 且不写脏数据）→ 内部统一模型
    ├─ 3. enrich()：machine_info + K8s → 失败降级 PARTIAL/FAILED，绝不丢告警
    ├─ 4. fingerprint：指纹 = 源 + 告警名 + 实体 + 指定 label 维度
    │
    ▼
alert_service.upsert_firing()   ← 部分唯一索引兜底（同一指纹仅一条 FIRING）
    │
    ├─ 已有 FIRING → 合并计数（occurrence_count），不重建工单、不重复播报
    │
    ▼
CorrelationEngine.correlate()
    │
    ├─ Never Link（最高优先）
    ├─ Force Link（命中即合并，强制同节点）
    ├─ Causal Rule（CR-K8S-001 等，声明 same_node 则强制校验）
    ├─ Topology（1 跳 / 2 跳，per-session 邻接缓存）
    └─ Score ≥ threshold(70) 且命中强关联信号
    │
    ▼
IncidentEngine
    │
    ├─ create / attach / 根因提升 / reconcile 自动合并
    ├─ state: OPEN → ACKNOWLEDGED → RECOVERING → RESOLVED（可 REOPEN）
    └─ timeline: incident_events（翻译成中文动作名后进卡片页脚）
    │
    ▼
ContextCollector.collect()   ← 仅在「建单」与「根因提升」时执行
    │
    ├─ Prometheus range 回溯（窗口/步长/查询上限/总预算均可配）
    ├─ K8s 只读事实 + 拓扑邻接
    └─ impact 影响面估计（节点/GPU/Pod）
    │
    ▼
DiagnosisClient.diagnose()
    │
    ├─ 有 Key：结构化 JSON 输出 + 证据校验 + 脱敏 + llm_calls 留档
    └─ 无 Key / 调用失败：规则兜底（engine=rule-stub，置信度封顶，明确标注未接入模型）
    │
    ▼
飞书播报
    ├─ 事件通道：Alert 粒度（创建/恢复）→ 8 条告警 = 8 条消息
    └─ 工单通道：Incident 粒度 → 8 条告警 = 1 张卡片（创建/根因变化/关闭）
```

------------------------------------------------------------------------

# 48. 分层架构与模块职责

## 48.1 实际目录结构

``` text
/home/lxy/aiops-alarm-self-healing/
  run.sh                          单 worker 启动脚本（含 PYTHONHOME 清理）
  requirements.txt  pytest.ini  README.md
  app/
    main.py                       FastAPI 入口 + 巡检后台任务（lifespan）
    config.py                     环境变量集中解析（Settings）
    timeutil.py                   时间口径：utcnow / ensure_utc / to_local / local_hm / local_display
    metrics.py                    极简 Prometheus 文本指标
    logging_setup.py              结构化 JSON 日志
    api/routes.py                 HTTP 接口层
    db/
      base.py                     DeclarativeBase
      session.py                  engine / SessionLocal / init_db / SQLite PRAGMA
      models.py                   8 张表 + UTCDateTime
      queries.py                  共享查询（工单/告警/时间线/计数/OPEN_STATES）
    models/schemas.py             接入契约（NightingalePayload）+ 内部统一模型
    services/
      raw_store.py                JSONL 归档（按天 + fsync + 字节偏移）
      normalizer.py               校验 / 标准化 / 实体识别 / 幂等键
      fingerprint.py              指纹计算
      enricher.py                 machine_info + K8s 富化，降级不丢告警
      alert_service.py            Alert upsert / 恢复 / 过期
      incident_service.py         Incident 全生命周期 + 保留清理
      context_collector.py        上下文采集（带预算）
      pipeline.py                 主流水线 + 飞书工单卡片出口
      sweeper.py                  巡检：过期 / 恢复验证 / 保留策略 / 自身指标
    correlation/
      engine.py                   关联决策
      rules.py                    规则加载与查询（yaml → RuleSet）
      topology.py                 关系表拓扑 + per-session 邻接缓存
    integrations/
      prometheus.py               range/instant 查询 + machine_info（单例）
      kubernetes.py               只读 K8s 客户端（单例）
      deepseek.py                 模型调用 + 脱敏 + 规则兜底 + 留档
      feishu.py                   双通道推送 + 三卡片版式 + 干跑
  rules/
    correlation.yaml              权重/阈值/强信号/force_link/never_link/根因优先级/指纹维度
    causal_rules.yaml             6 条确定性故障链
    context_queries.yaml          上下文查询模板
  tests/
    conftest.py                   关闭全部外部依赖，验证降级路径
    test_phase1.py                28 个用例
  design-doc/aiops-phase1.md      本文档
  data/                           运行期生成（raw_events/、outbox/、aiops.db）
```

## 48.2 依赖方向（单向，无循环）

``` text
api / services / correlation  ──►  integrations ──►  外部系统
        │                              │
        └──────────►  db / models / timeutil / config  ◄──┘
```

约束：

- `integrations/` 不反向依赖 `services/`；外部依赖异常一律吞掉并降级，只向上返回状态。
- 契约违规（payload 不合规）才允许向上抛 `NormalizeError`，由 API 层转成 422。
- `db/queries.py` 是查询的唯一落点，不允许在业务模块里再手写同一段 JOIN。

## 48.3 各模块关键函数

| 模块 | 关键函数 | 职责 |
| --- | --- | --- |
| `api/routes.py` | `ingest_nightingale` / `_process_batch` | 手工解析 body、批量处理、响应统计 |
| `services/pipeline.py` | `process_items` / `process_one` / `_handle_firing` / `_handle_resolved` / `send_incident_card` | 串起全链路；决定何时推卡片 |
| `services/normalizer.py` | `normalize` / `resolve_status` / `build_dedup_key` / `derive_event_id` / `split_payloads` | 兼容夜莺原生态字段、产出统一模型 |
| `services/incident_service.py` | `create_incident` / `attach_alert` / `_maybe_promote_root` / `reconcile_incidents` / `refresh_state` / `apply_diagnosis` / `purge_old_records` | 工单生命周期与状态机 |
| `correlation/engine.py` | `correlate` / `_score` / `_node_compatible` | 关联决策与可解释留档 |
| `services/context_collector.py` | `collect` / `estimate_impact` | 采集并压缩上下文 |
| `integrations/deepseek.py` | `diagnose` / `_rule_based` / `_mask` | 诊断与降级 |
| `integrations/feishu.py` | `build_incident_card` / `notify_incident` / `notify_alert_created` | 卡片构造与投递 |

------------------------------------------------------------------------

# 49. 数据模型实现规格

## 49.1 时间口径（全局约束）

SQLite 不保存时区，因此所有时间列统一用自定义 `UTCDateTime`：

- 落库：`ensure_utc(value)` 后去掉 tzinfo，存 naive UTC；
- 读回：补 `tzinfo=UTC`，内存里永远是 aware UTC；
- 展示：只在 `to_local()` / `local_hm()` / `local_display()` 时转 `AIOPS_DISPLAY_TZ`。

违反这条会出现两类故障（均已实测）：naive/aware 混用直接抛异常；
写库带偏移量导致 `last_seen >= :since` 字符串比较错乱，候选工单永远查不到、每条告警都新建工单。

## 49.2 表清单

### raw_events（原始事件）

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `event_id` | String(128) UNIQUE | 幂等键，重复请求不再产生第二条 |
| `source` | String(32) | 来源，一期固定 `nightingale` |
| `dedup_key` | String(128) INDEX | 业务幂等键（见 §51.4） |
| `payload_file` / `payload_offset` | String / Integer | JSONL 归档位置（文件 + 起始字节偏移） |
| `payload` | JSON | 原始 payload 完整副本 |
| `normalized` | JSON | 标准化结果，便于复核 |
| `processing_status` / `processing_error` | String / Text | RECEIVED / PROCESSED / FAILED |
| `occurred_at` / `received_at` | UTCDateTime | 告警时间 / 接收时间 |

### alerts（Alert Instance）

关键字段：`alert_id`(唯一)、`fingerprint`(索引)、`alertname`、`status`(FIRING/RESOLVED)、
`severity`、`entity_type/entity_id/ip/hostname/node/namespace`、
`cluster/region/zone/projset/project/accelerator_model`、`value/summary`、
`first_seen/last_seen/resolved_at/resolution_reason`、`occurrence_count`、
`incident_id`、`enrichment_status`、`labels/annotations/enrichment`。

索引与约束：

``` text
UNIQUE INDEX uq_alerts_active_fingerprint (fingerprint) WHERE status = 'FIRING'   ← 去重硬约束
INDEX idx_alerts_entity (entity_type, entity_id)
INDEX idx_alerts_last_seen (last_seen)
```

### incidents（工单）

关键字段：`incident_id`(唯一)、`title`、`status`、`severity`、
`root_entity_type/root_entity_id/root_alert_id`、`node`、`cluster`、
`suspected_root_cause`、`root_cause_confidence`、`ai_summary`、`ai_diagnosis`、
`first_seen/last_seen/acknowledged_at/resolved_at/recovery_deadline`、
`alert_count`、`context`、`merged_into`。

索引：`idx_incidents_status_last_seen (status, last_seen)`、`idx_incidents_merged_into`。

### incident_alerts（工单↔告警）

`incident_id` + `alert_id` 唯一约束 `uq_incident_alert`；
`relation_type`(ROOT/SYMPTOM/SAME_RESOURCE/TOPOLOGY)、
`correlation_score`、`correlation_reason`(JSON，含 reasons/rule_id/action/evaluated)。

### incident_events（时间线）

`incident_id`(索引)、`event_type`、`actor`、`content`(JSON)、`created_at`。

事件类型全集：`INCIDENT_CREATED`、`ALERT_ATTACHED`、`CONTEXT_COLLECTION_STARTED`、
`AI_ANALYSIS_FINISHED`、`FEISHU_SENT`、`INCIDENT_ROOT_CHANGED`、`INCIDENT_REOPENED`、
`RECOVERY_OBSERVING`、`RECOVERY_NOT_CONFIRMED`、`INCIDENT_RESOLVED`、`ALERT_STALE_EXPIRED`、
`ALERT_DETACHED`、`INCIDENT_MERGED_IN`、`INCIDENT_MERGED_OUT`、`USER_ACKNOWLEDGED`。

### resource_relations（拓扑）

`source_type/source_id` + `relation` + `target_type/target_id` 唯一；
双向查询走 `idx_relations_source` / `idx_relations_target`。

### llm_calls（模型留档）

`incident_id`(索引)、`provider`、`model`、`masked`、`prompt`、`response`、`parsed`、
`ok`、`error`、`duration_ms`。复盘误判与后续调参的唯一依据。

### feishu_messages（推送留档）

`channel`(event/incident)、`incident_id`、`alert_id`、`kind`、`payload`、`ok`、`dry_run`、`error`。
回答「到底发出去没有」只能靠这张表 + 日志里的飞书返回原文。

## 49.3 保留策略

| 对象 | 默认保留 | 执行者 |
| --- | --- | --- |
| `raw_events` 行 | 30 天 | sweeper（保留策略每小时执行一次） |
| `data/raw_events/*.jsonl` | 30 天 | 同上 |
| `incident_events` / `llm_calls` / `feishu_messages` | 90 天 | 同上 |

`alerts` 与 `incidents` 是长期资产，不自动清理（复盘与 RAG 的基础）。

------------------------------------------------------------------------

# 50. 接入契约（夜莺 → 网关）

## 50.1 传输约定

| 项 | 值 |
| --- | --- |
| 方法 / 路径 | `POST /api/v1/events/nightingale` |
| Content-Type | **不要求**（夜莺 HTTP 媒介默认不带；入口手工解析 body） |
| body 形态 | ① 单对象 `{...}` ② 数组 `[{...}]` ③ `{"events":[...]}` |
| 成功响应 | `200` + `{"received","processed","duplicated","failed","results":[...]}` |
| 契约失败 | `422` + 明确原因（不为不可解析的 body 写脏数据） |
| 空 body | `422` + 提示检查媒介的请求体模板（推荐 `{{ jsonMarshal $event }}`） |

夜莺侧配置（一期实测可行）：

``` text
通知媒介：Callback（ident=callback, request_type=http）
URL：     http://192.0.2.115:8701/api/v1/events/nightingale
请求体：  {{ jsonMarshal $event }}          ← 必须用 jsonMarshal（否则引号被 HTML 转义成 &quot;）
重试：    3 次 / 间隔 3000ms
通知规则：必须挂消息模板，否则夜莺查不到 message_template 会直接跳过（只打 warning）
```

## 50.2 夜莺原生事件字段形态（实测，非常关键）

| 夜莺字段 | 实际形态 | 平台处理 |
| --- | --- | --- |
| `rule_name` | str | → `alertname` |
| `severity` | int 1/2/3 | → P1/P2/P3 |
| `target_ident` | 常为 `192.0.2.122:9100` | 剥端口 → 实体 id；ip=host |
| `hash` | str | 参与业务幂等键 |
| `trigger_time` / `first_trigger_time` | epoch 秒（int） | → `occurred_at` / 首现时间 |
| `tags` | **字符串数组** `["k=v", ...]` | 归一成 dict 并入 labels |
| `tags_map` | dict | 同上（优先） |
| `original_tags` | 数组 | 忽略 |
| `annotations` | dict 或数组 | 归一成 dict，取 summary |
| `is_recovered` | bool | **判断恢复的权威信号** |
| `status` | **int 内部瞬时字段（实测 0）** | **不是告警状态**，认不出来就忽略 |
| `extra_info` / `notify_rules` / `notify_users_obj` | 数组/对象数组 | 模型 `extra="allow"`，不严格声明 |

因此接入契约遵循「**结构兼容优先，语义校验在 normalizer**」：
`NightingalePayload` 几乎全 Optional 且允许额外字段，真正校验放在 `normalizer.normalize()`。

------------------------------------------------------------------------

# 51. 标准化与指纹实现

## 51.1 必填与拒绝

- 缺 `alertname`（或 `event_type`/`rule_name`）→ `NormalizeError` → HTTP 422，事件标记 FAILED；
- `status` 认不出来时**不报错**：退回 `is_recovered` 判断，兜底默认 FIRING；
- 兜底返回值必须是大写 `FIRING`/`RESOLVED`（内部模型是 Literal，小写会 500）。

## 51.2 实体识别

按告警名的关键词提示推断 `entity_type`，取值：`pod` / `container` / `node` / `gpu` / `disk` / `job`。

- entity id 优先取 `target_ident`，其次 labels 中的 `instance` / `pod` / `node` / `hostname`；
- **形如 `host:port` 且 host 是 IPv4 的 id 一律剥端口**（`192.0.2.122:9100` → `192.0.2.122`）。
  不剥的话，同一台机器的 `HighDiskUsage(target=ip:9100)` 与 `NodeNotReady(target=ip)`
  会变成两个不同实体，同实体与同节点关联全部失效；
- 节点类告警若拿不到主机名，用 IP 兜底填 `node`，否则「同节点」这条关联依据无从判断。

## 51.3 指纹（Alert 去重维度）

``` text
fingerprint = sha256(source | alertname | entity_type | entity_id | 维度 label...)
```

维度 label 由 `rules/correlation.yaml` 的 `fingerprint_dimensions` 按实体类型指定，例如
`pod: [namespace, pod, container]`、`disk: [mountpoint, device]`。
**禁止把 `value` / `timestamp` 放进指纹**，否则同一故障每次取值变化都会新建 Alert。

## 51.4 幂等键（区分「重试」与「重复触发」）

``` text
raw_event.event_id  = f"evt_{sha256(dedup_key + 内容指纹)[:24]}"      ← HTTP 重试幂等
dedup_key           = f"{source}|hash={hash}|status={status}|marker={trigger_time}|value={value}"
```

只用 `hash` 做键会把「同一告警持续触发」误判成 HTTP 重试而丢弃，
`occurrence_count` 永远是 1。因此必须带上 `trigger_time`（marker）与取值。

------------------------------------------------------------------------

# 52. Alert 去重与状态机

## 52.1 去重

``` text
upsert_firing(session, norm, fingerprint) -> (Alert, created)
  1. 查同指纹的 FIRING 记录
  2. 命中 → 合并：last_seen 取大、occurrence_count += 1、字段按「有新值才覆盖」刷新
  3. 未命中 → 新建，并在 begin_nested() 里 flush
  4. 若触发唯一索引冲突（并发）→ 回查并转为合并路径，最多重试 3 次
```

硬保证是 `uq_alerts_active_fingerprint` 这个**部分唯一索引**（只约束 FIRING），
应用层先查后建只是快路径。

## 52.2 状态机

``` text
FIRING ──(夜莺 is_recovered=true)──► RESOLVED
   │
   └──(last_seen 超过 AIOPS_ALERT_STALE_SECONDS，默认 900s)──► RESOLVED (reason=stale)
```

超时过期是**夜莺不发恢复事件时的兜底**，缺了它老 Alert 会永远 FIRING，
第二天同类告警会被合并进几天前那条 Alert。

------------------------------------------------------------------------

# 53. 关联引擎实现

## 53.1 执行顺序（短路，优先即结论）

``` text
1. Never Link     命中 → 不合并，且优先级最高（记 verdict=never_link 到决策留档）
2. Force Link     命中 → 直接合并，不看分数；但强制校验「同一节点」
3. Causal Rule    命中 → 加规则权重；规则声明 same_node 时强制校验，跨节点则不成立
4. Topology       1 跳 +30 / 2 跳 +15（per-session 邻接缓存，避免重复 SQL）
5. Score          总分 ≥ threshold(70) 且命中强关联信号才成链
```

## 53.2 评分表（`rules/correlation.yaml`）

| 信号 | 权重 | 是否强信号 |
| --- | --- | --- |
| `exact_entity` 同实体 | 50 | ✅ |
| `same_physical_node` 同物理节点 | 40 | ❌ |
| `topology_distance_1` | 30 | ✅ |
| `topology_distance_2` | 15 | ✅ |
| `same_cluster` | 10 | ❌ |
| `same_namespace` | 10 | ❌ |
| `same_project` | 5 | ❌ |
| 时间 ≤1min / ≤3min / ≤5min | 20 / 15 / 10 | ❌ |
| `causal_rule` | 规则自带（一期 50/40） | ✅ |
| `force_link` | 置为阈值 | ✅ |

## 53.3 为什么需要「强关联信号」门槛

设计稿 §16 的评分表不自洽：`同节点(40) + 同集群(10) + ≤1min(20) = 70`，
恰好等于阈值，会让 §39 Case 5 的无关告警（DiskUsageHigh + GPUHighTemperature）被合并。
因此 `require_strong_link: true`：必须命中 `exact_entity` / `topology` / `causal_rule` / `force_link` 之一，
`same_node` 等只做加法、单独不足以成链。

一期要成链的两条主路径都不受影响：NodeNotReady 系走 `force_link`，Pod↔Node 走 `causal_rule`。

## 53.4 同一节点约束（真实数据打出来的修正）

- Force Link 与声明了 `same_node` 的因果规则，都要求两侧 `node` 一致；
- 缺节点信息时**不下结论**（不做否定判断），只降级为按分数评估。

实测案例：`node-021` 的 NodeNotReady 曾把 `node-022` 的 KubeletDown 强制关联进来，
工单里混入别的机器 —— 加了同节点校验后，两个节点各自成单。

## 53.5 决策可解释（留档）

每次评估结果写入 `incident_alerts.correlation_reason`：

``` json
{
  "reasons": ["causal_rule:CR-K8S-001:+50", "same_node:+40", "within_1m:+20"],
  "rule_id": "CR-K8S-001",
  "action": "ATTACH",
  "evaluated": [{"incident_id": "INC-...", "score": 110, "signals": ["causal_rule", "same_node"], "linkable": true}]
}
```

飞书卡片上的「关联依据」一行由此生成，直接回答「为什么这些告警算同一个故障」。

------------------------------------------------------------------------

# 54. Incident 引擎实现

## 54.1 状态机

``` text
        ┌──────────────── 新告警再次 FIRING（INCIDENT_REOPENED）
        ▼
     OPEN ──ack──► ACKNOWLEDGED ──┐
        │                          │
        └── 全部告警 RESOLVED ─► RECOVERING ── 观察期满且验证通过 ─► RESOLVED
                                   │                                ▲
                                   └── 出现新 FIRING / 验证不通过 ───┘（回 OPEN）
        任意状态 ── 被合并 ──► MERGED（merged_into 指向保留单）
```

- `RECOVERING` 观察期由 `AIOPS_RECOVERY_OBSERVE_SECONDS`（默认 300s）控制；
- 观察期满由 sweeper 调 `verify_recovery()`：先看 Alert 是否全恢复，
  已配置 Prometheus 时再用 `up{}` 确认指标真的回来了，否则回到 OPEN 并记 `RECOVERY_NOT_CONFIRMED`。

## 54.2 根因提升

``` text
新告警 relation 不是 ROOT 且 root_priority(新) <= root_priority(当前 root) → 不动
否则：
  · root_alert_id / root_entity / title 换成新告警
  · 旧 root 的 relation_type 降为 SYMPTOM，新告警升为 ROOT
  · 写 INCIDENT_ROOT_CHANGED 时间线
```

`root_priority` 表见 `rules/correlation.yaml`（NodeNotReady 95 > KubeletDown 80 > PodNotReady 20）。
没有这一步，「症状先到、根因后到」会把根因永久判成那个症状。

## 54.3 自动合并（reconcile）

每次关联完成后核对其它 OPEN 工单，命中任一条件即合并：

``` text
force_link / causal_rule / 拓扑距离 ≤ 2
前置：never_link 优先；跨 cluster 不合并；跨节点不合并（force_link 与因果规则场景）
保留哪张单：first_seen 更早者（相同则 incident_id 字典序小者）
```

设计意图：漏关联比错关联更容易发生，而没人会手工去合并两个工单。
典型场景是 PodNotReady 各自建单、NodeNotReady 到达后必须收敛成一张。

## 54.4 人工闭环接口

| 接口 | 用途 |
| --- | --- |
| `POST /api/v1/incidents/{id}/ack` | 认领（OPEN → ACKNOWLEDGED） |
| `POST /api/v1/incidents/{id}/resolve` | 关闭，并推一张恢复卡片 |
| `POST /api/v1/incidents/{id}/alerts/{alert_id}/detach` | **纠错出口：摘掉误关联的告警** |
| `POST /api/v1/incidents/{id}/analyze` | 重跑上下文 + AI（不推卡片，避免刷群） |

`detach` 是防过度关联的最后一道人工出口，必须有。

------------------------------------------------------------------------

# 55. 上下文采集与 AI 诊断实现

## 55.1 上下文采集（Context Collector）

回答的是「故障发生时系统处于什么状态」，与关联引擎（判断是否同一故障）职责分离。

| 数据源 | 实现要点 |
| --- | --- |
| Prometheus | **range query 回溯**：`first_seen - AIOPS_CONTEXT_LOOKBACK_SECONDS` ~ `last_seen + 60s`，step 可配；结果压成 min/max/first/last 摘要 |
| Kubernetes | 节点 conditions/taints、节点上 Pod 与未就绪数、相关事件；只读，超时可配 |
| 拓扑 | 实体的双向邻接（最多 40 条边） |
| impact | 影响面估计：节点数 / GPU 数 / Pod 数 |

**为什么必须用 range query**：NodeNotReady 时 node_exporter 已掉线，
即时查询 `up{instance=...}` 只会返回 0，没有诊断价值。

预算保护（避免一条告警拖垮平台）：单次采集的查询数上限
`AIOPS_CONTEXT_MAX_QUERIES`（默认 60）与总预算
`AIOPS_CONTEXT_DEADLINE_SECONDS`（默认 30s），超预算即截断并标 `truncated` 降级为 PARTIAL。

**执行时机**：只在「工单创建」与「根因提升」时执行。
每条挂进来的告警都重跑会导致 N-1 次白跑，且查询量随工单规模二次放大（O(N²)）。

## 55.2 AI 诊断

| 约束 | 实现 |
| --- | --- |
| 结构化输出 | `response_format=json_object`，二次解析校验，不合格即判失败 |
| 证据强制 | 输出必须带 `evidence[]`；无证据时置信度压到 0.35 以下 |
| 不许编造 | prompt 明确禁止虚构查询结果；未取到的数据源写成 `未取得…` 证据 |
| 脱敏 | `AIOPS_MASK_ASSETS=true` 时替换 IP/主机名后再出网 |
| 降级 | 无 Key 或调用失败 → 规则兜底（`engine=rule-stub`），置信度封顶并标注「未接入模型」 |
| 留档 | 每次调用写 `llm_calls`（脱敏 prompt + 原始响应 + 解析结果 + 耗时 + 错误） |

输出 Schema（`summary` / `suspected_root_cause` / `confidence` / `evidence[]` /
`recommended_checks[]` / `needs_human` / `impact`）见设计稿 §26，实现与之一致。

**摘要中不得嵌入会变化的数字**（如「已聚合 N 条告警」）：摘要会当工单标题用，
建单时 N=1，之后关联到 30 条标题还写 1。计数在卡片里是独立字段。

------------------------------------------------------------------------

# 56. 飞书播报实现

## 56.1 通道划分

| 通道 | 环境变量 | 粒度 | 消息类型 |
| --- | --- | --- | --- |
| 夜莺原始告警 | 不归本平台管 | 每条告警 | 保持现状，作为对照 |
| 事件通道 | `AIOPS_FEISHU_EVENT_WEBHOOK` | Alert 创建 / 恢复 | 🔔 新告警 / ✅ 告警恢复 |
| 工单通道 | `AIOPS_FEISHU_INCIDENT_WEBHOOK` | 工单创建 / 根因变化 / 关闭 | 🚨 / 🔄 / ✅ 三卡片 |

空值 = **干跑**：卡片照常构造并落 `data/outbox/*.jsonl` + `feishu_messages`，但不外发。

## 56.2 噪音控制矩阵（核心设计）

| 场景 | 事件通道 | 工单通道 |
| --- | --- | --- |
| 新 Alert 创建 | ✅ 1 条 | 若建单 → 1 张卡片 |
| 新 Alert 挂到已有工单 | ✅ 1 条 | ❌ 不发（仅刷新数据） |
| 根因被提升 | — | ✅ 补 1 张「根因更新」 |
| 工单关闭（自动/人工） | — | ✅ 1 张「工单恢复」 |
| 同一告警重复触发 | ❌ | ❌ |
| HTTP 重试（同 body） | ❌ | ❌ |

实测对照：8 条告警的风暴 → 事件通道 8 条、工单通道 **1 张**。
反面教材（第一版）：每关联一条告警就推一张卡片 → 8 条告警刷 8 张，比原来的告警还吵。

## 56.3 卡片版式规范

- 标题：`emoji + 动作 + 级别 + 故障对象 + 告警名`；状态/计数放双列字段区，不堆成段落；
- 分段顺序：概览字段 → 关联依据 → 初步判断/处理结果 → 关键证据 → 建议排查 → 关联告警 → 处理时间线 → 页脚；
- **关联告警按告警名聚合**（30 个 Pod 只占 1 行：`PodNotReady ×26（worker-1、worker-2…）`）；
- 时间线只保留关键事件：过滤每次刷新产生的例行事件（上下文采集/AI 分析/已推送），
  并把重复的 `ALERT_ATTACHED` 聚合成一行 `聚合告警 ×N`（实测 123 条事件 → 有效 3 条）；
- 时间按 `AIOPS_DISPLAY_TZ` 显示，格式 `MM-DD HH:MM:SS`；
- 表头色区分卡片种类：新工单红/橙（按级别）、根因更新红/橙、恢复绿。

------------------------------------------------------------------------

# 57. 配置项全量清单

## 57.1 环境变量（`app/config.py`）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `AIOPS_DATA_DIR` | `./data` | 数据目录（raw_events/、outbox/、aiops.db） |
| `AIOPS_DB_PATH` | `<data>/aiops.db` | SQLite 文件 |
| `AIOPS_RULES_DIR` | `./rules` | 规则目录 |
| `AIOPS_ALERT_STALE_SECONDS` | `900` | 无事件多久判定 Alert 恢复 |
| `AIOPS_SWEEP_INTERVAL_SECONDS` | `30` | 巡检间隔 |
| `AIOPS_RECOVERY_OBSERVE_SECONDS` | `300` | 工单恢复观察期 |
| `AIOPS_CONTEXT_LOOKBACK_SECONDS` | `600` | 上下文回溯窗口 |
| `AIOPS_CONTEXT_STEP_SECONDS` | `15` | 回溯采样步长 |
| `AIOPS_CONTEXT_MAX_QUERIES` | `60` | 单次采集查询上限 |
| `AIOPS_CONTEXT_DEADLINE_SECONDS` | `30` | 单次采集总预算 |
| `AIOPS_RAW_RETENTION_DAYS` | `30` | raw_events 保留天数 |
| `AIOPS_EVENT_RETENTION_DAYS` | `90` | 时间线/模型调用/推送记录保留天数 |
| `AIOPS_PROMETHEUS_URL` | 空 | 未配则富化与指标上下文降级 |
| `AIOPS_PROMETHEUS_TIMEOUT` | `5` | Prometheus 请求超时 |
| `AIOPS_MACHINE_INFO_TTL_SECONDS` | `300` | machine_info（轻量 CMDB）缓存 TTL |
| `AIOPS_K8S_API_URL` / `AIOPS_K8S_TOKEN` / `AIOPS_K8S_CA_FILE` | 空 | 只读账号 |
| `AIOPS_K8S_VERIFY` | `true` | TLS 校验 |
| `AIOPS_K8S_TIMEOUT` | `8` | K8s 请求超时 |
| `DEEPSEEK_API_KEY` | 空 | 未配则规则兜底 |
| `DEEPSEEK_BASE_URL` / `DEEPSEEK_MODEL` | `https://api.deepseek.com` / `deepseek-chat` | 兼容内网 OpenAI 协议服务 |
| `AIOPS_LLM_TIMEOUT` / `AIOPS_LLM_MAX_RETRIES` | `60` / `2` | 模型超时与重试 |
| `AIOPS_MASK_ASSETS` | `true` | 出网前脱敏资产信息 |
| `AIOPS_FEISHU_EVENT_WEBHOOK` / `AIOPS_FEISHU_INCIDENT_WEBHOOK` | 空 | 空 = 干跑落盘 |
| `AIOPS_FEISHU_TIMEOUT` | `8` | 推送超时 |
| `AIOPS_LOG_LEVEL` | `INFO` | 日志级别 |
| `AIOPS_DISPLAY_TZ` | `Asia/Shanghai` | 展示时区 |

## 57.2 规则文件

| 文件 | 内容 | 热加载 |
| --- | --- | --- |
| `rules/correlation.yaml` | threshold / weights / require_strong_link / strong_link_signals / correlation_windows / force_link / never_link / root_priority / fingerprint_dimensions | `POST /api/v1/rules/reload` |
| `rules/causal_rules.yaml` | 6 条故障链：CR-K8S-001、CR-NET-001、CR-DISK-001、CR-OOM-001、CR-GPU-001、CR-NET-002 | 同上 |
| `rules/context_queries.yaml` | 按告警名的查询模板（窗口/步长/上限在 config，不在此重复） |

关联阈值只有这一个来源（`correlation.yaml:threshold`），环境变量里**不再放**同名开关。

------------------------------------------------------------------------

# 58. 部署与运维

## 58.1 部署形态

``` text
单元：systemd aiops-gateway（enabled + active）
命令：/home/lxy/aiops-alarm-self-healing/.venv/bin/uvicorn app.main:app \
        --host 0.0.0.0 --port 8701 --workers 1
环境：EnvironmentFile=/etc/aiops-gateway.env（600 权限，含凭据）
      环境变量 AIOPS_DATA_DIR / AIOPS_DISPLAY_TZ / PYTHONUNBUFFERED
约束：⚠️ 必须单副本 —— SQLite + 进程内锁，多 worker 会状态不一致并重复通知
```

端口选择说明：8642/8643/8644 被本机 docker-proxy 占用，故用 **8701**。

## 58.2 常用命令

``` bash
systemctl status aiops-gateway
systemctl restart aiops-gateway
journalctl -u aiops-gateway -f
curl -s http://127.0.0.1:8701/readyz                 # 依赖配置状态
curl -s http://127.0.0.1:8701/api/v1/stats           # 压缩率等
curl -s http://127.0.0.1:8701/metrics                # 自身指标
curl -X POST http://127.0.0.1:8701/api/v1/sweep      # 手动巡检
curl -X POST http://127.0.0.1:8701/api/v1/rules/reload
```

## 58.3 自身可观测

指标（`/metrics`）：`aiops_events_received_total{source}`、`aiops_events_duplicate_total`、
`aiops_events_failed_total{reason}`、`aiops_alerts_created_total`、`aiops_alerts_updated_total`、
`aiops_alerts_resolved_total`、`aiops_correlation_total{action}`、`aiops_incidents_merged_total`、
`aiops_ai_analysis_total{ok}`、`aiops_ai_analysis_duration_seconds`、
`aiops_feishu_sent_total{channel,ok}`、`aiops_feishu_send_failed_total{channel}`，
以及 gauge：`aiops_alerts_active`、`aiops_incidents_open`、`aiops_raw_events_failed`。

日志：结构化 JSON，关键事件包括 `event_processing_failed`、`normalize_failed`、
`enrichment_done`、`alert_attached`、`new_incident`、`incident_root_promoted`、`incidents_merged`、
`context_collected`、`incident_analyzed`、`feishu_sent`（含飞书返回原文）、`sweep_done`。

## 58.4 排障手册

| 现象 | 先看哪里 | 常见原因 |
| --- | --- | --- |
| 夜莺侧 422 | 响应的 `detail` | 媒介请求体为空 / 模板没用 `jsonMarshal` / 缺 `alertname` |
| 告警进来了但没工单 | `/api/v1/alerts`、`/api/v1/stats` | 被判定为重复触发（ALERT_UPDATED）；或指纹维度不含关键 label |
| 工单被拆成多个 | 工单详情的 `correlation_reason.evaluated` | 缺 `node` 标签导致同节点约束不成立；或强信号未命中 |
| 该合的没合 | 同上 | 跨 cluster；或 never_link 命中 |
| 群里没收到卡片 | `feishu_messages`（ok/dry_run）、`journalctl` 的 `feishu_sent` | webhook 未配（dry_run=1）；或该场景本就不推（见 §56.2） |
| 卡片数量太多 | `feishu_messages` 按 kind 统计 | 检查是否有场景被误判为「根因变化」 |
| 富化恒为 PARTIAL | `/readyz` 的 prometheus/kubernetes | 未配置外部依赖（预期行为） |
| 磁盘持续增长 | `data/raw_events/` 大小、`/api/v1/sweep` 的 purged | 保留策略未到点或无新事件触发巡检 |

## 58.5 备份与恢复

``` text
备份：data/aiops.db（SQLite，WAL 模式）+ data/raw_events/*.jsonl
      sqlite3 data/aiops.db ".backup '/backup/aiops-$(date +%F).db'"
恢复：停服 → 还原 db 与 raw_events 目录 → 起服 → /readyz 校验
注意：raw_events 行只存文件+偏移，还原时 DB 与 JSONL 必须同批，否则归档指针失效
```

------------------------------------------------------------------------

# 59. 与设计稿的偏差清单（逐条说明）

| # | 设计稿原方案 | 实际问题 | 实施选择 | 验证方式 |
| --- | --- | --- | --- | --- |
| 1 | §16 评分表直接按总分判定 | `同节点40+同集群10+≤1min20 = 70` 恰好达阈值，会合并无关告警 | 增加 `require_strong_link` 强信号门槛 | `test_same_node_without_causal_evidence_does_not_merge` |
| 2 | 关联时 `relation` 写死 SYMPTOM | 症状先到、根因后到时根因永久判错 | 支持根因提升（ROOT 上位、旧 root 降 SYMPTOM） | `test_case4b_reverse_order_promotes_root_to_node` |
| 3 | 未提工单合并 | 症状各自建单后无法收敛 | 新增 `reconcile_incidents` 自动合并 | `test_incidents_merge_after_root_arrives` |
| 4 | 候选工单窗口用 `first_seen` | 长故障的后续告警找不到候选单 | 改用 `last_seen` | `test_case2_*` |
| 5 | 未提 Alert 过期 | 夜莺不发恢复时老 Alert 永远 FIRING | 增加 stale 过期兜底（默认 900s） | `test_stale_alert_then_recovery_resolves_incident` |
| 6 | 未提并发与幂等细节 | 重试与重复触发都可能丢事件或重复建单 | raw 幂等键 + 业务幂等键（含 trigger_time）+ 部分唯一索引 | `test_same_payload_twice_is_idempotent`、`test_case1_100_duplicate_alerts_collapse` |
| 7 | §23 即时查询指标上下文 | 节点故障时 exporter 已掉线，只能查到 0 | 改 range query 回溯 + 摘要压缩 | 实盘验收表 |
| 8 | §7 假设 tags 是 map、status 是字符串 | 夜莺实测 `tags` 是字符串数组、`status` 是 int 内部字段 | schema 层形状归一 + 状态解析退回 `is_recovered` | `test_nightingale_native_event_parses` |
| 9 | Webhook 按标准 JSON Body 接收 | 夜莺不带 Content-Type，声明 Body 会 422 | 手工 `await request.body()` + `json.loads` | `test_webhook_accepts_bare_array_and_no_content_type` |
| 10 | 未提实体 id 会带端口 | `target_ident=192.0.2.122:9100` 导致同机不同告警成两个实体 | 形如 host:port 且 host 为 IPv4 时剥端口 | 实盘排查 + 用例断言 |
| 11 | §27 按告警粒度播报 | 一场风暴刷 N 张卡片，比原告警更吵 | 工单卡片只在创建/根因变化/关闭时推 | `test_storm_sends_exactly_one_incident_card` |
| 12 | AI 摘要含「已聚合 N 条告警」 | 摘要当标题用，N 停留在建单时的 1 | 摘要不嵌计数，计数独立成字段 | 卡片渲染核对 |
| 13 | 阈值放环境变量 | 与 rules yaml 形成双份真相，改环境变量无效 | 只保留 `correlation.yaml:threshold` | `/readyz` 显示实际阈值 |
| 14 | 未提归档与留档保留 | raw_events/时间线/推送记录无界增长 | 加保留策略（30 天 / 90 天） | `/api/v1/sweep` 的 purged 字段 |
| 15 | 每条告警都跑上下文与 AI | N 条告警白跑 N-1 次，查询量 O(N²) | 只在建单与根因提升时执行 | 事件表计数：8 条告警仅 1 次采集/分析 |

------------------------------------------------------------------------

# 60. 一期验收记录

## 60.1 单元测试（28 个用例全绿）

覆盖：§39 Case1–5、幂等、同实体/拓扑/因果关联、自动合并、Alert 过期、恢复观察、
人工纠错（ack/resolve/detach）、归档校验、降级路径、夜莺原生报文兼容、
推送噪音控制、异常 body 处理。

``` bash
env -u PYTHONHOME -u PYTHONPATH .venv/bin/python -m pytest -q
# 28 passed
```

## 60.2 实盘验收（夜莺真实推送，非测试客户端）

| 场景 | 实测结果 |
| --- | --- |
| 节点故障风暴 8 次通知 | raw_events 8 → alerts 8 → **incidents 1**，压缩率 0.875 |
| 关联原因可追溯 | 节点告警 `force_link:NodeNotReady`(+70)；Pod 告警 `causal_rule:CR-K8S-001`(+50)+`same_node`(+40)+`within_1m`(+20)=130 |
| 同 body 重发 | `duplicated=1`，raw_events 不增长 |
| 同 hash 重复触发 | `ALERT_UPDATED`，occurrence_count 累加 |
| 跨节点同链条 | NodeNotReady(node01) 与 KubeletDown(node02) **不合并**（2 张工单） |
| 恢复事件 | 8 条 Alert 全 RESOLVED → 工单进 RECOVERING（不直接关单） |
| 夜莺 Web 点「测试」 | HTTP 200、`processed=1`，字段解析正确、工单落库 |
| 30 条告警风暴 → 飞书 | 工单通道 **1 张卡片**，飞书返回 `{"code":0,"msg":"success"}` |
| 卡片版式 | 新工单 🚨 / 根因更新 🔄 / 工单恢复 ✅，关联告警按告警名聚合，时间线 123 条→有效 3 条 |
| 附加告警不重跑 | 8 条告警的工单，时间线中上下文采集/AI 分析各仅 1 条 |

## 60.3 一期成功标准的达成情况（对照 §45）

| 标准 | 结果 |
| --- | --- |
| 告警能自动归类为工单 | ✅ 8 条告警 → 1 张工单 |
| 关联原因可解释 | ✅ 决策留档 + 卡片「关联依据」 |
| AI 输出有证据与置信度 | ✅ 无证据时置信度 <0.35，未接入模型时明确标注 |
| 告警不丢、不重复 | ✅ 幂等键 + 部分唯一索引，重复请求 `duplicated=1` |
| 一个人能看懂 | ✅ 卡片从「N 条告警」变成「1 个故障 + 影响 + 证据 + 排查路径」 |
| 不阻塞告警链路 | ✅ 外部依赖全降级，未配置时功能可用且状态可见 |

------------------------------------------------------------------------

# 61. 已知技术债与架构约束（二期入口）

## 61.1 必须尊重的一期约束

| 约束 | 原因 | 违反后果 |
| --- | --- | --- |
| 必须单副本、单 worker | SQLite + 进程内锁 | 状态不一致、重复通知 |
| 归档 DB 与 JSONL 必须同批备份 | raw_events 只存文件+偏移 | 归档指针失效 |
| 关联阈值只在 `rules/correlation.yaml` | 消除双份真相 | 改环境变量无效、误判配置未生效 |

## 61.2 技术债清单（按影响排序）

| # | 债 | 影响 | 计划 |
| --- | --- | --- | --- |
| 1 | ✅ **已解决（2026-09-11）** `PROCESS_LOCK` 曾覆盖富化/上下文/AI/飞书的网络调用 | 接入真实依赖后一次超时会冻结所有告警入库 | 已落地「快回 200 + 副作用出锁」最小可用版，见 §61.3 |
| 2 | 工单首卡在第一条告警到达时发出 | 首卡「关联告警」只显示 1 条，低估故障规模 | 二期 M1：加聚合窗口（P0/P1 立即，P2/P3 延迟） |
| 3 | 单副本 + SQLite | 无法水平扩展，写入吞吐受单写者限制 | 二期 M2 起评估 PostgreSQL |
| 4 | 时间线/推送记录只按时间保留 | 长期复盘需要按工单归档 | 二期 M2：按工单归属级联清理 |
| 5 | ✅ **已接入（2026-09-11）** Prometheus 查询 API（`192.0.2.13:9090`，dx0 集群）、K8s 只读 API（SA `airs-system/aiops-readonly`）、DeepSeek（`deepseek-v4-flash`） | 富化不再恒为 PARTIAL，AI 走真实模型 | 已完成；K8s 侧只读，无任何写权限 |
| 6 | 磁盘/OOM/NCCL/RDMA 规则未做场景验证 | 规则可能过拟合 | 二期 M2：按真实告警校准 |
| 7 | 批量 webhook 未在夜莺侧启用 | 风暴时请求数偏多 | 二期 M1：夜莺侧切数组 body |

## 61.3 副作用出锁实现（2026-09-11 落地）

**触发**：夜莺报 `context deadline exceeded (Client.Timeout exceeded while awaiting headers)`，
但飞书群其实收到了卡片 —— 整链同步执行要 11~15 秒（AI 诊断占 10~13 秒），
超过夜莺客户端超时，它在重试投递。

**改法**：

```text
POST /api/v1/events/nightingale
  └─ PROCESS_LOCK { raw 落盘 → 标准化 → 富化 → 指纹 → Alert/工单 短事务 }   ← 毫秒级
  └─ 入队：Alert 通知 + 工单分析（同一个单线程执行器，FIFO）
  └─ 立即 200

后台 worker（app/services/worker.py，单线程）：
  ① 等 webhook 事务提交（避免读到旧状态）
  ② 采集上下文（Prometheus/K8s）→ AI 诊断 → 飞书推送
  ③ 落库结果，analysis_status: PENDING → RUNNING → DONE/FAILED
```

**关键设计点**（都有实测原因，别删注释）：

| 点 | 原因 |
| --- | --- |
| 后台任务用 `WorkerSessionLocal`（AUTOCOMMIT） | 跨 HTTP 的长事务会持有读快照，回写升级写锁时 SQLite **立即**报 `database is locked`（busy_timeout 不生效），把 webhook 入库打挂 |
| webhook 侧遇 `database is locked` 重试 4 次 | 后台写库瞬间可能与请求抢写锁；重试放在 savepoint 内层，不影响同批已成功条目 |
| `analysis_status` 落在 DB 而非内存队列 | 进程重启后 `requeue_pending()` 能把 PENDING/RUNNING 重新入队 |
| sweeper 只重排 PENDING/RUNNING，不重排 FAILED | FAILED 时不自动重试，避免重复推卡片 |
| `AIOPS_INLINE_ANALYSIS=true` | 一键退回同步执行（对照/回滚） |
| `/analyze` 改为 202 + 入队 | 同步跑要 10 余秒，长事务会挡住 webhook 的写 |

**实测效果**（真实 Prometheus + K8s + DeepSeek，飞书干跑）：

```text
入库耗时：avg 101 ms（87/124/93 ms）      改动前：11~15 秒
请求返回时：analysis_status=PENDING，无诊断
后台完成后：analysis_status=DONE attempts=1
            prometheus OK(14 查询/54 series) + kubernetes OK
```

**残留**：后台任务仍是进程内单线程队列（不是可靠队列）；进程被 kill 时在途任务靠
启动补偿续跑。真正的 outbox + 可观测队列仍按二期 M1 做（见二期文档 §6/§7）。

------------------------------------------------------------------------

# 62. 二期详细规划（可执行切分）

> §41 是方向性描述；本节是可排期、可验收的落地切分。

## 62.1 二期目标

在一期「告警 → 工单」的基础上，补齐三件事：

1. **真实性**：接真实 Prometheus / K8s / 模型，让证据与判断来自真实数据；
2. **可扩展性**：副作用出锁 + 聚合窗口，让真实流量下不退化；
3. **可沉淀性**：把工单、时间线、变更事件变成可检索的故障知识，向 Runbook 演进。

## 62.2 里程碑

### M0：真实依赖接入（无架构改动，可立即开始）

| 项 | 内容 | 验收标准 | 依赖 |
| --- | --- | --- | --- |
| M0-1 | 接 Prometheus（只读） | 工单卡片「关键证据」出现真实指标回溯；影响面 GPU/Pod 数准确 | 只需 URL，无需新凭据 |
| M0-2 | 夜莺侧给 Pod 类规则加 `node` 标签 | Pod 告警能与节点告警关联（工单数下降） | 夜莺 Web 配置 |
| M0-3 | 接模型（DeepSeek 或内网 vLLM） | 卡片「初步判断」不再是「未接入模型」；`llm_calls` 有真实记录 | 需确认资产信息可否出网 |
| M0-4 | 接 K8s 只读 | Pod↔Node 拓扑自动建立，不再依赖手工标签 | 需只读 SA Token |

### M1：吞吐与播报质量（架构改动，需先出方案评审）

| 项 | 内容 | 验收标准 |
| --- | --- | --- |
| M1-1 | 副作用出锁：锁只包 DB 读-判-写，采集/诊断/推送进单 worker 队列 | 注入 5s 网络延迟时，告警入库 P99 不劣化 |
| M1-2 | 聚合窗口：`AIOPS_INCIDENT_NOTIFY_DELAY_SECONDS` | 30 条告警风暴的首卡显示完整数量（不再只 1 条） |
| M1-3 | 队列积压可观测 | 新增 queue depth / 处理延迟指标，积压时告警 |
| M1-4 | 夜莺侧启用批量 body | 风暴请求数下降，`received` 统计正确 |

### M2：知识沉淀（二期主体功能）

| 项 | 内容 | 验收标准 |
| --- | --- | --- |
| M2-1 | 变更事件接入（发布/扩缩容/配置变更） | 工单时间线能看到「同期变更」，证据里出现变更证据 |
| M2-2 | 日志证据（Loki / ES 只读） | NodeNotReady 工单能带出 kubelet/dmesg 关键日志片段 |
| M2-3 | 历史工单相似检索（RAG） | 新工单卡片给出「历史相似故障 + 当时的处置」 |
| M2-4 | 故障知识库：工单关闭时生成结构化复盘条目 | 可按集群/告警名检索历史故障与处置 |

### M3：自动化（需单独立项与审批）

Runbook 编排与执行审批（设计稿 §41 的方向）。**前提**：一、二期积累的
「诊断结论 → 人工处置」配对数据足够，且执行必须有审批与熔断。不建议与 M2 并行。

## 62.3 二期需要用户拍板的事项

| # | 事项 | 说明 |
| --- | --- | --- |
| 1 | 资产信息能否出网 | 决定用公网 DeepSeek 还是内网 vLLM（`AIOPS_MASK_ASSETS` 已可脱敏） |
| 2 | K8s 只读凭据 | 需要只读 SA Token 与 API 地址（一期已按只读边界实现） |
| 3 | 聚合窗口策略 | 统一延迟 30s，还是 P0/P1 立即 + P2/P3 延迟 |
| 4 | 是否接受换 PostgreSQL | M2 起若要多副本或更高写入吞吐，需停机迁移一次 |
| 5 | 告警基线 | 需要近 30 天飞书告警条数，用于量化压缩效果（一期已能算压缩率） |

## 62.4 二期风险

| 风险 | 影响 | 缓解 |
| --- | --- | --- |
| 接入真实依赖后暴露性能问题 | 风暴时入库阻塞 | M1 先做，不要等 M2 |
| 因果规则过拟合到少量告警名 | 该合的没合 | 每个新故障链都要有场景用例 + 误关联率统计 |
| 模型误判诱导错误处置 | 误导运维 | 证据强制 + 置信度门槛 + `llm_calls` 留档复核 |
| 自动执行过早引入 | 生产事故 | M3 单独立项，必须先有审批与熔断，且一期明确不做 |
