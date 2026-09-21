# AIOps 第二期详细设计文档

## ------ 真实证据、异步事件流水线、日志/变更关联、故障知识库与 RAG

**版本：** v2.2-design\
**日期：** 2026-09-11\
**阶段：** Phase 2\
**基线：** AIOps Phase 1 v1.1 实施版\
**性质：** 二期实施前详细设计稿（已过一轮实施方评审 + 一轮实施后回填）

**v2.1 修订说明**（评审后）：修正 6 处与一期实现不符的判据；新增
§48 PG 落地清单、§49 飞书交互方案（不使用自建应用）、§50 延迟预算、§51 Embedding 与脱敏；
§10 明确「接 Prometheus 查询 API 而非 Alertmanager 告警」并补多数据源路由。

**v2.2 修订说明**（实施后回填）：新增 **§52 二期实施篇** —— 把 §1–§51 的设计
落成可直接开工的清单：现状基线实测数据、M1 五个可交付项的表结构与状态机、
迁移步骤、验收标准、风险登记册。同时把原 M0 已完成的部分标为「已完成」。

------------------------------------------------------------------------

# 1. 基线与定位

本设计以一期**实际实施结果**为基线。一期已经完成 Nightingale
Webhook、Raw Event、Alert 幂等/状态机、Correlation Engine、Incident
Engine、Context Collector、结构化 AI
Diagnosis、飞书双通道、Timeline、自身监控、28 个测试及真实夜莺联调。

一期当前真实主链：

``` text
Nightingale
  ↓
Event Gateway
  ↓
Normalize / Enrich / Fingerprint
  ↓
Alert Engine
  ↓
Correlation Engine
  ↓
Incident Engine
  ↓
Context Collector
  ↓
AI Diagnosis
  ↓
Feishu
```

一期仍存在的二期入口：

``` text
SQLite + 单副本 + 单 Worker + 进程内 PROCESS_LOCK
真实 Prometheus/K8s/LLM 需要完整接入
外部网络副作用仍会占用主处理锁
无日志证据
无变更事件
无历史 Incident 相似检索
无结构化 Postmortem / Knowledge Loop
```

二期目标不是重新建设告警平台，而是：

> 把一期"能形成
> Incident"的系统升级为"能基于真实证据诊断、能承受真实流量、能从历史故障学习"的
> Incident Intelligence Platform。

------------------------------------------------------------------------

# 2. 二期核心目标

## 2.1 真实性

AI Diagnosis 必须建立在真实数据上：

``` text
Prometheus Metrics
Kubernetes Facts
Logs
Change Events
Topology
Historical Incidents
```

## 2.2 可扩展性

解除一期 `PROCESS_LOCK` 对外部调用的包裹：

``` text
Webhook
  ↓
短事务：Raw / Alert / Incident / Outbox
  ↓
COMMIT
  ↓
HTTP 200

后台 Worker
  ├─ Context
  ├─ Diagnosis
  ├─ Postmortem
  └─ Feishu
```

Prometheus、K8s、LLM、飞书任何一个超时，都不得冻结新告警入库。

## 2.3 可沉淀性

``` text
Incident RESOLVED
  ↓
Postmortem
  ↓
Knowledge Entry
  ↓
Embedding / Index
  ↓
Historical Retrieval
  ↓
下一次 Diagnosis
```

## 2.4 为三期自动修复准备数据

二期记录：

``` text
Diagnosis
→ Human Action
→ Action Result
→ Recovery Result
```

但不开放 AI 任意 Shell/SSH/生产变更。

------------------------------------------------------------------------

# 3. 二期范围

## M0：真实依赖

-   真实 Prometheus；
-   Kubernetes 只读 API；
-   真实 LLM（DeepSeek 或内网 vLLM）；
-   Pod 类夜莺规则补 `node` 标签；
-   Context Query 校准。

## M1：吞吐与异步化

-   PostgreSQL；
-   PROCESS_LOCK 缩小到 DB 事务边界；
-   Transactional Outbox；
-   后台 Worker；
-   Retry / Dead Letter；
-   Task 幂等；
-   Priority Queue；
-   Incident 聚合通知窗口；
-   Nightingale Batch Webhook；
-   Queue Metrics。

## M2：知识沉淀

-   Loki / Elasticsearch 日志证据；
-   Change Event；
-   Change Correlation；
-   Evidence 统一模型；
-   Postmortem；
-   Knowledge Base；
-   Historical Incident Retrieval；
-   pgvector / Hybrid RAG；
-   Human Feedback；
-   AI Diagnosis v2。

## M3：不属于本期

Runbook、审批、Executor、Verification、Rollback 单独立项。

------------------------------------------------------------------------

# 4. 二期总体架构

``` text
                    Prometheus / K8s / GPU / Bare Metal
                                  │
                                  ▼
                            Nightingale
                                  │ Batch Webhook
                                  ▼
┌────────────────────────────────────────────────────────┐
│                  AIOps Event Gateway                   │
│ Receive → Normalize → Persist → Correlate → Incident  │
└───────────────────────────┬────────────────────────────┘
                            │ Short DB Transaction
                            ▼
                    ┌───────────────┐
                    │ PostgreSQL    │
                    │ Source of Truth│
                    └───────┬───────┘
                            │
                       Task Outbox
                            │
                            ▼
                    ┌───────────────┐
                    │ Worker Queue  │
                    └───────┬───────┘
             ┌──────────────┼───────────────┐
             ▼              ▼               ▼
       Context Worker  Diagnosis Worker  Notify Worker
             │              │               │
     ┌───────┼────────┐     │               ▼
     ▼       ▼        ▼     ▼             Feishu
 Prometheus K8s     Logs   LLM
                    │
                    ├── Change Events
                    └── Historical RAG
                            │
                            ▼
                         Incident
                            │
                     Human Handling
                            │
                         RESOLVED
                            │
                            ▼
                     Postmortem Worker
                            │
                            ▼
                      Knowledge Base
                            │
                            └──────→ 下一次 Diagnosis
```

------------------------------------------------------------------------

# 5. PostgreSQL 设计

一期实际为 SQLite，二期建议正式迁移 PostgreSQL，原因：

-   多 Worker；
-   `FOR UPDATE SKIP LOCKED`；
-   更可靠的并发事务；
-   JSONB；
-   后续 pgvector；
-   长期 Incident 数据增长；
-   为多副本准备。

迁移原则：

``` text
迁移演练
→ 停写
→ SQLite Export
→ PostgreSQL Import
→ 行数/关键字段校验
→ 切换连接
→ 启动
→ readyz/回归测试
```

不建议长期 SQLite/PostgreSQL 双写。

一期表继续保留，新增：

``` text
task_outbox
evidences
change_events
incident_changes
postmortems
knowledge_entries
incident_similarities
human_feedback
```

------------------------------------------------------------------------

# 6. Transactional Outbox

不能采用：

``` text
DB commit
→ queue.publish()
```

否则 DB 成功、Queue 失败时任务会永久丢失。

必须在同一事务内：

``` text
Alert / Incident Update
+
task_outbox INSERT
+
COMMIT
```

## 6.1 task_outbox

``` sql
CREATE TABLE task_outbox (
    id BIGSERIAL PRIMARY KEY,
    task_id VARCHAR(64) UNIQUE NOT NULL,
    task_type VARCHAR(64) NOT NULL,
    aggregate_type VARCHAR(32) NOT NULL,
    aggregate_id VARCHAR(64) NOT NULL,
    priority INTEGER NOT NULL DEFAULT 50,
    payload JSONB NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    attempts INTEGER NOT NULL DEFAULT 0,
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    locked_at TIMESTAMPTZ,
    locked_by VARCHAR(128),
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);
```

状态：

``` text
PENDING → RUNNING → SUCCEEDED
                 └→ RETRY → RUNNING
                 └→ DEAD
```

------------------------------------------------------------------------

# 7. Worker

二期第一版建议直接使用：

``` text
PostgreSQL Outbox + Worker
```

Worker 通过：

``` sql
SELECT ...
FOR UPDATE SKIP LOCKED
```

领取任务，因此可以安全启动多个 Worker。

任务类型：

``` text
COLLECT_CONTEXT
DIAGNOSE_INCIDENT
NOTIFY_INCIDENT_CREATED
NOTIFY_ROOT_CHANGED
NOTIFY_INCIDENT_RESOLVED
BUILD_POSTMORTEM
INDEX_KNOWLEDGE
SYNC_K8S_TOPOLOGY
PURGE_DATA
```

后续三期再增加：

``` text
EXECUTE_RUNBOOK
VERIFY_REMEDIATION
```

------------------------------------------------------------------------

# 8. Task 幂等、重试与 Dead Letter

幂等键示例：

``` text
COLLECT_CONTEXT = incident_id + root_alert_id + context_version
DIAGNOSE        = incident_id + context_version
NOTIFY          = incident_id + card_kind + incident_version
POSTMORTEM      = incident_id + resolved_at
```

Retryable：

``` text
timeout / HTTP 429 / HTTP 5xx / network error
```

退避：

``` text
5s → 30s → 2m → 10m
```

最多 4 次。

Non-Retryable：

``` text
401 / 403 / invalid query / invalid schema
```

直接 `DEAD` 并产生平台自身告警。

优先级：

  Severity     Priority
  ---------- ----------
  P0                100
  P1                 90
  P2                 60
  P3                 40

------------------------------------------------------------------------

# 9. Incident 聚合通知窗口

一期首卡在第一条告警到达时发送，故障风暴时首卡可能显示：

``` text
关联告警：1
```

实际几秒后已经聚合 30 条。

二期新增：

``` text
notify_after
```

推荐：

``` text
P0: 0~3s
P1: 5s
P2: 15s
P3: 30s
```

窗口内只聚合 Alert，不发首卡。窗口结束后：

``` text
Incident Snapshot
→ Context
→ Diagnosis
→ Feishu
```

窗口内发生 Root Promotion 时不额外发送根因变化卡，首卡直接使用最新
Root。

------------------------------------------------------------------------

# 10. 真实 Prometheus

## 10.1 接的是什么：查询 API，不是告警

二期需要的是 Prometheus **HTTP 查询 API**（只读）：

``` text
GET /api/v1/query          即时查询
GET /api/v1/query_range    区间查询（回溯用）
```

**不需要接 Alertmanager / 告警推送**：告警这一层由夜莺承担（夜莺在本架构中就是
告警引擎的角色），一期已经从夜莺 Webhook 收到告警。Prometheus 在这里只负责
「提供指标证据」，不参与告警发现。

## 10.2 多数据源路由

生产环境的指标源不止一个，不能只配一个 URL：

``` text
中心集群      n9e-host-01 192.0.2.250:9090 / prom02 192.0.2.13
其他机房      VictoriaMetrics（Prometheus 兼容 API，端口按现场）
```

因此二期的 `PrometheusProvider` 需要：

``` text
按 cluster / region / projset 选择数据源
→ 未命中映射时回退默认源，并在 Evidence 上标注实际使用的数据源
→ 单个源不可用只影响该源覆盖的工单（降级 PARTIAL，不阻断）
```

配置建议：`AIOPS_PROMETHEUS_SOURCES`（JSON 或 rules yaml），形如
`{"default": "http://192.0.2.250:9090", "dx0-calc1": "http://...", ...}`。

**实测补充（2026-09-11，192.0.2.250 联邦聚合实例）**：

``` text
路由键应优先用 __prom_env__：指标侧带 __prom_env__=cluster02 这类标签，
夜莺告警的 tags 里也有同名的 __prom_env__（一期实测报文里就是 __prom_env__=dx0-calc1-dev），
两边可以直接对上，是天然的关联键。
不要用 cluster 标签：实测该标签几乎不存在（只有零星 series 有）。
region / projset / job 可用作辅助维度（region 覆盖十几个机房）。
```

因此二期路由优先级：`__prom_env__` → `region`/`projset` → 默认源。

## 10.3 查询窗口与压缩

保留一期正确的 Range Query 设计：

``` text
start = first_seen - lookback
end   = last_seen + 60s
```

默认：

``` text
lookback = 600s
step = 15s
```

NCCL/RDMA/GPU 可配置更长窗口。

所有指标结果压缩为：

``` text
first / last / min / max / trend
```

而不是把完整时序送给 LLM。

------------------------------------------------------------------------

# 11. Kubernetes 真实接入

ServiceAccount 只允许：

``` text
get / list / watch
```

**实施建议（评审补充）**：第一版**只使用 get / list（pull 模式）**，不启用 watch。
一期已是 pull（`SYNC_K8S_TOPOLOGY` 周期同步即可），watch 需要额外处理
`410 Gone`、resync、长连接重连，且会显著增加 API Server 压力。
watch 留到确实需要秒级拓扑变更时再启用；RBAC 里可以先授予，但代码不用。

资源：

``` text
nodes
pods
events
namespaces
deployments
statefulsets
daemonsets
replicasets
jobs
cronjobs
```

禁止：

``` text
create / update / patch / delete
```

自动拓扑：

``` text
Pod --RUNS_ON--> Node
Pod --OWNED_BY--> ReplicaSet / Job
ReplicaSet --OWNED_BY--> Deployment
Node --MEMBER_OF--> Cluster
```

夜莺 Pod 告警如果已经携带 `node`，优先使用告警事实，K8s API
用于验证和补充。

------------------------------------------------------------------------

# 12. Evidence 统一模型

二期将所有诊断依据统一为 Evidence：

``` text
METRIC
KUBERNETES
LOG
CHANGE
TOPOLOGY
HISTORICAL
HUMAN
```

``` sql
CREATE TABLE evidences (
    id BIGSERIAL PRIMARY KEY,
    evidence_id VARCHAR(64) UNIQUE NOT NULL,
    incident_id VARCHAR(64) NOT NULL,
    evidence_type VARCHAR(32) NOT NULL,
    source VARCHAR(64) NOT NULL,
    category VARCHAR(64),
    entity_type VARCHAR(64),
    entity_id VARCHAR(255),
    occurred_at TIMESTAMPTZ,
    title VARCHAR(512),
    content JSONB NOT NULL,
    confidence NUMERIC(5,4),
    fingerprint VARCHAR(128),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

Evidence 是二期 AI 可解释性的核心。

**去重约束（评审补充）**：必须加唯一索引，否则根因提升触发 context v2、
或人工 reanalyze 时，同一份证据会被反复插入：

``` sql
CREATE UNIQUE INDEX uq_evidence_incident_fingerprint
    ON evidences (incident_id, fingerprint);
```

`fingerprint` 建议 = `evidence_type + source + entity_id + 内容摘要哈希`，
与一期 `incident_alerts.uq_incident_alert` 的思路一致（应用层先查后建只是快路径，
唯一索引才是硬约束）。

------------------------------------------------------------------------

# 13. 日志证据

支持：

``` text
Loki
或
Elasticsearch
```

抽象统一 `LogProvider`：

``` python
search_logs(entity, start, end, selectors, limit)
```

禁止让 LLM 自由生成无限范围日志查询，采用：

``` text
Incident Type → Log Query Template
```

示例：

``` yaml
NodeNotReady:
  - category: kubelet
    keywords: ["NotReady", "heartbeat", "runtime"]
  - category: kernel
    keywords: ["link", "mlx", "bond", "oom"]

GPUXidError:
  - category: kernel
    keywords: ["NVRM", "Xid"]

NCCLTimeout:
  - category: training
    keywords: ["NCCL", "timeout", "NET/"]
```

日志必须经过：

``` text
Search
→ Filter
→ Deduplicate
→ Pattern Extract
→ Top N
→ Evidence
```

禁止直接把数万行日志送入模型。

------------------------------------------------------------------------

# 14. Change Event

二期把 Change 作为一等事件。

第一批来源建议：

``` text
Kubernetes Deployment Rollout
Scale
Config Change
Node cordon/drain
Ansible Job
人工登记
```

后续：

``` text
CI/CD
GitHub/GitLab
云审计
堡垒机操作
```

Schema：

``` json
{
  "change_id": "CHG_xxx",
  "source": "kubernetes",
  "change_type": "DEPLOYMENT_ROLLOUT",
  "entity_type": "deployment",
  "entity_id": "llm-api",
  "cluster": "prod",
  "namespace": "default",
  "actor": "user/system",
  "occurred_at": "...",
  "summary": "...",
  "before": {},
  "after": {}
}
```

------------------------------------------------------------------------

# 15. Change Correlation

Incident 创建后查询：

``` text
first_seen - 30m
~
last_seen + 5m
```

优先级：

``` text
same entity
same workload
same node
same namespace
same cluster
topology distance <= 2
```

命中后生成：

``` text
CHANGE Evidence
```

例如：

``` text
09:55 Deployment image changed
10:02 Pod CrashLoop
```

AI 可以说：

``` text
“故障前 7 分钟存在直接相关发布变更。”
```

但不能直接断言发布就是根因。

------------------------------------------------------------------------

# 16. Context Collector v2

一期 Context Collector 升级为：

``` text
Context Orchestrator
  ├─ MetricCollector
  ├─ KubernetesCollector
  ├─ LogCollector
  ├─ ChangeCollector
  └─ HistoricalCollector
```

统一：

``` python
class Collector:
    def collect(self, incident, budget) -> list[Evidence]:
        ...
```

建议预算：

``` text
Prometheus  30 queries
K8s        10 calls
Logs       10 queries
Changes     5 queries
Historical  5 queries
总 deadline 30s
```

> 与一期对齐：一期 `AIOPS_CONTEXT_MAX_QUERIES` 默认 60（只有 Prometheus 一个 collector）。
> 二期加入 Logs / Changes / Historical 后，指标查询收紧到 **30** 并纳入统一总预算，
> 实施时同步改配置默认值，避免「文档写 30、代码跑 60」。

超预算：

``` text
context_status = PARTIAL
```

不阻断后续流程。

------------------------------------------------------------------------

# 17. Historical Incident Retrieval

检索源优先使用：

``` text
RESOLVED
+
人工确认根因
```

的 Incident。

采用 Hybrid Retrieval：

``` text
Structured Filter
+
Rule Similarity
+
Vector Similarity
```

Structured：

``` text
alertname
entity_type
accelerator_model
root_cause_category
cluster/environment
```

Rule Similarity：

``` text
Root Alert
Alert Set Jaccard
Evidence Category
Topology Pattern
```

Vector 文本：

``` text
故障摘要
+
确认根因
+
关键证据
+
人工处置
+
处理结果
```

建议 PostgreSQL 使用 `pgvector`，暂不额外部署专用 Vector DB。

------------------------------------------------------------------------

# 18. Postmortem

Incident `RESOLVED` 后产生：

``` text
BUILD_POSTMORTEM
```

结构：

``` text
Summary
Impact
Detection
Timeline
Root Cause
Contributing Factors
Evidence
Actions Taken
Recovery Verification
What Worked
What Did Not Work
Monitoring Gaps
Follow-up Actions
Knowledge Tags
```

自动计算：

``` text
TTD = first_received - first_seen
TTA = acknowledged_at - first_seen
TTR = resolved_at - first_seen
```

缺字段则为 `null`，禁止 AI 编造。

------------------------------------------------------------------------

# 19. Knowledge Entry

只有 Incident 关闭后才生成知识条目。

``` json
{
  "knowledge_id": "KB_xxx",
  "incident_id": "INC_xxx",
  "title": "H800节点因RoCE链路异常导致NCCL Timeout",
  "symptoms": [],
  "root_cause": {
    "category": "NETWORK_RDMA",
    "summary": "RoCE链路异常"
  },
  "evidence": [],
  "resolution": {
    "actions": [],
    "result": "resolved"
  },
  "environment": {
    "accelerator_model": "H800"
  },
  "lessons": [],
  "verified": true
}
```

------------------------------------------------------------------------

# 20. RAG 防污染

这是二期必须重点控制的问题。

高可信知识：

``` text
RESOLVED
+
confirmed_root_cause
+
人工确认
```

未人工确认：

``` text
verified=false
```

降低检索权重。

禁止：

``` text
AI猜测 RCA
→ 自动写成 verified Knowledge
→ 下一次 AI 当成事实
```

否则会形成错误知识自我强化。

------------------------------------------------------------------------

# 21. Human Feedback

新增：

``` text
DIAGNOSIS_CORRECT
DIAGNOSIS_PARTIAL
DIAGNOSIS_WRONG
CORRELATION_CORRECT
CORRELATION_WRONG
ROOT_CAUSE_CONFIRMED
RECOMMENDATION_USEFUL
RECOMMENDATION_NOT_USEFUL
```

Incident 增加：

``` text
confirmed_root_cause
resolution_summary
resolved_by
```

人工确认永远高于 AI `suspected_root_cause`。

------------------------------------------------------------------------

# 22. AI Diagnosis v2

输入：

``` text
Incident
+
Alerts
+
Topology
+
Metric Evidence
+
Kubernetes Evidence
+
Log Evidence
+
Change Evidence
+
Historical Cases
```

输出：

``` json
{
  "summary": "...",
  "root_cause_candidates": [
    {
      "cause": "RoCE network degradation",
      "confidence": 0.82,
      "evidence_ids": ["EVD_001", "EVD_004"]
    }
  ],
  "primary_root_cause": "...",
  "confidence": 0.82,
  "impact": {},
  "change_correlation": {},
  "historical_cases": [],
  "recommended_checks": [],
  "recommended_actions": [],
  "needs_human": true
}
```

**字段兼容（评审修正）**：一期输出与消费的字段是 `suspected_root_cause`
（app/integrations/deepseek.py，飞书卡片与 `/api/v1/incidents/{id}` 都读它）。
二期新增 `primary_root_cause` / `root_cause_candidates` 时必须**同时回写**
`suspected_root_cause`，或者同步改掉卡片与接口的读取字段 ——
否则上线当天飞书卡片的「初步判断」会直接变空。
建议：新字段先并行写入，旧字段保留一个版本后再移除。

------------------------------------------------------------------------

# 23. Confidence 二次约束

模型自报 confidence 不能直接使用。

平台执行 Evidence Cap：

``` text
无 Evidence                → max 0.30
只有 Historical Evidence   → max 0.50
Metrics + K8s              → 可 >0.60
Metrics + Logs 一致        → 可 >0.70
Metrics + Logs + Change
+ Historical 一致          → 可 >0.80
```

> 一期实现：模型未给出 evidence 时封顶 0.30（app/integrations/deepseek.py）。
> 二阶的上限沿用一期的 0.30，不要写成 0.35，否则验收时两边数字对不上。

``` text
effective_confidence =
min(model_confidence, evidence_cap)
```

------------------------------------------------------------------------

# 24. Context / Diagnosis Version

增加：

``` text
context_version
diagnosis_version
```

触发新版本：

``` text
Incident Root Changed
重要 Change 到达
人工 Reanalyze
```

例如：

``` text
context v1 → diagnosis v1
root promoted
context v2 → diagnosis v2
```

历史版本必须保留。

------------------------------------------------------------------------

# 25. 飞书卡片 v2

保留一期卡片结构，新增：

``` text
同期变更
历史相似故障
知识库建议
```

示例：

``` text
🚨 P1 GPU节点通信异常

Incident: INC-20260911-001

影响:
4 Nodes / 32 GPU / 2 Jobs

初步根因:
RoCE 网络异常

置信度:
82%

关键证据:
1. RDMA retry 故障前升高
2. bond0 drop 同期升高
3. NCCL日志出现timeout

同期变更:
09:55 网络配置发生变更

历史相似:
INC-20260821-013
相似度 88%
历史处置：检查交换机PFC配置后恢复

建议:
1. 检查 PFC/ECN
2. 检查 NIC counter
3. 对比交换机端口
```

建议交互（**已按「不使用自建应用」修正，见 §49**）：

``` text
[查看工单]      ← URL 按钮，跳转到工单 API / 后续极简页面
```

一期使用的是**自定义机器人 Webhook**（`/open-apis/bot/v2/hook/...`）：
它只能发送卡片，**不能接收卡片按钮回调**。因此
`[确认根因] [诊断不准确] [重新分析]` 这类回调按钮本期不做，
人工反馈走平台 API（§49.4）。

所有交互仍写入 Timeline + Human Feedback。

------------------------------------------------------------------------

# 26. 二期 API

保留一期 API，新增：

``` http
GET  /api/v1/incidents/{id}/evidence
GET  /api/v1/incidents/{id}/changes
GET  /api/v1/incidents/{id}/similar
GET  /api/v1/incidents/{id}/postmortem

POST /api/v1/incidents/{id}/feedback
POST /api/v1/incidents/{id}/confirm-root-cause
POST /api/v1/incidents/{id}/analyze      ← 一期已有此路径（重跑上下文 + 诊断）

POST /api/v1/changes

GET  /api/v1/knowledge
GET  /api/v1/knowledge/{id}
```

------------------------------------------------------------------------

# 27. 二期代码目录

``` text
app/
├── api/
│   ├── routes.py
│   ├── changes.py
│   ├── knowledge.py
│   └── feedback.py
├── services/
│   ├── pipeline.py
│   ├── incident_service.py
│   ├── context_collector.py
│   ├── evidence_service.py
│   ├── change_service.py
│   ├── postmortem_service.py
│   ├── knowledge_service.py
│   └── retrieval_service.py
├── workers/
│   ├── runner.py
│   ├── context_worker.py
│   ├── diagnosis_worker.py
│   ├── notification_worker.py
│   ├── postmortem_worker.py
│   └── topology_worker.py
├── queue/
│   ├── outbox.py
│   ├── dispatcher.py
│   └── retry.py
├── integrations/
│   ├── prometheus.py
│   ├── kubernetes.py
│   ├── loki.py
│   ├── elasticsearch.py
│   ├── llm.py
│   ├── embedding.py
│   └── feishu.py
├── retrieval/
│   ├── structured.py
│   ├── similarity.py
│   └── reranker.py
└── db/
    ├── models.py
    ├── queries.py
    └── migrations/
```

------------------------------------------------------------------------

# 28. 自身可观测性

一期指标继续保留，新增：

``` text
aiops_task_queue_depth{type,status}
aiops_task_oldest_age_seconds{type}
aiops_task_processing_duration_seconds{type}
aiops_task_retry_total{type}
aiops_task_dead_total{type}
aiops_worker_active
aiops_worker_heartbeat_timestamp

aiops_context_evidence_total{type}
aiops_context_partial_total{source}
aiops_log_query_duration_seconds
aiops_change_events_total
aiops_rag_query_duration_seconds
aiops_rag_results_total
aiops_postmortem_total{ok}
aiops_feedback_total{type}
aiops_diagnosis_confidence
```

AIOps 自身必须对：

``` text
Queue Depth
Oldest Task Age
Dead Task
Worker Heartbeat
Collector Failure
```

配置夜莺告警。

------------------------------------------------------------------------

# 29. 安全边界

## LLM

公网模型必须默认：

``` text
AIOPS_MASK_ASSETS=true
```

禁止发送：

``` text
密码
Token
Private Key
Secret
AK/SK
完整用户凭据
```

## Kubernetes

只读 SA。

## Logs

日志进入 LLM 前过滤：

``` text
Authorization
Cookie
Token
Password
Secret
AK/SK
```

## Knowledge

知识库不得保存明文凭据和完整 Secret。

------------------------------------------------------------------------

# 30. 故障降级

任何 Collector 失败均不能阻断 Incident。

例如：

``` text
Prometheus OK
K8s OK
Loki FAIL
Historical OK
```

结果：

``` text
Context = PARTIAL
```

AI 必须明确：

``` text
未取得日志证据
```

而不是补全不存在的数据。

------------------------------------------------------------------------

# 31. 数据保留

建议：

  对象                              保留
  ------------------- ------------------
  raw_events                       30 天
  原始日志 Evidence                30 天
  压缩 Evidence                   180 天
  llm_calls                       180 天
  feishu_messages                  90 天
  Incident Timeline     长期/随 Incident
  Incident                          长期
  Postmortem                        长期
  Knowledge                         长期
  Human Feedback                    长期

> 注意（评审补充）：一期已明确 **`alerts` 与 `incidents` 是长期资产、不自动清理**
> （一期文档 §49.3），二期不要把它们纳入定期删除 —— 它们是复盘与 RAG 的数据基础。
> 仍需清理的只有 raw_events、Evidence、llm_calls、feishu_messages。

二期应解决一期"时间线/推送只按时间删除"的问题，长期知识数据按 Incident
归属管理。

------------------------------------------------------------------------

# 32. 二期 KPI

重点指标：

``` text
Webhook P95/P99
告警压缩率
关联误合并率
关联漏合并率
detach_rate
manual_merge_rate
Evidence完整率
AI有证据输出率
人工诊断认可率
历史案例Top3命中率
Queue Delay
MTTA
MTTR
```

AI 质量按：

``` text
alertname
cluster
root_cause_category
```

分别统计，不只看总体准确率。

------------------------------------------------------------------------

# 33. M0 验收

### Prometheus

真实 NodeNotReady 工单：

``` text
关键证据出现真实故障前指标回溯
```

### Kubernetes

Pod 告警：

``` text
Pod → Node
```

自动建立。

### LLM

按一期字段语义核对（`provider` 恒为 `deepseek`，规则兜底时 `model` 才是 `rule-stub`）：

``` text
llm_calls.model <> 'rule-stub'
llm_calls.ok = true
llm_calls.parsed ->> 'engine' LIKE 'llm:%'
```

> 评审修正：原文写 `llm_calls.provider != rule-stub` 无法作为判据 ——
> 一期 `_record` 里 provider 是硬编码的 "deepseek"，
> 规则兜底写入的是 `model='rule-stub'`、`ok=false`、`error='no_api_key'`。

### 夜莺

Pod 类 Alert：

``` text
node label 可用于关联
```

------------------------------------------------------------------------

# 34. M1 验收

## 慢 Prometheus

注入 5s 延迟：

``` text
Webhook DB Persist P99 不明显退化
```

## 慢 LLM

注入 30s：

``` text
新 Alert 持续正常入库
```

## Worker Crash

任务 RUNNING 时杀 Worker：

``` text
Lease 超时后可重新领取
```

## 重复消费

同一 Notify Task 执行两次：

``` text
飞书只能发送一次
```

> 达成这条验收需要一个**硬约束**（评审补充）：`task_outbox.task_id` 的 UNIQUE
> 只能保证不重复入队；Worker 租约超时后重新领取、或人工重放，仍会重复投递。
> 因此需要给 `feishu_messages` 加唯一约束（或单独的 notify 去重表）：

``` sql
CREATE UNIQUE INDEX uq_feishu_once
    ON feishu_messages (incident_id, kind, dedup_key);
```

`dedup_key` = `incident_id + card_kind + incident_version`（与 §8 的 NOTIFY 幂等键一致）。
一期 `feishu_messages` 目前没有唯一约束，属于纯应用层保证；二期必须补上。

## 30 条告警风暴

``` text
Incident = 1
首卡 alert_count 接近聚合后的实际数量
```

------------------------------------------------------------------------

# 35. M2 验收

### Logs

NodeNotReady：

``` text
返回 kubelet/kernel 关键日志 Evidence
```

### Change

Deployment 后 CrashLoop：

``` text
Timeline 出现同期 Change
```

### Historical

构造历史相似 Incident：

``` text
Top 3 中出现正确案例
```

### Postmortem

Incident Resolved：

``` text
自动生成结构化 Postmortem
```

### Knowledge

人工确认根因后：

``` text
生成 verified Knowledge Entry
```

------------------------------------------------------------------------

# 36. 性能目标

初始建议：

``` text
Webhook DB Persist P95 < 200ms
Webhook DB Persist P99 < 500ms

单批 100 Events 可正常接收

外部依赖 30s Timeout
不影响新事件持久化

P0 Queue Oldest Age < 10s
P1 Queue Oldest Age < 30s
```

上线后根据真实 30 天告警基线调整。

------------------------------------------------------------------------

# 37. 二期重点真实场景

建议覆盖：

``` text
1. NodeNotReady
2. Pod CrashLoop / OOM
3. DiskFull
4. GPU Xid
5. GPU Missing
6. NCCL Timeout
7. RDMA / RoCE
8. Kubelet异常
```

重点建议验证：

``` text
GPU Xid
NCCL
RDMA/RoCE
```

因为这些场景天然需要 Metrics + Logs + Topology + History。

------------------------------------------------------------------------

# 38. NCCL / RDMA 标准诊断链

``` text
Training Job Error
      ↓
NCCL Timeout
      ↓
Pod / Node Mapping
      ↓
GPU / NIC / RDMA Mapping
      ↓
Prometheus
  ├─ NIC drop
  ├─ retry
  ├─ throughput
  └─ GPU util
      ↓
Logs
  ├─ NCCL
  ├─ RDMA
  └─ kernel
      ↓
Change
  ├─ driver
  ├─ network
  └─ node operation
      ↓
Historical Incident
      ↓
AI RCA
```

------------------------------------------------------------------------

# 39. 三期 Runbook 数据准备

二期 Timeline 新增：

``` text
OPERATOR_ACTION_RECORDED
```

结构：

``` json
{
  "action": "restart_kubelet",
  "target": "node01",
  "operator": "...",
  "result": "success",
  "incident_recovered": true
}
```

长期统计：

``` text
故障类型
+
诊断
+
人工动作
+
成功率
```

只有真实数据足够后，三期才把高成功率、低风险动作转成 Runbook。

------------------------------------------------------------------------

# 40. 推荐实施顺序

``` text
Phase 2.0
真实 Prometheus / K8s / LLM
        ↓
Phase 2.1
PostgreSQL
        ↓
Phase 2.2
Outbox / Worker / 副作用出锁
        ↓
Phase 2.3
聚合窗口 / Batch Webhook
        ↓
Phase 2.4
Evidence / Logs / Change
        ↓
Phase 2.5
Postmortem / Knowledge
        ↓
Phase 2.6
Historical Retrieval / RAG
        ↓
Phase 2.7
Human Feedback / AI Evaluation
```

------------------------------------------------------------------------

# 41. 8 周建议排期

  周       主要工作
  -------- -----------------------------------------------
  Week 1   真实 Prometheus、K8s、LLM、Pod node 标签
  Week 2   PostgreSQL Schema、SQLite Migration、迁移演练
  Week 3   Outbox、Worker、Retry、Dead Letter、幂等
  Week 4   副作用出锁、聚合窗口、Batch Webhook、性能压测
  Week 5   Evidence、Loki/ES、Change Event
  Week 6   Postmortem、Knowledge、人工确认根因、Feedback
  Week 7   Hybrid Retrieval、pgvector、Historical RAG
  Week 8   Diagnosis v2、飞书 v2、综合演练与验收

------------------------------------------------------------------------

# 42. 开工前必须确认

1.  **LLM**：公网 DeepSeek 还是内网 vLLM；资产信息是否允许出网。
2.  **Kubernetes**：API Server、只读 SA Token、CA。
3.  **Logs**：当前使用 Loki 还是 Elasticsearch；kubelet/kernel/training
    日志是否已集中采集。
4.  **Change**：第一批接 Kubernetes、Ansible、CI/CD 还是人工事件。
5.  **PostgreSQL**：实例从哪来（本机容器 / 独立实例 / 现有集群）、
    谁维护备份、是否接受一次停机迁移（落地清单见 §48）。
6.  **聚合窗口与延迟预算**：确认分级目标（见 §50），
    P0/P1 是否同意「不进批、不进窗口」。
7.  **告警基线**：准备近 30 天告警量，用于压测和压缩率基线。
8.  **Embedding**（RAG 前置）：内网部署哪个 embedding 模型、向量维度（见 §51）。
9.  **飞书**：已确认本期不使用自建应用，人工反馈走 API / 极简页面（见 §49）。

> 已确认：飞书反馈交互**不使用自建应用**（不做卡片按钮回调）。

------------------------------------------------------------------------

# 43. 推荐第一条开发主线

不要先做 RAG。

先跑通：

``` text
真实 NodeNotReady
      ↓
Nightingale
      ↓
Gateway
      ↓
PostgreSQL
      ↓
Incident
      ↓
Outbox
      ↓
Worker
      ↓
Prometheus Real Evidence
      ↓
K8s Real Evidence
      ↓
LLM Diagnosis
      ↓
聚合窗口
      ↓
Feishu
```

然后故意注入：

``` text
Prometheus delay = 5s
LLM delay = 30s
```

验收：

``` text
新的 Nightingale Alert
仍可持续快速落库。
```

这条链稳定后再加入：

``` text
Logs
→ Change
→ Postmortem
→ Knowledge
→ RAG
```

------------------------------------------------------------------------

# 44. 二期与三期边界

二期：

``` text
AI 可以：
读取 / 查询 / 关联 / 诊断 / 检索历史 / 生成复盘 / 推荐操作

AI 不可以：
直接执行生产变更
```

三期：

``` text
Runbook
+
Policy Engine
+
Human Approval
+
Executor
+
Verification
+
Rollback
```

------------------------------------------------------------------------

# 45. 二期成功标准

一期解决：

> 这么多告警是不是同一个故障？

二期要解决：

> 这个故障发生了什么、为什么发生、故障前有什么证据、是否存在相关变更、以前是否发生过、以前如何处理，以及如何把本次经验变成下一次可复用知识。

最终一个 Incident 应包含：

``` text
Incident
├─ Root Alert
├─ Related Alerts
├─ Correlation Reasons
├─ Topology
├─ Metric Evidence
├─ Kubernetes Evidence
├─ Log Evidence
├─ Change Evidence
├─ Historical Cases
├─ AI Diagnosis
├─ Human Feedback
├─ Timeline
├─ Resolution
└─ Postmortem
      ↓
   Knowledge Entry
```

------------------------------------------------------------------------

# 46. 二期最终交付物

1.  真实 Prometheus；
2.  真实 Kubernetes 只读接入；
3.  真实 LLM；
4.  PostgreSQL；
5.  SQLite → PostgreSQL Migration Tool；
6.  Transactional Outbox；
7.  Worker；
8.  Retry / Dead Letter；
9.  Task Idempotency；
10. Priority Queue；
11. Incident 聚合窗口；
12. Nightingale Batch Webhook；
13. Queue Metrics；
14. Evidence Model；
15. Loki / Elasticsearch Collector；
16. Change Event Gateway；
17. Change Correlation；
18. Context Collector v2；
19. AI Diagnosis v2；
20. Historical Incident Retrieval；
21. pgvector / Embedding；
22. Postmortem；
23. Knowledge Entry；
24. Human Feedback；
25. Confirmed Root Cause；
26. Feishu Incident Card v2；
27. 诊断质量指标；
28. 关联质量指标；
29. GPU Xid / NCCL / RDMA 等真实场景测试；
30. 二期部署、迁移与运维 SOP。

------------------------------------------------------------------------

# 47. 最终数据闭环

``` text
Alert
  ↓
Incident
  ↓
Evidence
  ↓
Diagnosis
  ↓
Human Action
  ↓
Resolution
  ↓
Postmortem
  ↓
Knowledge
  └──────────────→ 下一次 Incident
```

二期真正需要建设的核心不是"再加一个 AI"，而是：

``` text
Async Incident Pipeline
+
Real Evidence
+
Logs
+
Changes
+
Historical Incidents
+
Postmortem
+
Knowledge
+
Human Feedback
```

这套数据基础成熟以后，三期 Runbook 和自动修复才有可靠的工程基础。

------------------------------------------------------------------------

# 48. PG 落地清单（M1 前置）

> §5 只说了"要不要迁"。本节是"真要迁"时必须先定的事，按顺序确认，缺一项不要开工。

## 48.1 实例与凭据

| 项 | 需要确认 | 建议 |
| --- | --- | --- |
| 实例来源 | 本机容器 / 独立实例 / 现有集群 | 一期就在 192.0.2.115，优先本机容器或同机房独立实例，避免跨机房查询 |
| 版本 | ≥ 14 | 需要 `SKIP LOCKED`（9.5+）、JSONB、后续 pgvector |
| 凭据 | 用户名/库名 | 写入 `/etc/aiops-gateway.env`（600 权限），与一期飞书 webhook 同一位置 |
| 扩展 | 建库时就要装 | `pgvector` 二期的 RAG 要用，**建库时一并 `CREATE EXTENSION vector`**，后补要停服 |
| 连接数 | Worker 数量 × 连接 | 一期是单进程；二期多 Worker，需要连接池（pgbouncer 或 SQLAlchemy pool 上限） |

## 48.2 迁移窗口与夜莺重试的冲突（最容易丢告警的一步）

夜莺那个 Callback 媒介的配置是 **重试 3 次 / 间隔 3000ms**：

``` text
首次失败 → +3s 重试 → +6s 重试 → +9s 重试 → 放弃（告警永久丢失）
```

也就是说**停写窗口一旦超过约 10 秒，这段时间内的告警会被夜莺直接丢弃且无法恢复**。
§5 的迁移流程只写了"停写"，没有窗口上限，照着做很容易丢数据。

三种可选方案（按推荐度）：

``` text
方案 A（推荐）：先导出再停写
  1. 正常导出（不停写）：sqlite3 .dump / .backup 出全量
  2. 进入停写，只做「最后一次增量」：重新 dump 或用收到的偏移补差
  3. 切换连接 → 起服 → 处理积压（目标窗口 < 10s）

方案 B：切换期间临时改夜莺指向备用接收端
  夜莺侧把告警临时发给一个极简落盘接收端（可以是本机的 /api/v1/events 干跑实例），
  切换完成后回放这批 raw 事件；代价是要在夜莺里改一次媒介配置。

方案 C：接受缺口
  明确记录「切换窗口内丢失的告警」并接受，仅适用于非关键时段。
```

无论选哪种，**迁移必须在夜莺告警低谷期做**，并提前在群里公告。

## 48.3 DB 与归档文件必须同批迁移

一期 `raw_events` 表**只存「文件路径 + 字节偏移」**，正文在 `data/raw_events/*.jsonl`：

``` text
DB 换到别的机器，但 JSONL 没跟着走 → 所有归档指针失效（复核原始事件全部读不到）
```

迁移清单里必须包含：`data/raw_events/` 整个目录（或改成对象存储，另立项）。

## 48.4 备份与恢复

| 项 | 一期（SQLite） | 二期（PostgreSQL） |
| --- | --- | --- |
| 备份命令 | `sqlite3 aiops.db ".backup ..."` | `pg_dump -Fc` |
| 频率 | 未自动化 | 每日全量 + WAL 归档（PITR） |
| 保留 | 手动 | 至少 30 天，且与告警数据保留策略对齐 |
| 演练 | 未做 | **迁移前后各做一次恢复演练**，记录耗时 |

## 48.5 单副本约束与锁改造（最容易漏的一项）

一期 `PROCESS_LOCK` 是**进程内**锁（`threading.RLock`）。换 PG **不会**自动让多副本安全：

``` text
只换 PG 不改锁 → 两个副本各自持本地锁 → 仍会重复建工单、重复发卡片
```

因此 M1 必须同时完成：

``` text
① 锁粒度收窄：只包 DB 读-判-写（一期已规划，见**一期文档** §61.2 技术债 #1）
② 跨副本互斥：改用 PG advisory lock，或让「读-判-写」完全靠
   SKIP LOCKED + 唯一索引保证（工单去重已有 uq_incident_alert 这类硬约束可复用）
③ 启动校验：多副本时必须在 /readyz 上暴露 advisory lock 状态
```

## 48.6 回滚方案

``` text
1. 切换前完整保留 SQLite 文件与 raw_events 目录（只读冻结，不删）
2. 连接方式做成开关（AIOPS_DB_URL），改配置即可切回
3. 回滚判定：切换后 1 小时内出现无法定位的数据错误 → 切回 SQLite 并保留 PG 现场
4. 回滚后必须核对：切换期间在 PG 里产生的告警是否需要补录
```

## 48.7 迁移验收清单（逐条打勾才允许切生产）

``` text
[ ] 行数校验：raw_events / alerts / incidents / incident_alerts / incident_events 全表比对
[ ] 关键字段校验：随机抽样 20 条工单，逐字段比对（含时间字段的时区口径）
[ ] 时间字段校验：确认所有 TIMESTAMPTZ 读回与一期 UTCDateTime 口径一致（不出现少 8 小时）
[ ] 唯一约束重建：uq_alerts_active_fingerprint / uq_incident_alert / uq_relation 全部存在
[ ] 索引重建：idx_incidents_status_last_seen 等全部存在
[ ] 归档指针校验：随机 5 条 raw_events 用 payload_file + offset 能读回原文
[ ] 接口回归：28 个一期用例在 PG 上全绿 + /readyz 正常
[ ] 迁移演练：至少完整演练一次，记录停写窗口实际耗时（必须 < 10s）
[ ] 回滚演练：演练一次切回 SQLite
```

## 48.8 数据访问层改造要点

一期 SQLite 特有的假设在 PG 上**不成立，必须逐条改**：

| 一期假设 | PG 上的问题 | 改法 |
| --- | --- | --- |
| `autoflush=False` + 显式 `flush()` | 可保留 | 不变，但计数查询前置 flush 的注释要保留 |
| 部分唯一索引 `sqlite_where` | PG 语法不同 | 改 `postgresql_where`（同一语义，SQLAlchemy 支持条件索引） |
| `UTCDateTime` TypeDecorator | PG 有 `TIMESTAMPTZ`，不再需要手工去 tzinfo | 保留 Decorator 但底层切 `TIMESTAMP(timezone=True)`，避免两套口径 |
| `PRAGMA journal_mode=WAL` 等 | PG 无此概念 | 连接事件里按 dialect 分支，不要无条件执行 |
| `BigInteger().with_variant(Integer, "sqlite")` | PG 直接用 BIGSERIAL/BIGINT | 保留 variant 写法即可 |
| `busy_timeout` / 单写者 | 换成行级锁与 MVCC | 锁改造见 §48.5 |

------------------------------------------------------------------------

# 49. 飞书交互方案（不使用自建应用）

## 49.1 约束

一期接入的是**自定义机器人 Webhook**：

``` text
POST https://open.feishu.cn/open-apis/bot/v2/hook/<token>
```

它的能力边界：

| 能力 | 是否支持 |
| --- | --- |
| 发送文本 / 富文本 / 交互式卡片 | ✅ |
| 卡片内 URL 按钮（跳转） | ✅ |
| 卡片按钮点击回调到平台 | ❌ |
| 接收群消息 / @机器人指令 | ❌ |
| 读取群成员、更新历史卡片 | ❌ |

**结论：本期不做卡片按钮回调。** §25 原稿里的
`[确认根因] [诊断不准确] [重新分析] [查看历史案例] [关闭工单]` 全部作废。

## 49.2 卡片上能做什么

``` text
[查看工单]  → URL 按钮，跳转到 GET /api/v1/incidents/{id}
              （二期有极简页面后换成页面地址）
```

卡片正文里可以放一行操作说明，例如：

``` text
处置与反馈：POST /api/v1/incidents/{id}/feedback 或回复工单号给值班同学录入
```

一期的 `notify_*` 走的是 `msg_type=interactive` + `card`，加 URL 按钮只需在
elements 里加 `{"tag":"action","actions":[{"tag":"button","url":...}]}`，不需要新权限。

## 49.3 人工反馈的落地方式（本期三选一或组合）

| 方式 | 说明 | 成本 | 建议 |
| --- | --- | --- | --- |
| A. 纯 API | `POST /incidents/{id}/feedback`、`/confirm-root-cause`、`/ack`、`/resolve` | 0 | 必做，是其它方式的基础 |
| B. 极简页面 | 单页只做「工单列表 + 详情 + 三个反馈按钮」，直接调 A 的接口 | 低（1~2 天） | **推荐**，值班同学才可能真的用 |
| C. 群里固定格式 | 值班同学在群里回固定格式文本，人工/脚本录入 | 低 | 过渡期可用，但要人工转录 |

> 若采用 B，注意它天然是只读+受限写：只允许 ack/resolve/feedback，
> 不允许改工单关联关系（避免页面成为绕过关联规则的后门；
> 关联纠错仍走 `/detach`，由运维按需调用）。

## 49.4 反馈数据必须落库（与 §21 对应）

``` text
feedback: incident_id + feedback_type + actor + comment + created_at
→ incident_events 写一条 HUMAN_FEEDBACK
→ human_feedback 表留档
→ 人工确认的根因写入 confirmed_root_cause（优先级永远高于 AI suspected_root_cause）
```

## 49.5 将来升级到自建应用的路径（本期不做，仅记录）

若后续确实需要「卡片点一下就反馈」，需要：

``` text
1. 创建企业自建应用，拿到 app_id / app_secret
2. 配置「卡片回调」地址（必须是 HTTPS 公网可达或走内网代理）
3. 校验请求签名，处理 challenge 握手
4. 卡片改用 card 2.0 + callback 行为
5. 群机器人改为应用机器人（或两者并存）
```

工作量与运维成本都明显高于本期收益，故本期不做。

------------------------------------------------------------------------

# 50. 延迟预算（从告警产生到群消息）

> 一期只定义了单点超时（上下文 30s、LLM 60s、飞书 8s），没有端到端预算。
> 二期引入聚合窗口与批处理后，**必须给整链设上限**，否则最坏情况可能 1 分钟以上才到群。

## 50.1 链路分解与归属

| 段 | 归属 | 一期实测/设定 | 二期目标 |
| --- | --- | --- | --- |
| 告警评估 | 夜莺 | 规则 `for_duration`（现场配置，常见 60s） | 不变 |
| 推送（含重试） | 夜莺 | 失败重试 3 次 × 3s | 不变 |
| 入库（Raw/Alert/Incident） | 网关 | 目标 P95 < 200ms | 不变（副作用出锁后应更稳） |
| 聚合窗口 | 网关 | 无 | 分级：P0 0~3s / P1 5s / P2 15s / P3 30s |
| 上下文采集 | 网关 Worker | deadline 30s | 30s（超预算降级 PARTIAL 继续） |
| 诊断（LLM） | 网关 Worker | timeout 60s × 重试 | **实测单次 16s**（deepseek-v4-flash）→ P0/P1 超时建议放宽到 20s，否则会频繁走「先发初步卡片再补」路径 |
| 发送飞书 | 网关 Worker | 8s | 不变 |

## 50.2 分级预算（必须同时满足）

``` text
P0  incident → 群消息  ≤ 30s   （不含夜莺 for_duration）
P1  incident → 群消息  ≤ 45s
P2  incident → 群消息  ≤ 90s
P3  incident → 群消息  ≤ 150s
```

## 50.3 组合规则（避免延迟叠加）

``` text
           批量接收    聚合窗口    诊断超时
P0/P1      不用        不用        20s（实测单次 16s），超时先发初步卡
P2         可用        15s         60s
P3         可用        30s         60s
```

要点：

- **P0/P1 既不进批也不进窗口**，走最短路径；
- P2/P3 才叠加「批 + 窗口」，且两者叠加后的等待上限写死（P3 ≤ 30s + 批间隔）；
- §9 的"窗口内 Root Promotion 不额外发卡、首卡直接用最新 Root"保持不变。

## 50.4 超时护栏

``` text
P0/P1 诊断超时或失败：
  → 立即发出「初步卡片」（含 Root + 关联告警数 + 已有证据 + 明确写"诊断未完成"）
  → 诊断完成后作为一条更新（同 kind 幂等，见 §34 唯一约束）补发
不允许因为等 AI 而让 P0/P1 的首卡超过 30s
```

## 50.5 需要埋的指标

``` text
aiops_incident_to_card_seconds{severity}          端到端时延（直方图）
aiops_notify_wait_seconds{kind}                   聚合窗口实际等待
aiops_diagnosis_sla_exceeded_total{severity}      诊断超预算次数
```

没有这三个指标，§50.2 的预算无法验收。

------------------------------------------------------------------------

# 51. Embedding 与脱敏（RAG 前置）

## 51.1 embedding 服务选型

| 项 | 要求 | 说明 |
| --- | --- | --- |
| 部署位置 | **内网优先** | 若资产信息不允许出网，embedding 也不能出网（全文/摘要比 IP 更敏感） |
| 候选 | 内网 vLLM 部署的 embedding 模型（如 bge-m3 一类） | 与 LLM 同一套内网推理底座，省一套运维 |
| 维度 | **建表前定死** | `vector(N)` 的 N 写死在 DDL 里；改维度 = 全量重建索引 |
| 批量 | 支持 batch 与并发限流 | 工单关闭时批量索引，别把推理服务打满 |
| 失败处理 | 索引失败不阻断流程 | 与 §30 一致：只标记 `knowledge.indexed=false`，由 `INDEX_KNOWLEDGE` 任务重试 |

选型未定之前**不要建 `knowledge_entries` 的向量列**，否则后面要重建。

## 51.2 进向量的文本构造

``` text
故障摘要 + 确认根因 + 关键证据（各条 title）+ 人工处置动作 + 处理结果
```

要点：

- 只放**结论性字段**，不要把整条时间线塞进去（噪声会拉低检索质量）；
- 一期已有的字段可直接复用：`incidents.title`、`ai_summary`、
  `suspected_root_cause` / `confirmed_root_cause`、`incident_events.content`；
- 未人工确认根因的条目照样可以索引，但 `verified=false`、检索权重下调（见 §20）。

## 51.3 脱敏必须复用同一套掩码，不能只靠关键词过滤

§29 只列了凭据类关键词（Authorization/Cookie/Token/Password/Secret/AK/SK）。
但进向量的文本来自日志与工单，还会带上：

``` text
主机名 / IP / 集群名 / 项目名 / 镜像地址 / 任务名 / 用户目录 / 内网域名
```

一期已有资产掩码能力（`AIOPS_MASK_ASSETS` 对应的掩码函数，作用于 LLM 出网前）。
二期要求：

``` text
① 知识条目写入前 → 过一遍掩码
② 向量化之前     → 再过一遍掩码（防止只脱敏了展示、没脱敏向量）
③ 日志证据入 Evidence 前 → 先过滤凭据类关键词，再过掩码
④ 掩码策略必须可配置（掩码哪些字段），且改动后要重建向量（因为向量内容变了）
```

## 51.4 禁止入库 / 入向量的内容

``` text
密码、Token、私钥、Secret、AK/SK、完整连接串、完整的 kubeconfig
```

## 51.5 维度与模型的变更成本

``` text
改 embedding 模型或维度 → 现有向量全部失效 → 必须全量重建
重建期间检索降级为「结构化过滤 + 规则相似度」（§17 的前两档），不能返回错误结果
```

因此：**先定模型和维度，再建表**；并且把「已索引条数 / 待索引条数 / 上次重建时间」
做成指标（§28）。

## 51.6 与 §20 的呼应

embedding 只解决"找得到"，不解决"找得对不对"。因此：

``` text
verified=true 的知识条目（人工确认根因）才有高检索权重
未确认条目必须在卡片上标注「历史相似（未确认）」，不能让运维误当成结论
```

------------------------------------------------------------------------

# 52. 二期实施篇（v2.2 回填，可直接开工）

§1–§51 是设计与取舍。本章把设计落成"照着做"的清单：现状基线、每个可交付项的
表结构/状态机/迁移/验收/回滚，以及风险登记册。

## 52.1 现状基线（2026-09-11 实测，二期开工起点）

| 能力 | 状态 | 实测证据 |
| --- | --- | --- |
| 夜莺 webhook 接入 | ✅ 生产可用 | n9e v8.5.1，内置 Callback 媒介，返回 `status_code:200` |
| **入库延迟** | ✅ **101 ms**（avg） | 出锁前 11~15 s；实测 87 / 124 / 93 ms |
| 事件幂等 | ✅ | `hash + status + trigger_time`，alerts 部分唯一索引兜底 |
| 关联引擎 | ✅ | 同节点+40 / 拓扑+30 / 同集群+10 / 1min+20；实测 150 分合并 |
| 富化（K8s 侧） | ✅ kube-state-metrics + **K8s 只读 API** | SA `airs-system/aiops-readonly`；Pod 告警自动定位到节点 |
| 指标证据 | ✅ Prometheus 查询 API | `http://192.0.2.13:9090`（dx0 集群），单工单 4~14 查询 / 32~54 series |
| AI 诊断 | ✅ DeepSeek 真实模型 | `deepseek-v4-flash`，单次 5~16 s，带 Evidence + Confidence |
| 飞书双通道 | ✅ 工单通道 | 卡片只在「创建 / 根因变化 / 关闭」三时机推 |
| **副作用出锁** | ✅ **已落地**（原 M1 项） | 后台单线程 worker + `analysis_status` 状态机 + 启动补偿 |
| 巡检（sweeper） | ✅ | Alert 过期、恢复观察、保留策略、卡住分析重排 |
| 测试 | ✅ 42 用例 | `pytest -o addopts=""` |

**结论：原文档里的 M0（接真实依赖）已基本完成**，二期主战场是 **M1 可靠化 + 降噪 + 证据加厚**。

一期遗留的两个"临时方案"（二期必须替换）：

1. 后台队列是**进程内单线程 ThreadPoolExecutor**，不是可靠队列；
2. 单副本 + SQLite + `PROCESS_LOCK`。

## 52.2 M1 目标与不可妥协项

**目标**：把"能在生产跑"变成"生产规模下不会丢、不会乱、不会吵"。

不可妥协项（验收红线）：

``` text
A. 任何一条告警，只要夜莺投递成功，就必须落库（不能因为后台忙/重启而丢）
B. 任何一条工单，必须有终态（DONE 或 DEAD+原因），不能永久停在中间
C. 同一故障的重复触发与 HTTP 重试，只能产生一个工单、一张首卡
D. 后台失败不影响入库；入库延迟 P99 < 500 ms
E. 所有"外部调用"都可配超时、可降级、可观测
```

## 52.3 M1-1 可靠队列（DB Outbox）——替换进程内队列

### 52.3.1 为什么必须换

| 现在（进程内队列） | 问题 |
| --- | --- |
| 任务只在内存里 | 进程被 `kill -9` / OOM 时在途任务丢失（靠 `analysis_status` 补偿，但那是"事后补救"） |
| 无法多副本 | 两个副本各自一个队列，会重复分析、重复推卡 |
| 不可观测 | "队列里有多少活"看不见，只能靠 `SELECT count(*) WHERE analysis_status='PENDING'` 猜 |
| 无退避重试 | 失败只能等 sweeper 到点重排 |
| 无法限流 | 一场风暴 = 一次性排 N 个任务，Prometheus/LLM 会被瞬时打满 |

### 52.3.2 表结构（SQLite 现在 / PG 将来通用）

```sql
CREATE TABLE outbox_jobs (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    job_type       TEXT    NOT NULL,             -- incident_analysis / alert_notify / incident_card / log_evidence
    target_id      TEXT    NOT NULL,             -- incident_id 或 alert_id
    dedup_key      TEXT    NOT NULL,             -- 幂等键：job_type + target_id + variant
    payload        JSON,                         -- 任务参数（如卡片 kind）
    status         TEXT    NOT NULL DEFAULT 'PENDING',
                                                 -- PENDING / RUNNING / DONE / FAILED / DEAD
    attempts       INTEGER NOT NULL DEFAULT 0,
    max_attempts   INTEGER NOT NULL DEFAULT 5,
    available_at   DATETIME NOT NULL,            -- 退避重试：到点才可被领取
    lease_until    DATETIME,                     -- 租约：多副本下的互斥，过期可被抢占
    locked_by      TEXT,                         -- worker 实例标识（hostname:pid）
    last_error     TEXT,
    created_at     DATETIME NOT NULL,
    updated_at     DATETIME NOT NULL
);

-- 唯一约束：同一目标同一变体只允许一个"未完成"任务（防重复推卡/重复分析）
CREATE UNIQUE INDEX uq_outbox_active
    ON outbox_jobs (dedup_key)
    WHERE status IN ('PENDING', 'RUNNING');

-- 领取扫描
CREATE INDEX idx_outbox_claim ON outbox_jobs (status, available_at);
```

**状态机**：

```text
生产者（webhook 事务内）: INSERT (PENDING, available_at = now [+ delay])
                            └─ 与业务数据同事务 → 告警落库与任务入队原子，不会"有告警没任务"

消费者（worker）:  PENDING ──claim──> RUNNING ──成功──> DONE
                     ▲                   │
                     │                   ├─失败(可重试)──> PENDING（available_at = now + backoff）
                     │                   └─失败(超次)────> FAILED ──> DEAD（人工介入，写 last_error）
                     └── 租约过期（worker 挂了）自动可被重新 claim

backoff: 1min / 5min / 15min / 1h（attempts 指数退避；与 sweeper 周期解耦）
```

**关键语义**：

- `lease_until = now + 5min`：worker 领任务时打租约；处理中定期续租；
  进程死亡后租约自然过期，任务被别的 worker 领走（这就是"不丢"的机制）。
- `dedup_key`：`incident_analysis:{incident_id}:{kind}` —— 同一工单同一卡片种类
  同时只有一个未完成任务；根因变化再用 `incident_card:{id}:root_changed` 变体，互不冲突。
- `DONE` 保留 7 天供排查，之后由 sweeper 清理。

### 52.3.3 生产/消费接线

```text
生产（app/services/pipeline.py）：
  with PROCESS_LOCK:
      ...DB 短事务（raw/alert/incident）...
      outbox.enqueue(session, job_type=..., target_id=..., dedup_key=...)   ← 同一事务
  # 返回 200

消费（新增 app/worker_main.py，独立 systemd 单元 aiops-worker）：
  loop:
      job = outbox.claim(worker_id)        # UPDATE ... WHERE status='PENDING' AND available_at<=now
      if not job: sleep(1); continue
      try:  dispatch(job)                  # 调一期已有的 analyze_incident_now / notify_alert_now
      except: outbox.fail(job, error)      # 自动退避或置 DEAD
      else: outbox.complete(job)
```

**一期代码可直接复用**：`pipeline.analyze_incident_now(incident_id, kind)` 和
`pipeline.notify_alert_now(alert_id, kind)` 就是现成的 dispatcher，
换队列只是把「谁调用它」从进程内线程池换成 outbox 消费者。

### 52.3.4 迁移步骤（不停机）

```text
① 建表 + 索引（幂等 DDL，可随时执行）
② 上线 outbox 生产者：webhook 仍走进程内队列"影子写"——同时写 outbox（只写不消费）
③ 比对 24h：outbox 里的任务数 == 实际处理数（证明生产逻辑正确）
④ 切消费者：启动 aiops-worker（systemd），关闭进程内 ThreadPoolExecutor 提交
   （开关 AIOPS_OUTBOX_CONSUME=true，回滚 = 置 false + 重启，进程内队列逻辑保留 1 个版本）
⑤ 观察：outbox 深度、DONE/FAILED/DEAD 计数、P99 端到端延迟
⑥ 清理：确认稳定后删除旧代码路径
```

### 52.3.5 验收

| 编号 | 场景 | 期望 |
| --- | --- | --- |
| M1-1-a | 处理中 `kill -9` worker | 租约过期后任务被重新领取，工单最终 DONE，卡片不重复 |
| M1-1-b | LLM 连续失败 5 次 | job 置 DEAD + `last_error`，工单 `analysis_status=FAILED`，不刷群 |
| M1-1-c | 一次性灌 100 条告警 | 入库 P99 < 500ms；outbox 深度先升后降；无丢失（100 条对上） |
| M1-1-d | 起 2 个 worker | 无重复分析、无重复卡片（`dedup_key` + 租约生效） |
| M1-1-e | 队列满时重启 | 重启后所有 PENDING 继续处理，无人工干预 |

## 52.4 M1-2 聚合等待窗口（首卡不再只有 1 条告警）

**问题**：首卡在第一条告警到达时发出，"关联告警"栏只有 1 条，低估故障规模
（实测：8 条告警的风暴，首卡显示 1 条）。

**设计**：

```text
incidents.aggregate_until  DATETIME   -- 发首卡的最早时间；NULL 表示立即

严重级 → 窗口：P0/P1 = 0s（立即发，抢时间）
              P2    = 30s
              P3    = 60s

发卡前检查：now < aggregate_until → 不立即发，入 outbox（available_at = aggregate_until）
新告警挂入同一工单 → 刷新 aggregate_until = max(原值, now + 窗口)，但：
   · 最多延长 1 次
   · 且硬上限 90s（防饥饿：源源不断的新告警不能无限推迟首卡）
```

**与 §50 延迟预算的一致性**：P2 首个告警 → 30s 后发卡 + 诊断 5~16s ≈ 46s，
落在 §50 的 P2 ≤ 90s 之内。P0/P1 窗口为 0，仍是「立即」。

**验收**：8 条同类告警的风暴 → 1 张首卡且「关联告警」显示 8 条 / N 类；
停止灌入后 90s 内必发；P0 告警在 5s 内发。

## 52.5 M1-3 日志证据（用 K8s API 拉，不必等 Loki）

**新发现（一期收尾时实测）**：只读身份 `airs-system/aiops-readonly`
**有 `pods/log` 权限**。因此二期"日志证据"可以先不做日志平台集成。

```text
取值：按工单根因实体（pod → 自身；node → 该节点上 not_ready / 重启次数最多的 pod）
      最多 3 个容器，每容器 tailLines=200，2 分钟内的时间窗
调用：GET /api/v1/namespaces/{ns}/pods/{pod}/log?tailLines=200&timestamps=true
安全：只读、不 exec、不 follow（避免长连接与交互）
成本：单工单最多 3 次调用、总字节 ≤ 256KB、超时 5s、失败降级为 "logs_unavailable"
脱敏：进 prompt 前必须过 _MASK_ASSETS（日志里一定有 IP/主机名/业务标识）
灰度：AIOPS_LOG_EVIDENCE=false 默认关闭；先在 L1 观察两周一类告警
```

**验收**：Pod CrashLoopBackOff 的工单里，evidence 出现 `[log]` 条目且包含容器退出信息；
未配置日志证据时行为与现在完全一致（降级无副作用）。

## 52.6 M1-4 飞书卡片幂等

一期已做到"三时机推卡"，但仍缺数据库级幂等：

```sql
CREATE UNIQUE INDEX uq_feishu_message ON feishu_messages (channel, kind, target_id);
```

发送前 INSERT（唯一约束挡重复），发送后回填 `ok / message_id / error`。
这样即使 outbox 重试、多 worker 并发，同一工单的同一类卡片也只发一次。

## 52.7 M1-5 可观测与运维（网关自监控）

**新增指标**（一期已有 `aiops_*` 基础指标，补队列维度）：

```text
aiops_outbox_pending            gauge    待处理任务数
aiops_outbox_oldest_age_seconds gauge    最老待处理任务的等待时长（关键：卡住一眼可见）
aiops_outbox_failed_total       counter  按 job_type / reason
aiops_outbox_dead_total         counter  进入 DEAD 的任务
aiops_analysis_duration_seconds histogram 端到端（入队 → 卡片发出）
aiops_log_evidence_total        counter  按 ok / degraded / skipped
```

**建议的抓取与告警**（Prometheus 侧由运维自行配置，此处给出可直接使用的文本）：

```yaml
# 抓取
- job_name: aiops-gateway
  static_configs:
    - targets: ["192.0.2.115:8701"]

# 告警规则
- alert: AIOpsGatewayDown
  expr: up{job="aiops-gateway"} == 0
  for: 2m
- alert: AIOpsOutboxBacklog
  expr: aiops_outbox_pending > 50 or aiops_outbox_oldest_age_seconds > 600
  for: 10m
- alert: AIOpsAnalysisFailures
  expr: increase(aiops_outbox_dead_total[30m]) > 0
```

**备份与保留**（上生产必做）：`data/`（SQLite + raw JSONL）每日备份，
保留 14 天；备份脚本与网关同机、cron 触发；恢复演练每季度一次。

## 52.8 M2 前置：SQLite → PostgreSQL 与多副本

**目标**：去掉 `PROCESS_LOCK` 与单副本限制。

**迁移要点**（详见 §48）：

| 项 | 一期（SQLite） | 二期（PG） |
| --- | --- | --- |
| JSON 列 | `JSON`（TEXT） | `JSONB` |
| 时间列 | `UTCDateTime`（naive UTC 存取） | `timestamptz`（原生 aware） |
| 部分唯一索引 | `sqlite_where=` | `postgresql_where=`（语法一致，谓词照抄） |
| 自增主键 | `BigInteger.with_variant(Integer)` | `BIGSERIAL` / `IDENTITY` |
| 并发控制 | 进程内 `PROCESS_LOCK` + busy_timeout | 行级锁 `SELECT … FOR UPDATE` + 唯一索引 |
| 队列 | 单表 outbox + 租约 | 同左（PG 下更稳，可用 `SKIP LOCKED`） |

**停写窗口**：夜莺媒介重试 3 次 × 3s ≈ **10 秒**就放弃投递（见一期 §61.2）。
所以停写窗口必须 < 10s，或采用：
① 先导出 + 只把最后一次增量放进停写窗口；
② 切换期把夜莺指向备用接收端，切完回放；
③ 明确记录缺口（最后手段，需书面确认）。

**多副本前置条件清单**：

```text
□ 换 PG（或其它支持行级锁的库）
□ PROCESS_LOCK 全部替换为数据库级并发控制
□ outbox 租约机制上线（否则多副本会重复消费）
□ 幂等保证全部落到 DB 约束（不靠"先查后建"）
□ 定时任务（sweeper）加分布式锁或单实例选主
□ 配置外置（不能靠单机 .env）
```

## 52.9 M2：变更事件关联与 RAG 知识库

**变更事件**（§13）：接发布系统 / K8s `kubectl` 审计 / 配置中心，落
`change_events` 表，按实体 + 时间窗参与关联（"故障前 10 分钟有没有变更"）。
一手来源优先级：K8s 审计日志 > 发布系统 API > 人工登记。

**RAG**（§20/§51）：

```sql
CREATE TABLE knowledge_items (
    id            INTEGER PRIMARY KEY,
    incident_id   TEXT,                  -- 来源工单
    signature     TEXT NOT NULL,         -- 关联指纹（实体类型+告警类+关键标签）
    root_cause    TEXT,
    resolution    TEXT,
    verified      BOOLEAN DEFAULT FALSE, -- 人工确认过才有高权重
    embedding     BYTEA,                 -- 维度建表前定死！
    embedding_model TEXT,
    created_at    DATETIME
);
CREATE UNIQUE INDEX uq_evidence ON evidences (incident_id, source, content_hash);
```

`verified=true` 的条目高权重；未确认的必须在卡片上标注「历史相似（未确认）」。

## 52.10 里程碑与交付物

| 里程碑 | 内容 | 交付物 | 依赖 |
| --- | --- | --- | --- |
| **M0** ✅ 已完成 | 接真实 Prometheus / K8s 只读 / DeepSeek / 飞书 | 实测证据、42 测试 | — |
| **M1.1** | DB Outbox + 独立 worker | 表结构、worker 单元、5 条验收 | snapshot 无 |
| **M1.2** | 聚合窗口 | 首卡显示 N 条告警 | M1.1（延迟发卡用 outbox 的 available_at） |
| **M1.3** | 日志证据（K8s pods/log） | `[log]` 证据条目、脱敏、限额 | M1.1 |
| **M1.4** | 卡片幂等 | 唯一约束 + 重试安全 | M1.1 |
| **M1.5** | 自监控 + 备份 | 指标、告警规则、备份任务 | — |
| **M2** | PG + 多副本 | 迁移演练报告、切换脚本 | PG 实例就位 |
| **M2+** | 变更事件、RAG | 新表、检索链路 | embedding 模型定稿 |

## 52.11 风险登记册

| 风险 | 触发条件 | 影响 | 对策 |
| --- | --- | --- | --- |
| 迁移时丢告警 | 停写窗口 > 10s | 不可恢复的告警缺口 | 见 §52.8 三方案；迁移前演练 |
| 队列积压 | 风暴 + LLM 慢 | 卡片延迟到失去意义 | outbox 深度/最老时长告警；P0/P1 优先队列 |
| AI 幻觉 | 证据不足时给高置信度 | 运维被误导 | 强制 Evidence + Confidence；无证据 ≤0.3；卡片标注来源 |
| 资产信息出网 | 掩码失效/新字段漏掩 | 合规风险 | 出网前统一过 `_MASK_ASSETS`；新增字段必须回归用例 |
| 日志证据带业务数据 | 日志里有用户数据 | 合规风险 | 默认关闭 + 灰度 + 强脱敏 + 限额 |
| 单点 | 网关所在机挂掉 | 全部告警丢失（夜莺 10s 后放弃） | 双机 + VIP 或夜莺多媒介；至少做到"挂掉有告警" |
| 模型供应商变更 | API 不可用/涨价 | 诊断降级 | 保留 rule-stub 降级；模型可切换（配置项） |

## 52.12 待拍板事项（更新）

| # | 事项 | 状态 |
| --- | --- | --- |
| 1 | 资产信息能否出网（决定用公网模型还是内网 vLLM） | ✅ 已定：用 DeepSeek，出网前脱敏 |
| 2 | PG 实例从哪来、谁维护 | ⏳ 待定（M2 前置，不阻塞 M1） |
| 3 | 日志平台现状（Loki/ES） | ✅ 绕过：用 K8s `pods/log`（§52.5）；若要长周期检索再评估 |
| 4 | embedding 用内网哪个模型、多少维 | ⏳ 待定（M2+ 前置，建表前必须定死） |
| 5 | 聚合窗口取值（P2=30s / P3=60s 是否合适） | ⏳ 待确认（M1.2 开工前定） |
| 6 | 是否做"维护窗口抑制"（maintain 标签告警降级） | ⏳ 待定（实测有 master 节点在 maintain 阶段报 NotReady 的噪音） |
| 7 | 是否上线日志证据 | ⏳ 待定（建议先灰度一类告警） |
| 8 | 网关入口访问控制（安全组白名单 / URL token） | ⏳ 待定（生产建议至少做一项） |
| 9 | 飞书人工反馈（URL 按钮 + 平台 API） | ⏳ 待定（§49） |

## 52.13 二期总验收清单

```text
□ 100 条风暴：0 丢失、入库 P99 < 500ms、工单数符合关联预期
□ 处理中 kill -9 worker：任务自动续跑，卡片不重复
□ 同一告警重复投递（含 HTTP 重试）：只有 1 个工单、1 张首卡
□ P2 风暴首卡显示全部关联告警（≥8 条）
□ 每张卡片都能回答：根因是什么、证据在哪、置信度多少、下一步查什么
□ outbox 深度 / 最老任务时长 / DEAD 数可在 Prometheus 看到并有告警
□ data/ 每日备份，且做过一次恢复演练
□ K8s 侧只有 get/list/watch + pods/log，无任何写权限（SelfSubjectAccessReview 复核）
□ 出网 payload 抽样复核：无真实资产名/IP
□ 一页运维手册：怎么看待 PENDING/FAILED/DEAD、怎么手动重跑（/analyze）、怎么回滚
```
