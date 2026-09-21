# AIOps 一期：告警事件中心 / 关联引擎 / AI 辅助诊断

对应设计文档 `design-doc/aiops-phase1.md` 的 MVP 实现（设计 v1.1 + 实施篇 §46–§62）。

**当前状态（2026-09-14）：生产可用**（单副本，跑在 192.0.2.115:8701）——
真实夜莺 / Prometheus / K8s 只读 / DeepSeek / 飞书 全部接通，
webhook 入库 **约 100ms**（出锁前 11~15 秒），**53 个用例全绿**。

链路：

```
夜莺 Webhook → Event Gateway → Raw Event(本地 JSON) → Normalize → Enrich
   → Fingerprint → Alert 去重 → Correlation(Entity/Time/Topology/Causal/Score)
   → Incident → 立即 200 ─┐
                          └→ 后台 worker: Context Collector(Prometheus/K8s) → AI 诊断 → 飞书 → 人工闭环
```

## 1. 安装与启动

```bash
cd /home/lxy/aiops-alarm-self-healing

# 依赖
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -r requirements.txt

# 启动（单副本！SQLite + 进程内锁，不要开多 worker）
# ⚠️ 本机 8642/8643/8644 已被 docker 占用，故用 8701
.venv/bin/uvicorn app.main:app --host 0.0.0.0 --port 8701

# 测试
.venv/bin/python -m pytest -v
```

⚠️ 本机 `PYTHONHOME`/`PYTHONPATH` 被 Hermes runtime 污染，直接跑 venv 里的 python 会报
`No module named 'encodings'`，必须 `env -u PYTHONHOME -u PYTHONPATH`（run.sh 里已处理）。
Hermes 的 `execute_code` 内嵌 Python 同样被污染，不要用它跑本项目脚本。

### 1.1 systemd 托管（已在 192.0.2.115 部署）

```bash
systemctl status aiops-gateway     # active
systemctl restart aiops-gateway
journalctl -u aiops-gateway -f     # 结构化 JSON 日志
```

unit 位置 `/etc/systemd/system/aiops-gateway.service`，`Restart=always` 且已 enable（开机自启）。
监听 `0.0.0.0:8701`，数据目录 `/home/lxy/aiops-alarm-self-healing/data`。
**必须单副本**（SQLite + 进程内锁），不要改 `--workers`。

内网 pip 镜像（如需）：

```bash
uv pip install --python .venv/bin/python -r requirements.txt \
  -i https://pypi.tuna.tsinghua.edu.cn/simple
```

## 2. 夜莺侧接入

### 2.1 推荐：直接用夜莺的通知媒介（不需要自己写 body 之外的模板）

夜莺 v8.5.1（n9e-host-01）里 `request_type=http` 的媒介有一批是内置集成
（飞书/企微/钉钉/Slack/Telegram/Lark/SMS...），它们的 ident 决定了 body 形状、不能乱动。
**想往自己的 URL 发，要用的内置媒介是 `Callback`（自定义回调）**——它本身就是
`request_type=http`：URL 默认取 `{{$params.callback_url}}`、body 默认 `{{ jsonMarshal $events }}`。
照着它新建一个媒介（ident=callback）即可，不是"夜莺没有这个入口"。

也可以不用媒介，走这两条：

| 方式 | 位置 | 适用范围 |
| --- | --- | --- |
| 通知媒介 ident=callback | 告警管理 → 通知媒介 → 新建（参照内置 Callback） | 由通知规则决定 |
| 全局 Webhook | 告警管理 → Webhook（API `GET/PUT /api/n9e/webhooks`，PUT 需 admin；存 `config` 表 ckey=`webhook`） | 所有告警事件 |
| 告警规则回调 | 每条告警规则的「回调地址」（`alert_rule.callbacks`，空格分隔多个 URL） | 单条规则 |

三者都是 `POST` + `Content-Type: application/json`，body = **单条事件 JSON**
（夜莺 `AlertCurEvent`：`rule_name` / `severity`(int) / `target_ident` / `trigger_time` /
`tags_map` / `annotations` / `is_recovered` / `hash` ...），不需要写业务字段映射。

```
POST http://192.0.2.115:8701/api/v1/events/nightingale
```

#### 媒介里必须填 / 必须满足的四件事

1. **请求体不能为空**，填一行就够：
   ```
   {{ jsonMarshal $event }}
   ```
   - 必须用 `jsonMarshal` 函数（返回 `template.HTML`，不转义）；直接写 `{{ $event }}`
     是 Go 结构体打印、不是 JSON，写 `"{{$event.RuleName}}"` 会被转义成 `&#34;` 让 JSON 非法。
   - 引擎预置变量：`$event` `$events` `$tpl` `$sendto` `$sendtos` `$params`，不用自己定义。
   - 用 `$event`（单对象）比 `$events`（数组）稳；网关两者都兼容。
2. **请求头建议加 `Content-Type: application/json`**（不加也能收，网关是手工解析 body 的）。
3. **必须在通知规则里被引用**（通知规则 → 通知配置 → 渠道选该媒介）。媒介建了但没挂到
   任何通知规则 = 永远不会发。
4. **必须绑定一个消息模板**（通知配置里的模板 ID）。源码里 `message_template` 查不到会直接
   `continue`，只打一条 warning、什么都不发。用内置的 `Callback`(id=3) 就行——
   模板内容对 HTTP 媒介无影响（我们的 body 只用 `$event`）。

#### 网关侧的容错

入口不声明 pydantic Body 模型，手工 `json.loads`，因此兼容：单对象 `{...}`、数组 `[...]`、
以及 `{"events":[...]}` 原生批量；并且**不要求 `Content-Type`**（夜莺 HTTP 媒介默认不带）。
请求体为空时返回 422 并明确提示"检查媒介的请求体模板"。

### 2.3 卡片版式

工单通道只有三种卡片，靠表头颜色和图标区分，不会看混：

| kind | 表头 | 触发时机 |
| --- | --- | --- |
| `incident_created` | 🚨 红/橙（按级别） | 工单创建 |
| `incident_root_changed` | 🔄 红/橙 | 根因被提升（如症状先到、根因后到） |
| `incident_resolved` | ✅ 绿 | 工单关闭（自动恢复或人工关闭） |

版式规则：

- 标题只放「级别 + 故障对象 + 告警名」，状态/计数放双列字段区，不堆成一大段文字
- 分段：概览字段 → 关联依据 → 初步判断/处理结果 → 关键证据 → 建议排查 → 关联告警 → 时间线
- **关联告警按告警名聚合**（30 个 Pod 只占 1~2 行，不会刷 30 行）
- **关联依据**一行说清「为什么这些告警算同一个故障」（强制关联/因果规则 CR-xxx/同一节点…）
- 时间线只保留关键事件（工单创建、聚合告警 ×N、根因更新、关闭…），
  例行事件（每次刷新的上下文采集/AI 分析/推送记录）不进时间线
- 时间统一按 `AIOPS_DISPLAY_TZ`（默认 Asia/Shanghai）显示，格式 `MM-DD HH:MM:SS`

⚠️ 已知取舍：工单创建卡片是在**第一条告警**到达时发的，所以首卡里的「关联告警」通常显示 1 条；
后续告警挂进同一工单时不再刷群（避免风暴刷屏），完整数量要靠「工单恢复」卡片或 API 查看。

事件通道（Alert 粒度）另有两张简单卡片：🔔 新告警 / ✅ 告警恢复。

网关已适配这套原生格式：

| 夜莺字段 | 网关映射 |
| --- | --- |
| `rule_name` | `alertname` |
| `tags_map` | `labels`（instance / cluster / node 等） |
| `target_ident` | `entity.id` |
| `is_recovered` | `firing` / `resolved` |
| `severity` 1/2/3 | `P1` / `P2` / `P3` |
| `trigger_time`（epoch 秒） | `occurred_at` |
| `hash` | 幂等 + fingerprint 参考 |

⚠️ 幂等键用 `hash + status + trigger_time`。夜莺的 `hash` 是 `rule_id+vector_key`，
对同一条告警长期不变——只拿 hash 当幂等键，重复触发会被误判成 HTTP 重试丢掉，
`occurrence_count` 永远是 1。

⚠️ 不要开 Webhook 的「批量(batch)」模式：批量 body 是数组包装，一期未适配。

⚠️ 恢复事件靠 `is_recovered`，别在模板里写死 status=firing，否则 Incident 永远不会自动关闭。

### 2.2 备选：夜莺侧发格式化 payload

如果后面想让夜莺直接发标准格式（`schema_version` 必须带，便于模板升级时区分新旧），
用「通知媒介 → 新建 → 类型 HTTP」（只能经 API/SQL 建，UI 不暴露），
body 模板里**必须用 `jsonMarshal`**——body 走的是 `html/template`，
直接写 `{{$event.RuleName}}` 里的引号会被转义成 `&#34;` 导致 JSON 非法：

```
{{$event := .event}}{
  "schema_version": "1",
  "source": "nightingale",
  "alertname": {{ jsonMarshal $event.RuleName }},
  "status": {{ if $event.IsRecovered }}"resolved"{{ else }}"firing"{{ end }},
  "severity": {{ $event.Severity }},
  "occurred_at": {{ $event.FirstTriggerTime }},
  "entity": {"type": {{ jsonMarshal $event.TargetIdent }}, "id": {{ jsonMarshal $event.TargetIdent }}},
  "labels": {{ jsonMarshal $event.TagsMap }},
  "value": {{ jsonMarshal $event.TriggerValue }},
  "annotations": {"summary": {{ jsonMarshal $event.RuleNote }}}
}
```

`$event` / `$tpl` / `$events` / `$sendto` / `$sendtos` / `$params` 是引擎预置变量，
不用自己定义。可用函数见 `pkg/tplx/tplx.go`（jsonMarshal / toUpper / stripPort / toTime 等）。

```json
{
  "schema_version": "1",
  "source": "nightingale",
  "event_id": "夜莺侧的告警哈希（用于幂等，强烈建议带）",
  "alertname": "NodeNotReady",
  "status": "firing",
  "severity": "P1",
  "occurred_at": "2026-09-11T10:00:12+08:00",
  "entity": {"type": "node", "id": "gpu-node-021", "ip": "192.0.2.21", "node": "gpu-node-021"},
  "scope": {"cluster": "h800-prod", "region": "beijing", "zone": "zone-a", "project": "training-prod", "node": "gpu-node-021"},
  "labels": {"instance": "192.0.2.21:9100", "pod": "", "namespace": ""},
  "annotations": {"summary": "节点 NotReady"},
  "value": "1"
}
```

- `status` 只接受 firing / resolved（也兼容 alerting / ok / fixed 等常见写法）。
- Pod 类告警请带上 `entity.node` 或 `scope.node`，否则没有 K8s 接入时无法与节点告警关联。
- 缺 `alertname` 或缺 `status` → 直接 422 拒收，不会静默写脏数据。
- 夜莺原生 payload（`rule_name` + `tags`，或 `events` 数组）也能吃，但建议尽快切到格式化。

## 3. 环境变量

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `AIOPS_DATA_DIR` | `./data` | 数据目录（含 raw_events/、outbox/、aiops.db） |
| `AIOPS_DB_PATH` | `./data/aiops.db` | SQLite 文件 |
| `AIOPS_ALERT_STALE_SECONDS` | `900` | 无事件多久自动判定 Alert 恢复（夜莺不发 resolve 的兜底） |
| `AIOPS_RECOVERY_OBSERVE_SECONDS` | `300` | Incident 全量恢复后的观察期 |
| `AIOPS_SWEEP_INTERVAL_SECONDS` | `30` | 后台巡检间隔 |
| `AIOPS_CONTEXT_LOOKBACK_SECONDS` | `600` | 上下文指标回溯窗口 |
| `AIOPS_CONTEXT_STEP_SECONDS` | `15` | 回溯采样步长 |
| `AIOPS_CONTEXT_MAX_QUERIES` | `60` | 单次采集的查询上限（超了降级 PARTIAL，不死等） |
| `AIOPS_CONTEXT_DEADLINE_SECONDS` | `30` | 单次采集总预算 |
| `AIOPS_RAW_RETENTION_DAYS` | `30` | raw_events 归档与索引保留天数 |
| `AIOPS_EVENT_RETENTION_DAYS` | `90` | 时间线/模型调用/推送记录保留天数 |
| `AIOPS_MACHINE_INFO_TTL_SECONDS` | `300` | machine_info（轻量 CMDB）缓存 TTL |
| `AIOPS_K8S_TIMEOUT` | `8` | K8s 只读请求超时 |
| `AIOPS_LLM_TIMEOUT` / `AIOPS_LLM_MAX_RETRIES` | `60` / `2` | 模型调用超时与重试 |
| `AIOPS_FEISHU_TIMEOUT` | `8` | 飞书推送超时 |
| `AIOPS_LOG_LEVEL` | `INFO` | 日志级别 |
| `AIOPS_DEBUG_SKIP_DEDUP` | `false` | **调试开关**：跳过事件幂等、Alert 指纹合并与关联，便于联调反复触发整条链路。⚠️ 生产必须关闭，否则夜莺 HTTP 重试会重复建工单；状态在 `/readyz` 可见 |
| `AIOPS_INLINE_ANALYSIS` | `false` | 副作用出锁回退开关：`true` 时采集/诊断/推卡在 webhook 内同步执行（行为同改动前，用于对照/回滚） |
| `AIOPS_ANALYSIS_STALE_SECONDS` | `300` | 分析卡住多久算超时（sweeper 兜底重排） |
| `AIOPS_ANALYSIS_MAX_ATTEMPTS` | `3` | 单工单分析最大重试次数 |
| `AIOPS_PROMETHEUS_URL` | 空 | 不配则富化降级为 PARTIAL，不报错 |
| `AIOPS_K8S_API_URL` / `AIOPS_K8S_TOKEN` / `AIOPS_K8S_CA_FILE` | 空 | 只读账号，未配则跳过 |
| `DEEPSEEK_API_KEY` | 空 | 不配则走规则兜底（`engine=rule-stub`，会明确标注） |
| `DEEPSEEK_MODEL` | `deepseek-chat` | |
| `AIOPS_MASK_ASSETS` | `true` | 走公网模型时脱敏 IP/主机名后再发 |
| `AIOPS_FEISHU_EVENT_WEBHOOK` | 空 | 事件通道（Alert 创建/恢复）。空=干跑落盘 |
| `AIOPS_FEISHU_INCIDENT_WEBHOOK` | 空 | 工单通道（Incident 创建/根因变化/关闭）。空=干跑落盘 |

三个飞书通道的关系：夜莺原始告警通道不归本服务管，保持现状作为对照；
本服务只负责「事件通道」和「工单通道」，且**绝不按每条 raw event 推送**。

关联阈值只有一个来源：`rules/correlation.yaml` 的 `threshold`（改环境变量无效，`/api/v1/rules/reload` 热加载）。

## 4. API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/v1/events/nightingale` | 夜莺 Webhook |
| GET | `/api/v1/incidents` | 工单列表（`?status=OPEN&cluster=`） |
| GET | `/api/v1/incidents/{id}` | 详情：告警、关联原因、时间线、上下文 |
| POST | `/api/v1/incidents/{id}/ack` | 认领 |
| POST | `/api/v1/incidents/{id}/resolve` | 关闭 |
| POST | `/api/v1/incidents/{id}/alerts/{alert_id}/detach` | **人工纠错：摘掉误关联的告警** |
| POST | `/api/v1/incidents/{id}/analyze` | 重跑上下文 + AI 诊断 |
| GET | `/api/v1/alerts` | Alert 列表 |
| GET | `/api/v1/raw-events` / `/{event_id}` | 原始事件 + 归档校验 |
| POST/GET | `/api/v1/topology/relations` | 拓扑关系维护 |
| POST | `/api/v1/sweep` | 手动触发巡检 |
| GET | `/api/v1/stats` | 告警压缩率等 |
| GET | `/healthz` `/readyz` `/metrics` | 健康与自身指标 |

## 5. 相对设计文档的实现修正（重要）

1. **关联评分表不自洽 → 增加「强关联信号」门槛**
   文档 §16：`同节点(40) + 同集群(10) + 1分钟内(20) = 70`，恰好达到阈值，
   会让 §39 Case 5 的无关告警（DiskUsageHigh + GPUHighTemperature）被错误合并。
   因此在 `rules/correlation.yaml` 加 `require_strong_link: true`：
   必须命中 `exact_entity` / `topology` / `causal_rule` / `force_link` 之一才允许成链，
   same_node 等只做加法。NodeNotReady 链路靠 force_link，Pod↔Node 靠因果规则，都不受影响。

2. **不写死 `relation=SYMPTOM` → 支持根因提升**
   症状先到、根因后到时（PodNotReady → NodeNotReady），新告警会被提升为 ROOT，
   旧 root 降为 SYMPTOM，并留 `INCIDENT_ROOT_CHANGED` 时间线。

3. **新增 Incident 自动合并**（文档缺失）
   症状各自建了工单、根因到达后必须收敛：`reconcile_incidents` 在每次关联后核对，
   命中 force_link / 因果 / 拓扑≤2 / 同节点根因即合并，但 `never_link` 优先。

4. **候选工单窗口按 `incident.last_seen`**，不是 `first_seen`（文档伪代码用错字段）。

5. **Alert 过期兜底**（文档缺失）
   夜莺不发 resolve 时，`last_seen` 超过 900s 的 FIRING 自动置 RESOLVED，
   否则老 Alert 永远 OPEN，新故障会被合并进几天前的 Alert。

6. **并发与幂等**（文档缺失）
   - raw event 幂等：同一份 payload 重发只落一条（`event_id` 唯一）。
   - Alert 去重硬保证：`alerts` 表部分唯一索引（同一 fingerprint 只能一条 FIRING），
     应用层先查后建只是快路径，冲突时转 update。
   - 单副本约束：SQLite + 进程内可重入锁。多副本必须换 PostgreSQL + 分布式锁。

7. **Context Collector 用 range query**
   NodeNotReady 时 node_exporter 已掉线，即时查询只有 0。
   改为回溯 `first_seen - 600s ~ last_seen + 60s`，取回后压成 min/max/last 摘要。

## 6. 目录

```
app/
  main.py                  FastAPI 入口 + 巡检后台任务 + 启动补偿（requeue_pending）
  config.py  metrics.py  logging_setup.py
  netutil.py               共享小工具：IP 判定 + PromQL 字符串转义
  api/routes.py            HTTP 接口
  db/{base,session,models}.py   SQLite / SQLAlchemy 2.x 模型
                             · session.py 另有 WorkerSessionLocal(AUTOCOMMIT) 与轻量列迁移
  db/queries.py            共享查询（工单/告警/时间线/计数），避免同一 JOIN 多处重写
  timeutil.py              UTC 内部口径 + 本地时区展示
  models/schemas.py        格式化 payload 契约 + 内部统一模型
  services/
    locks.py               PROCESS_LOCK（独立模块，避免 pipeline ↔ worker 循环依赖）
    worker.py              副作用出锁：后台单线程执行器（采集+AI+推卡）、启动补偿、队列观测
    raw_store.py           raw event 本地 JSONL 归档（按天、fsync、字节偏移指针）
    normalizer.py          校验 + 标准化 + 幂等键
    fingerprint.py         Alert 指纹
    enricher.py            kube-state-metrics / machine_info / K8s API 富化，失败降级不丢告警
    alert_service.py       Alert upsert / 恢复 / 过期
    incident_service.py    Incident 创建/关联/根因提升/合并/时间线/恢复/保留清理
    context_collector.py   上下文采集（Prometheus range + K8s API + 拓扑）
    pipeline.py            DB 短事务流水线 + 后台分析入口（analyze_incident_now）
    sweeper.py             巡检：Alert 过期、恢复验证、保留策略、卡住分析重排
  correlation/
    engine.py              Never → Force → Causal → Topology → Score
    rules.py  topology.py
  integrations/
    prometheus.py  kubernetes.py  deepseek.py  feishu.py
rules/
  correlation.yaml         权重/阈值/强信号/force_link/never_link/根因优先级/指纹维度
  causal_rules.yaml        确定性故障链
  context_queries.yaml     上下文查询模板（窗口/step 在 config）
tests/
  conftest.py              关闭所有外部依赖，验证降级路径（异步路径默认同步执行）
  test_phase1.py           §39 Case1-5 + 幂等 + 关联 + 合并 + 恢复 + 人工纠错 + 推送噪音
                           + 脱敏还原 + 出锁/补偿/幂等 + 事务重试（53 个用例）
tools/
  backfill_raw_events.py   补录：把归档里「有原文、没入库」的事件重投回网关（默认只报告）
docs/
  current-flow.html        给非技术同学看的全流程讲解页（离线可开，零外部依赖）
design-doc/
  aiops-phase1.md          一期设计 v1.1 + 实施篇
  aiops-phase2.md          二期详设 v2.2（含 §52 二期实施篇）
  aiops-phase2-review.md   二期文档评审意见（带 file:line 证据）
data/                      运行时生成：raw_events/*.jsonl、outbox/*.jsonl、aiops.db
k8s-readonly/              K8s 只读接入凭据（600，勿入库/勿外发）
```

## 7. 已验证 / 待办

### 7.1 已验证（2026-09-11，真实依赖全开）

**测试**：`53 个用例全绿`（`env -u PYTHONHOME -u PYTHONPATH .venv/bin/python -m pytest -o addopts="" -q`）

**端到端（真实 Nightingale + Prometheus + K8s 只读 + DeepSeek，飞书按需）**：

| 场景 | 结果 |
| --- | --- |
| 告警入库耗时 | **avg 57~101 ms**（出锁前 11~15 秒）→ 夜莺不再 `context deadline exceeded` |
| 后台分析 | `analysis_status: PENDING → RUNNING → DONE`，带 attempts 计数 |
| Pod 告警自动定位 | PodNotReady 经 K8s API 解析出所属节点 → 与节点告警合成 1 个工单（关联分 150） |
| 指标证据 | 单工单 4~14 条查询 / 32~54 条 series，含真实曲线（io_time ≈1.0、重传率、丢包率…） |
| K8s 证据 | 节点 conditions / taints / kubelet 版本 + 该节点 Pod 列表与 not_ready 数 |
| AI 诊断 | `deepseek-v4-flash`，5~16 秒，强制 Evidence + Confidence（证据不足自动 ≤0.3） |
| 节点故障风暴 8 次通知 | raw_events 8 → alerts 8 → **incidents 1**，压缩率 0.875 |
| 同一告警重复投递 | `duplicated=1`（幂等），不重复建单；卡片有发送幂等，不会重复推 |
| 恢复（is_recovered） | Alert 全 RESOLVED → Incident 进 RECOVERING → 观察期满后关闭 |
| 飞书卡片 | 只在「创建 / 根因变化 / 关闭」三时机推，实测 30 条告警 → 1 张卡 |
| K8s 只读边界 | `SelfSubjectAccessReview` 实测：读类 true，`get secrets` / 全部写操作 **false** |
| 出网脱敏 | 真实主机名/IP 出网前换成占位符，返回后还原；指标名/组件名/规则名保留 |

**逐轮演练结论（同一批测试）**：模型诊断置信度从 0.3 → 0.45 → 0.5 → 0.6 上升，
原因是证据逐步补齐（指标 → K8s 事实 → 指标名可见）。现在卡的判断能做到：
引用真实数据、列出差分对比、明确说"无法区分"，不编造。

### 7.2 本轮代码评审修复（simplify-code：3 个只读审查员并行，35 条发现）

**真 bug（会造成错误行为）**

| 问题 | 修复 |
| --- | --- |
| 后台任务等 webhook 提交超时后**直接丢弃任务**（docstring 说会继续跑） | 超时也继续执行；`analyze_incident_now` 找不到行会安全返回 |
| 后台用 DB 里的旧 kind 触发分析 → **根因更新被推成「新工单」卡片** | 以请求 kind 为准，DB kind 只用于「是否已按同 kind 跑完」判断 |
| 分析运行期间的新请求（人工 `/analyze`、根因提升）被**静默丢弃**（接口还返回 202） | 合并成「最新待跑 kind」，跑完自动补一次；新增回归用例 |
| `_EXECUTOR.submit` 在 try 之外 → 提交失败时 `_INFLIGHT` 泄漏；executor 关闭后进程内无法再提交 | executor 惰性重建 + 提交失败回滚去重状态 |
| `notify_alert_now` 用事务型会话跨飞书网络调用 → 与 webhook 抢锁报 `database is locked` | 改用 `WorkerSessionLocal`（AUTOCOMMIT），与 `analyze_incident_now` 口径统一 |
| **sweeper 持 `PROCESS_LOCK` 打 Prometheus/飞书**（每 30s 一次）→ 会把 webhook 堵住，正是刚修掉的超时根因 | 锁只包 DB 读-判-写；网络阶段在锁外；定时与手动巡检加单飞锁 |
| 瞬时 `database is locked` → `analysis_status=FAILED`，而 FAILED 不自动重试 → **诊断永久缺失** | 瞬时错误保持 PENDING，交给 sweeper 续跑；仅业务性失败才 FAILED |
| 卡片已发但状态未落 DONE 时重启 → 启动补偿**重复推卡** | 发送前查 DB 幂等（同 kind 已成功推过就跳过） |
| 脱敏把 `INC-20260911-001` / `ALT-<hex>-<ts>` 当主机名掩成 `<host-N>` | 业务标识加入跳过表；`id` 从资产键移除；新增回归用例 |
| PromQL 字符串只转义反斜杠、不转义双引号 → 标签含 `"` 时查询语法错误 | `netutil.promql_string` 统一处理 `\` 与 `"`，context_collector / sweeper 共用 |

**复用收敛**

- `sweeper` 硬编码 `("PENDING","RUNNING")` → 用 `worker.RESUMABLE_STATUSES` / `DEFAULT_KIND`
- `/analyze` 与 `schedule_analysis` 重复的状态机写入 → 抽 `pipeline.schedule_analysis`
- `notify_alert_now` 与 `_notify_alert` 重复的分派逻辑 → 抽 `_dispatch_alert_notify`
- 三份 IPv4 正则 + 两份 PromQL 转义 → 收敛到 `app/netutil.py`
- `_instance_regex` 里重复实现 Prometheus 标签提取 → 复用富化结果与采集缓存

**效率（都在热路径上）**

- enricher 在 webhook 里对节点告警多打 2 次 K8s API（pods/events，后台会重取）→ 移出请求路径
- `_instance_regex` 对同一节点重复查 `kube_node_info`（N+1）→ 富化结果优先 + 本次采集缓存
- 脱敏在裁剪之前做（对大 context 递归 + 3 正则）→ 先裁剪再脱敏；`_clip` 改批量裁剪（原 O(n²)）
- Prometheus 客户端每次查询新建连接 → 改持久 `httpx.Client`
- sweeper 重排/启动补偿拉整行（含大 JSON 的 context）→ 只取需要的列
- 后台轮询等提交每次新建 session（最坏 ~100 次）→ 单 session 复用 + 指数退避

**结构性遗留（记录，未本次处理）**

- analysis 状态、kind、EventResult.status 等字符串字面量散落多处，建议提为常量/枚举
- `session._ensure_columns` 的 DDL 与 `models.py` 是两处真相，建议加一致性测试或引入迁移工具
- `correlation/engine.py` 对候选工单逐条查 `attached_alerts`（N+1）、sweeper 的 `status_counts` 全表 COUNT，
  工单/告警量上来后需要批量化与降频

### 7.3 生产日志排查修复（2026-09-14）

线上跑了 3 天后翻日志，抓到两个真问题，都已修：

**① 告警被丢（`database is locked` → 事件未入库、夜莺不重投）**

- 现场：`2026-09-14 14:01:50` 一条告警 4 次重试全败（重试间隔 50/100/150ms，与代码退避完全对上），
  `raw_events` 里查不到 → 没有 Alert / 工单 / 诊断，静默丢失。现役期间 32 条收 1 条丢（≈3%）。
- 根因：SQLite 在「事务里先读过、之后才写」时，若期间别的连接提交过写，会**立刻**报
  `database is locked`（SQLITE_BUSY_SNAPSHOT，`busy_timeout=30s` 根本不参与）；
  而旧代码的**重试放在 savepoint 内层** —— savepoint 回滚不换快照，所以重试必然继续失败。
- 修复：
  1. **每条事件一个独立事务 + 失败回滚重开再重试**（savepoint 内层重试是无效的）；
  2. 事务以 **`BEGIN IMMEDIATE`** 开始：冲突变成「排队等待」（busy_timeout 生效），而不是写时炸；
  3. 同一事件重试期间**归档只写一次**（旧行为：一次失败在 JSONL 里留 4 行重复）；
  4. 新增 `aiops_events_locked_retry_total` 指标 + 失败事件带 `event_id` 落日志（丢了能查、能补）。

**② 飞书事件通道一直"干跑"，指标还会骗人**

- `/readyz` 显示 `feishu.event=dry_run`（env 里 `AIOPS_FEISHU_EVENT_WEBHOOK` 是注释状态）
  → Alert 粒度的原始告警从未真正发出，群里只有工单卡片。
- 更坑的是 `aiops_feishu_sent_total{channel="event"}=31` —— 干跑也计入了 sent，监控会得出"正常"的错误结论。
- 修复：干跑单独计 `aiops_feishu_dry_run_total`，`sent_total` 只在真发时记。

**补录工具（把丢掉的告警捞回来）**

归档是「先写文件再入库」，写失败不回滚 → 原文通常还在，可以重放：

```bash
cd /home/lxy/aiops-alarm-self-healing
# 只报告（默认，不动数据）
env -u PYTHONHOME -u PYTHONPATH .venv/bin/python tools/backfill_raw_events.py
# 真的补投（走网关入口，服务端幂等，重复投递安全）
env -u PYTHONHOME -u PYTHONPATH .venv/bin/python tools/backfill_raw_events.py --apply
```

实测输出：归档 43 个事件、库里 41 个、**缺失 2 个**（09-11 的 NodeDown、09-14 的 master-not-ready）。

### 7.4 「工单恢复」曾是假恢复（2026-09-14 修正）

**现象**：一条告警卡片之后 20 分钟，群里来一张「✅ 工单恢复」——但被"恢复"的节点其实一直没恢复。

**证据链**
```
① 告警 15 分钟不再来 → 被当成"已结束"（alerts.resolution_reason=stale）
   全库分布：stale 结束 29 条，夜莺真恢复信号(resolved) 只有 5 条
② 进 5 分钟观察期 → 到期后用 Prometheus 查 up{instance=~"<告警里的 ip>.*"} 判恢复
③ 而那个 ip 是 198.51.100.210 —— kube-state-metrics 的地址（job=kube-state-metrics,
   instance=198.51.100.210:8080），4 台不同 master 的告警共用它，且它永远 up=1
实测（生产指标源）：
   up{instance=~"198.51.100.210.*"}                        → 198.51.100.210:8080=1   ← 旧判据据此判"已恢复"
   kube_node_status_condition{node=...,condition=Ready,status="true"} → 0        ← 真相：节点未就绪
   K8s API 实测：4 台 master 至今 Ready=Unknown（kubelet 停止上报，带 unreachable 污点，
   Ready 最近一次变化是 08-23 / 09-01），标签显示 machine/baai/ac/cn/phase=maintain
```
**修复（4 处）**
1. **实体 IP 纠正**：`normalizer` 不再把集群级采集端（job=kube-state-metrics）的 instance 当实体 IP；
   `enricher` 用 `kube_node_info.internal_ip` 权威覆盖，并在 enrichment 里留 `ip_corrected_from` 痕迹。
2. **恢复判据「同类型 + 精确」**：节点类先查 K8s 权威判据
   `kube_node_status_condition{node,condition="Ready",status="true"}`；`up` 兜底匹配精确到 `^<ip>:`；
   **查不到任何序列 = unverified（无法确认），绝不再当成"已恢复"**（旧代码把空结果判成 recovered，是假恢复的直接成因）。
3. **stale ≠ 恢复**：只有夜莺显式恢复（is_recovered=true → resolution_reason=resolved）才进恢复观察期；
   单纯 stale 过期的工单**保持 OPEN**、记 `RECOVERY_UNCONFIRMED`、不推恢复卡（等恢复信号或人工确认）。
4. **恢复卡必须写「恢复依据」**：卡片新增一行，写清用了什么判据（K8s Ready / Prometheus up / 仅告警状态）。

**遗留（需人工确认）**：库里已有若干个按旧逻辑误判关闭的工单（INC-20260914-013/017/018/022/026 等），
对应的 4 台 master 实际仍未就绪 —— 这些工单的状态需要人工核实后处理（重新打开会再推卡，所以没自动做）。

### 7.5 下一阶段（按优先级）

**A. 生产运维三件（建议尽快）**

1. **入口访问控制**：8701 监听 0.0.0.0，任何可达机器都能伪造告警 → 安全组白名单（192.0.2.13 / n9e-host-01）
   或 URL token 校验（二选一，后者我可以实现）。
2. **数据备份**：`data/`（SQLite + raw JSONL）每日备份、保留 14 天 + 季度恢复演练。
3. **网关自监控**：抓 `192.0.2.115:8701/metrics`，告警规则见二期文档 §52.7
   （进程存活 / outbox 积压 / 分析 DEAD）。

**B. 二期 M1（可靠化，详见 `design-doc/aiops-phase2.md` §52.3–§52.7）**

4. **DB Outbox 替换进程内队列**：任务落表 + 租约 + 退避重试 + DEAD 死信，
   进程被 kill 也不丢；同时把「最快」的准备做给多副本。
5. **聚合等待窗口**：P0/P1 立即、P2 30s、P3 60s → 首卡显示完整告警数量（现在是 1 条）。
6. **日志证据**：只读身份有 `pods/log` 权限，可直接从 K8s API 取容器日志当证据，不必等 Loki。

**C. 二期 M2**

7. 换 PostgreSQL + 去 `PROCESS_LOCK` → 支持多副本；停写窗口必须 < 10s（夜莺重试窗口）。
8. 变更事件关联（故障前有没有发版/改配置）与历史工单 RAG。

**D. 已知噪音与口径（夜莺侧）**

9. Pod 类告警把 `node` 打进 tags（有 K8s 接入后不再是唯一依据，但更稳）。
10. 通知规则覆盖 S2/S3；`maintain` 阶段节点的告警考虑降级/标注（实测有 master 节点维护期报 NotReady）。


### 7.6 运维口径（2026-09-15 起）

- **每天最多一张卡**：同一工单当天只推第一张卡片（恢复卡不受限——终态、每单一次、是好消息）。
  未恢复的故障由**每日 09:00 汇总卡**重新提醒一次（默认只在有未恢复工单时发；
  要每天固定报到用 `AIOPS_DAILY_DIGEST_ALWAYS=true`）。
- **自动探测恢复**（运维处理完不必等人告诉我们）：告警静默 ≥5 分钟后，每 5 分钟主动查权威判据
  （节点类查 `kube_node_status_condition{condition="Ready"}`，通用查 `up{instance=~"^IP:"}`）。
  真恢复 → 关单 + 群里通报恢复卡；探到未恢复 / 拿不到判据 → 保持 OPEN，等次日汇总。
  **没配指标源就不探测**（探测不出 ≠ 已恢复）。
- **Web 工单页**：`http://192.0.2.115:8701/ui/incidents`（只读、无鉴权；点标题行就地展开详情，
  `?scope=all` 看最近全部）。飞书卡片带 `[查看工单详情]` 按钮直达单工单页。
  对外地址用 `AIOPS_PUBLIC_BASE_URL` 覆盖（默认 `http://192.0.2.115:8701`）。
- **同故障收敛**：同规则+同资源的告警在 `AIOPS_INCIDENT_RECURRENCE_HOURS`(24h) 内复发，会挂回原工单
  （必要时复开），不再每天新建一张。
- **相关环境变量**：`AIOPS_ALERT_STALE_SECONDS`(7200，必须 > 告警源重发周期)
  `AIOPS_INCIDENT_RECURRENCE_HOURS`(24) `AIOPS_DAILY_CARD_CAP`(true) `AIOPS_DAILY_DIGEST_ENABLED`(true)
  `AIOPS_DAILY_DIGEST_HOUR`(9) `AIOPS_DAILY_DIGEST_ALWAYS`(false)
  `AIOPS_RECOVERY_PROBE_SECONDS`(300) `AIOPS_RECOVERY_PROBE_AFTER_SECONDS`(300)
- **变更日志**：`CHANGELOG.md`；噪声治理评审意见：`design-doc/aiops-noise-reduction-2026-09-15-review.md`。
