# AIOps 告警事件中心

接收 Nightingale（夜莺）等告警源的事件，做去重、关联、成单、上下文采集与大模型辅助诊断，
按工单粒度推送到飞书，并提供只读的 Web 工单页。

## 做什么

一场故障通常会产生大量告警：同一条规则按小时重发、上下游组件连锁报错、同一批机器同时失联。
逐条推送既刷屏也无法定位根因。本项目的处理方式是：

- **去重**：以「规则 + 实体 + 关键标签」计算指纹，重复上报只累加次数，不新建记录。
- **关联**：显式强关联（force_link）→ 因果规则 → 拓扑距离 → 评分，超过阈值的合成一个工单。
- **收敛**：同一故障在复发窗口内再次上报，挂回原工单，不重复建单。
- **诊断**：把关联告警、拓扑、指标回溯、K8s 事实一起交给模型，输出结论、证据、置信度与排查建议。
- **通知**：同一工单每天最多一张卡片；恢复卡单独发；未恢复的每天 09:00 汇总一次。

## 运行要求

- Python 3.12，**单副本**运行（状态在 SQLite + 进程内锁，多 worker 会导致状态不一致与重复推送）。
- 可选外部依赖，缺任一项都不阻塞告警接收，只做降级：
  - Nightingale：告警源（HTTP 回调媒介）
  - Prometheus HTTP API：指标回溯与恢复校验
  - Kubernetes 只读 API：节点 / Pod 事实
  - 飞书自定义机器人 webhook：通知（未配置时卡片落到 `data/outbox/*.jsonl`，可离线核对）
  - DeepSeek（OpenAI 兼容接口）：诊断；未配置时走规则兜底并在结果里标注 `engine=rule-stub`

## 快速开始

```bash
uv venv --python 3.12 .venv && . .venv/bin/activate
pip install -r requirements.txt

export AIOPS_PROMETHEUS_URL=http://prometheus.example.com:9090
export AIOPS_FEISHU_INCIDENT_WEBHOOK=https://open.feishu.cn/open-apis/bot/v2/hook/xxxx
export DEEPSEEK_API_KEY=sk-xxxx

uvicorn app.main:app --host 0.0.0.0 --port 8701 --workers 1
pytest
```

### systemd 托管

```ini
[Unit]
Description=AIOps event gateway
After=network-online.target

[Service]
WorkingDirectory=/opt/aiops-alarm-self-healing
EnvironmentFile=/etc/aiops-gateway.env
ExecStart=/opt/aiops-alarm-self-healing/.venv/bin/uvicorn app.main:app --host 0.0.0.0 --port 8701 --workers 1
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

配置写在 `EnvironmentFile` 里（权限 600），改完 `systemctl daemon-reload && systemctl restart`。
确认新进程已生效看 `systemctl show aiops-gateway -p ExecMainStartTimestamp --value`。

## 数据流

```
POST /api/v1/events/nightingale
  ├─ 原始事件落盘归档（按天 JSONL）+ 幂等入库（event_id）
  ├─ 标准化：规则名 → 实体类型/实体 ID/作用域；实体 IP 以 K8s 事实为准
  ├─ 富化：kube-state-metrics 节点/Pod 事实、machine_info（轻量资产表）、K8s 只读
  ├─ 指纹去重：重复上报只更新 last_seen 与 occurrence_count
  ├─ 关联：Force → 因果规则 → 拓扑 → 评分；新建工单或并入已有工单
  └─ 立即返回 200
        │
        └─ 后台单线程 worker：上下文采集 → 模型诊断 → 推卡片
              │
巡检（默认 30s）：告警过期 / 恢复校验 / 主动探测 / 每日汇总 / 保留策略
```

外部调用（Prometheus、K8s、模型、飞书）一律不持锁：进程锁只包数据库的读-判-写，
避免长事务把 webhook 的写入堵住。

## 接入 Nightingale

用内置 `Callback` 媒介，请求体直接放事件本身：

| 项 | 值 |
| --- | --- |
| 地址 | `http://<本服务>:8701/api/v1/events/nightingale` |
| 方法 | POST |
| Body | `{{ jsonMarshal $event }}` |
| 建议请求头 | `Content-Type: application/json`（服务端不依赖该头，手工解析原始 body） |

两种事件形态都支持：夜莺原生 `$event` 结构，以及自行拼装的精简结构。

幂等键为「`hash` + `status` + `trigger_time`」：夜莺的重试与重放不会产生重复工单。

注意三处字段形态：`tags` 是字符串数组（`["k=v", ...]`）；`status` 是整数，不是告警状态；
告警是否恢复以 `is_recovered` 为准。

卡片字段：级别、状态、故障对象、节点、集群、关联告警数、影响面、首次/最近异常、持续时长、
AI 摘要与疑似根因（含置信度）、关键证据、建议排查、节点详情（合并工单列出所有机器）、时间线。
底部两个按钮指向 Web 页面（`AIOPS_PUBLIC_BASE_URL` 未配置时不渲染）。

## 环境变量

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `AIOPS_DATA_DIR` | `./data` | 数据目录（raw_events/、outbox/、aiops.db） |
| `AIOPS_DB_PATH` | `./data/aiops.db` | SQLite 文件 |
| `AIOPS_ALERT_STALE_SECONDS` | `7200` | 无事件多久判定告警过期；**必须大于告警源重发周期** |
| `AIOPS_INCIDENT_RECURRENCE_HOURS` | `24` | 同指纹复发挂回原工单的时间窗 |
| `AIOPS_RECOVERY_OBSERVE_SECONDS` | `300` | 收到恢复信号后的观察期 |
| `AIOPS_RECOVERY_PROBE_SECONDS` | `300` | 主动探测间隔（同一工单限流） |
| `AIOPS_RECOVERY_PROBE_AFTER_SECONDS` | `300` | 告警静默多久后开始探测 |
| `AIOPS_SWEEP_INTERVAL_SECONDS` | `30` | 巡检间隔 |
| `AIOPS_DAILY_CARD_CAP` | `true` | 同一工单每天只推第一张卡片 |
| `AIOPS_DAILY_DIGEST_ENABLED` / `_HOUR` / `_ALWAYS` | `true` / `9` / `false` | 每日汇总：开关、时点、无未恢复工单时是否也发 |
| `AIOPS_PUBLIC_BASE_URL` | 空 | 卡片按钮与页面链接的根地址（绝对地址） |
| `AIOPS_CONTEXT_LOOKBACK_SECONDS` / `_STEP_SECONDS` | `600` / `15` | 指标回溯窗口与步长 |
| `AIOPS_CONTEXT_MAX_QUERIES` / `_DEADLINE_SECONDS` | `60` / `30` | 采集查询上限与总预算 |
| `AIOPS_RAW_RETENTION_DAYS` / `AIOPS_EVENT_RETENTION_DAYS` | `30` / `90` | 归档与记录保留天数 |
| `AIOPS_MACHINE_INFO_TTL_SECONDS` | `300` | 资产信息缓存 TTL |
| `AIOPS_ANALYSIS_STALE_SECONDS` / `_MAX_ATTEMPTS` | `300` / `3` | 分析超时与最大重试 |
| `AIOPS_PROMETHEUS_URL` | 空 | 未配则富化降级为 PARTIAL |
| `AIOPS_K8S_API_URL` / `_TOKEN` / `_CA_FILE` | 空 | 只读凭据，未配则跳过 |
| `DEEPSEEK_API_KEY` / `DEEPSEEK_MODEL` | 空 / `deepseek-chat` | 模型；未配走规则兜底 |
| `AIOPS_MASK_ASSETS` | `true` | 出网前对 IP/主机名脱敏，返回后还原 |
| `AIOPS_FEISHU_EVENT_WEBHOOK` | 空 | 事件通道（每条告警一张卡）；空=干跑落盘 |
| `AIOPS_FEISHU_INCIDENT_WEBHOOK` | 空 | 工单通道（创建/根因变化/关闭/恢复/汇总）；空=干跑落盘 |
| `AIOPS_INLINE_ANALYSIS` | `false` | 回退开关：`true` 时采集/诊断/推卡在 webhook 内同步执行 |
| `AIOPS_DEBUG_SKIP_DEDUP` | `false` | 调试用：跳过幂等与关联。生产必须关闭 |
| `AIOPS_LOG_LEVEL` | `INFO` | 日志级别 |

关联阈值只有一处来源：`rules/correlation.yaml` 的 `threshold`（改环境变量无效，`POST /api/v1/rules/reload` 热加载）。

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/v1/events/nightingale` | 告警源入口 |
| GET | `/api/v1/incidents` | 工单列表（`?status=OPEN&cluster=`） |
| GET | `/api/v1/incidents/{id}` | 详情：告警、关联原因、时间线、上下文 |
| POST | `/api/v1/incidents/{id}/ack` | 认领 |
| POST | `/api/v1/incidents/{id}/resolve` | 关闭 |
| POST | `/api/v1/incidents/{id}/alerts/{alert_id}/detach` | 摘掉误关联的告警 |
| POST | `/api/v1/incidents/{id}/analyze` | 重跑采集与诊断 |
| GET | `/api/v1/alerts` | 告警列表 |
| GET | `/api/v1/raw-events`、`/{event_id}` | 原始事件与归档校验 |
| POST/GET | `/api/v1/topology/relations` | 拓扑关系维护 |
| POST | `/api/v1/sweep` | 手动触发巡检 |
| GET | `/api/v1/stats` | 压缩率等统计 |
| GET | `/healthz`、`/readyz`、`/metrics` | 健康检查与自身指标 |
| GET | `/ui/incidents`、`/ui/incidents/{id}` | 只读 Web 工单页（列表点开就地展开，`?scope=all` 看全部） |

## 目录

```
app/
  api/routes.py           HTTP 接口与 Web 页面路由
  services/               标准化、富化、指纹、关联、工单、上下文采集、巡检、汇总、流水线
  integrations/           Nightingale / Prometheus / Kubernetes / 模型 / 飞书
  correlation/            关联规则与拓扑
  db/                     ORM 模型、会话、查询、轻量迁移
  ui.py                   工单页服务端渲染
rules/                    关联规则、上下文查询模板、拓扑规则
tests/                    端到端接口用例与单元用例
design-doc/               架构说明与变更日志
tools/                    运维脚本（归档补录等）
```

## 运维口径

- **告警过期窗口**要大于告警源的重发周期。窗口偏小会把"持续未恢复"误判成"结束又新发"，同一故障被拆成多张工单。
- **告警静默不等于恢复**。只有两个来源能结束工单：告警源显式恢复信号，或主动探测拿到权威判据
  （节点看 `kube_node_status_condition{condition="Ready"}`，其他实体看采集端 `up{instance=~"^<ip>(:.*)?"}`）。
  拿不到判据时结论为"无法确认"，工单保持打开，不做推断。
- **主动探测**：告警静默超过 `AIOPS_RECOVERY_PROBE_AFTER_SECONDS` 后按间隔探测；探测到恢复即关单并推送恢复卡。
- **通知节奏**：同一工单每天最多一张卡片，恢复卡除外；未恢复的由每日汇总提醒。
- **Web 页面默认无鉴权**，请放在私有网络或由反向代理加访问控制后再暴露。

## 已知限制

- 单副本：SQLite 与进程内锁，不支持多实例横向扩展（二期计划换队列与共享存储）。
- 关联规则目前覆盖节点与 Pod 两类实体；GPU/存储等故障链在二期补充。
- 采集与诊断依赖外部系统可用性，降级时结论会标注证据缺口，不猜测。
- 大模型输出经结构化校验（必填字段、置信度区间、证据为空时压降置信度），但不保证结论正确，最终以人工判断为准。

## 变更日志

见 `CHANGELOG.md`。

## 许可

MIT（见 `LICENSE`）。
