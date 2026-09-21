# 告警噪声治理改动汇总

**日期：** 2026-09-15\
**基线：** Phase 2 实施版\
**性质：** 未提交的工作区改动说明（`git status` 见 §6）\
**验证：** `pytest -q` 66 passed（其中新增 10 条）

------------------------------------------------------------------------

## 1. 起因

线上实测暴露两个互相叠加的问题：

一场 09-14 ~ 09-15 持续四天未恢复的 master NotReady 故障，被切成 **17 张工单、
24 次 LLM 调用、24 张飞书卡片**，而每条 Alert 的 `occurrence_count` 都是 1 ——
从数据上完全看不出「同一个故障已经烧了四天」。

根因是两层：

1. `alert_stale_seconds = 900` 小于夜莺的重发周期（实测 3600 秒）。持续未恢复的
   故障每小时被判一次 stale，下一次重发就被当成全新告警建新单。
2. 关联引擎没找到候选时直接新建工单，没有「同类故障复发」这一层兜底。中途被巡检
   判过恢复的故障，每次复发都是一张新单。

用户据此提出的要求是：**同一工单的告警卡片每个自然日最多一张**。

## 2. 改动一：让同一个故障收敛到同一张工单

**stale 窗口**（`app/config.py`）：`alert_stale_seconds` 900 → **7200**。这个值必须
大于告警源的重发周期，否则「持续未恢复」会被误判成「结束后又新发」。

**按 fingerprint 复用工单**（`app/services/pipeline.py:_handle_firing`）：关联引擎
返回空候选时，再查一层 `queries.incident_by_fingerprint()` —— 找最近
`incident_recurrence_hours`（默认 24）内曾经关联过同一 fingerprint 的未合并工单，
挂回去而不是新建，`action` 记为 `RECURRENCE`。fingerprint = 同规则 + 同资源，正是
「相同类型的故障」的准确定义。

**允许 RESOLVED 复开**（`app/services/incident_service.py:attach_alert`）：原来只有
`RECOVERING` 会复开，`RESOLVED` 不会。复用逻辑要生效就必须让终态也能复开，同时清掉
`resolved_at`，事件记 `from_status` 便于回溯。

两层的分工：stale 窗口防「连续重发」，fingerprint 复用防「有间隔的复发」。

## 3. 改动二：卡片按天封顶

`app/services/pipeline.py:send_incident_card` 在既有的 `already_notified` 幂等检查
之后，加一道 `sent_today()`：当天（**本地自然日**，`app/timeutil.py:local_day_start`）
已为这张工单发过任何工单卡，就跳过，计 `aiops_incident_card_suppressed_total`。

两个刻意的设计选择：

- **按 `incident_id` 计数而非按 `kind`**：同一张工单当天的创建卡 + 根因更新卡 +
  复发卡加起来也只发第一张。按 kind 计数等于每种 kind 各发一张，达不到要求。
- **恢复卡豁免**（`_DAILY_CAP_EXEMPT = {"incident_resolved"}`）：终态、每单只可能
  一次、是好消息。压掉会让人不知道故障已经好了。

事件驱动本身不变 —— 工单该建就建、该关联就关联、诊断照跑，只是卡片按天收口。

## 4. 改动三：每日汇总（封顶规则的另一半）

新增 `app/services/digest.py`。封顶之后，一场四天没恢复的故障从第二天起彻底安静，
没人推也就没人想起它还挂着。汇总卡每天把「还没好的都在这」说一次。

- 触发搭巡检循环的车（`app/main.py:_digest_once`，sweeper 每 30s 一跑），不引入
  cron / APScheduler。判据是「今天本地日历日有没有成功发过 `daily_digest`」，所以
  进程重启、或 09:00 那一刻没在运行，都能在下一次巡检补发，不漏也不重。
- 独立 session：汇总失败不能回滚掉巡检的恢复判定；两者各自 try，互不影响。
- `already_sent_today` 只认 `ok=True`：发失败不算发过，当天会重试 —— 汇总卡没有别的
  补偿路径，按「尝试过」算的话当天就永远丢了。
- 没有未恢复工单也发，报个平安。

配置：`AIOPS_DAILY_DIGEST_ENABLED`（默认开）、`AIOPS_DAILY_DIGEST_HOUR`（默认 9）。

## 5. 改动四：顺带修掉的三个缺陷

- **PromQL 注入**（`app/services/enricher.py`）：4 处把告警标签直接插进 PromQL
  选择器，改用既有的 `app/netutil.py:promql_string`。原来标签里一个 `"` 就能改写
  选择器，让 `entity.ip` 被覆盖成另一台机器的 IP，进而让恢复校验对错误的主机取
  `up`，误判恢复。
- **正则元字符**（`app/services/sweeper.py:76`）：`up{instance=~"^{ip}:"}` 里的 IP
  未做正则转义，`.` 会匹配任意字符。补 `re.escape`。
- **事件通道卡片不落库**（`app/services/pipeline.py:notify_alert_now`）：
  `WorkerSessionLocal` 是 AUTOCOMMIT + `autoflush=False`，`_post` 只 `session.add`，
  没有 flush，INSERT 从未发出。后果是 `FeishuMessage` 缺行，`already_notified`
  的 alert 粒度幂等保护形同失效。补 `session.flush()`。

## 6. 文件清单

| 文件 | 改动 |
|---|---|
| `app/config.py` | stale 900→7200；新增 recurrence / cap / digest / public_base_url 配置 |
| `app/services/pipeline.py` | `sent_today` 封顶；fingerprint 复用；`flush()` 修复 |
| `app/services/digest.py` | 新增，每日汇总 |
| `app/integrations/feishu.py` | 汇总卡 `build_digest_card`；卡片详情链接 |
| `app/services/incident_service.py` | 允许 RESOLVED 复开 |
| `app/db/queries.py` | `incident_by_fingerprint`、`unresolved_incidents` |
| `app/timeutil.py` | `local_date` / `local_day_start` / `local_hour_today` |
| `app/main.py` | 汇总搭巡检循环；启动日志加三项配置 |
| `app/services/enricher.py`、`sweeper.py` | 注入 / 转义修复 |
| `tests/test_phase1.py` | +10 条（封顶 5、汇总 5） |

## 7. 已知缺口（需决策）

卡片里的「查看工单详情」按钮指向 `{AIOPS_PUBLIC_BASE_URL}/ui/incidents/{id}`，
**但这个路由不存在** —— 应用目前只有 `/api/v1/*` JSON 接口和 `/`。
`AIOPS_PUBLIC_BASE_URL` 默认不配，此时不渲染链接，所以当前不影响运行；一旦配上
就会 404。两条路，需要选一条：

1. 加一个最小的 HTML 详情页（`/ui/incidents`、`/ui/incidents/{id}`）；
2. 链接改指 `/api/v1/incidents/{id}`，代价是点开是原始 JSON。

## 8. 验证

```bash
pytest -q                       # 66 passed
pytest -q -k "daily_cap or digest"   # 本次新增的 10 条
```

端到端：设 `AIOPS_DAILY_DIGEST_HOUR` 为当前小时之前的值，对
`/api/v1/events/nightingale` 连发两条同 fingerprint 告警 —— 第一条出卡，第二条
应只在 `FeishuMessage` 留一行、日志出 `incident_card_suppressed_daily_cap`；
等一个巡检周期后应看到 `daily_digest_sent`。未配 webhook 时卡片落到
`settings.outbox_dir` 的 `.jsonl`，可直接看内容。
