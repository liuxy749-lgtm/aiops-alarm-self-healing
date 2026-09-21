# 《告警噪声治理改动汇总》评审意见

**被评文档：** `design-doc/aiops-noise-reduction-2026-09-15.md`（2026-09-15）
**评审：** Hermes（实施方）　**日期：** 2026-09-15
**核验方式：** 逐条对照工作区代码（`git diff 1c48df1..工作区`）+ 实跑 `pytest`，不采信文档自述

---

## 0. 结论

**可以合入。** 文档里 12 条技术断言全部与实现一致，未发现事实性错误（核验表见 §1）；
`pytest` 实跑 **66 passed**（新增 10 条），与文档声称一致。

**但有 2 项必须先定，1 项建议改判据：**

1. §7 把「`/ui` 详情页路由不存在」当成纯功能缺口 —— 真正的风险是**入口无认证**（见 §2.1），
   配 `AIOPS_PUBLIC_BASE_URL` 之前必须先做访问控制；
2. 被按天封顶压掉的**根因更新卡没有补偿路径**，汇总卡里也没有根因（见 §2.2）；
3. fingerprint 复用用 **24 小时时间窗**，对「重发周期 > 24h」的规则依旧每天一张新单 ——
   与本次目标（同一故障收敛）冲突（见 §3.1）。

三处取舍（按 `incident_id` 而非 `kind` 计数、恢复卡豁免、汇总只认 `ok=True` 且搭巡检车）经核验**判断正确**，见 §2.3。

---

## 1. 核验表（逐条对照，全部通过）

| 文档断言 | 代码位置 | 结果 |
| --- | --- | --- |
| `alert_stale_seconds` 900 → 7200 | `app/config.py:65` | ✅ |
| 新增 `incident_recurrence_hours`（默认 24） | `app/config.py:70` | ✅ |
| 新增 `daily_card_cap` / `daily_digest_hour` / `daily_digest_enabled` / `public_base_url` | `app/config.py:109,111,112,115` | ✅ |
| `sent_today()` 按 `incident_id` 计数 + 只认 `ok=True` | `app/services/pipeline.py:263` | ✅ |
| 恢复卡豁免 `_DAILY_CAP_EXEMPT = {"incident_resolved"}` | `app/services/pipeline.py:260` | ✅ |
| 关联引擎无候选时按 fingerprint 复用（`RECURRENCE`） | `app/services/pipeline.py:426` + `app/db/queries.py:62` | ✅ |
| 允许 `RESOLVED` 复开，清 `resolved_at`、事件带 `from_status` | `app/services/incident_service.py:155` | ✅ |
| 汇总搭巡检车、独立 session、`already_sent_today` 只认 `ok=True` | `app/main.py:_digest_once`、`app/services/digest.py:32,53,79` | ✅ |
| PromQL 注入修复 4 处（改用 `promql_string`） | `app/services/enricher.py:53,59,85,99` | ✅ 实为 4 处 |
| `up{instance=~"^ip:"}` 补正则转义 | `app/services/sweeper.py:77` | ✅ |
| 事件通道卡片补 `flush()` | `app/services/pipeline.py:365` | ✅ |
| §7「应用只有 `/api/v1/*` 和 `/`」 | `app/main.py:122` + `app/api/routes.py` | ✅ 准确 |
| `pytest -q` 66 passed（+10） | 实跑 | ✅ 66 passed |

**顺带确认**：`notify_alert_now` 的 flush 缺陷描述准确 —— `WorkerSessionLocal` 是 AUTOCOMMIT +
`autoflush=False`，`_post` 只 `session.add`，不 flush 确实不会发出 INSERT，`FeishuMessage` 缺行会让
`already_notified`（`pipeline.py:249`，只认 `ok=True`）失效。这个修复同时保住了恢复卡的幂等。

---

## 2. 工程缺口

### 2.1 【必须先定】详情页链接的前置条件：入口访问控制

文档 §7 只写「路由不存在、不配 base_url 就不渲染、配了就 404」。

**低估了风险**：网关现在监听 `0.0.0.0:8701` 且**无任何认证**（任何能连通的机器都能伪造告警，
也能直接读 `/api/v1/incidents/{id}`）。一旦配上 `AIOPS_PUBLIC_BASE_URL`，等于把工单详情
（内网 IP、主机名、告警原文、AI 结论、时间线）挂到一个匿名可访问的 Web 地址上。

**建议**：§7 补一条前置条件 —— **先做入口访问控制（安全组白名单 / 反向代理 + Basic 或 token），
再配 `AIOPS_PUBLIC_BASE_URL`**；否则两条路（HTML 页 / JSON 链接）都不该开。

### 2.2 【建议补】被压掉的根因更新没有补偿路径

`incident_root_changed` 卡被按天封顶压掉后：`FeishuMessage` 不落行（封顶在 `_post` 之前返回），
所以次日也不会补发（`already_notified` 认为"没发过"，但也没有任何调度会再次触发根因更新卡）——
这次根因变化对群里就是**彻底不可见**，只能靠人点开时间线看。

**建议**：汇总卡每条工单加一行「当前疑似根因（截断 40 字）+ 置信度」。
这样即使卡片当天被压，每天早上也一定能看到最新根因；改动很小（`build_digest_card` 里加一段）。

### 2.3 【认可】三处取舍经核验正确

- **按 `incident_id` 而非 `kind` 计数**：对 —— 按 kind 等于每种各发一张，达不到"每天最多一张"。
- **恢复卡豁免**：对 —— 终态、每单一次、是好消息。
- **汇总只认 `ok=True` + 搭巡检车（不引 cron）**：对 —— 发失败当天可重试；重启/错过整点都能补发，
  且 `due()` 用 `hour >= 配置值` 而不是 `hour == 配置值`，不会因为 09:00 那刻进程没起来就漏发。

---

## 3. 与本次目标冲突的设计建议

### 3.1 复用窗口 24h：判据建议改成"旧单还没关"

`queries.incident_by_fingerprint()`（`app/db/queries.py:62`）用 `Incident.last_seen >= now - 24h` 过滤。

**冲突场景**：重发周期大于 24 小时的规则（例如每天固定时间报一次的日报类告警，或人工触发的批作业告警）——
每次触发都已超出窗口 → 依旧每次新建一张工单，而旧单可能还挂在 `OPEN`（stale 已不自动关单）。
这跟"同一故障收敛到同一张工单"的目标正好相反。

**建议**：旧单仍在 `OPEN_STATES`（`OPEN/ACKNOWLEDGED/RECOVERING`，见 `queries.py:16`）时**无条件复用**；
时间窗只约束已 `RESOLVED` 的单（防止把很久以前关掉的单拉起来）。
同一 fingerprint = 同规则 + 同资源，旧单未关的情况下"挂回去"是准确定义。

### 3.2 stale 仍在结束 Alert（中期口径，建议文档写明）

本次把 stale 900 → 7200，本质是**把「重发周期 vs stale 窗口」的耦合点推远**（2× 重发周期）。
更干净的口径：stale 只表示"沉默"，**不把 Alert 置 `RESOLVED`**；只有夜莺 `is_recovered` 才结束 Alert。
这样重发周期怎么变都不会再出现"结束 → 下次重发当成新告警"的假象，也不必靠调参维持。

另外提醒一处**两层口径不一致**，建议文档写清：09-14 已改的是「stale 不自动关**工单**」（工单保持 OPEN），
而 **Alert 层仍然会被 stale 置为 RESOLVED(reason=stale)**。两层含义不同，容易在后续排查里误读。

---

## 4. 待拍板

| # | 事项 | 我的建议 |
| --- | --- | --- |
| 1 | `/ui` 详情页走「最小只读 HTML 页」还是「链接改指 `/api/v1/incidents/{id}` JSON」 | 倾向最小 HTML 页，但**必须先做 §2.1 的入口访问控制** |
| 2 | 汇总卡「没有未恢复工单也发平安卡」= 一年 365 张 | 加开关 `AIOPS_DAILY_DIGEST_ALWAYS`（默认关：仅在有事时发） |
| 3 | §3.1 复用判据改成「旧单未关即复用」 | 建议改，改动约 5 行 |
| 4 | §2.2 汇总卡加根因/置信度 | 建议加 |
| 5 | 本次改动是否一并提交 Gitee（当前 10 个文件已改 + `digest.py` 未跟踪） | 等 1–4 定了一起提交 |

---

## 5. 对文档自身的修改建议（本次未改动原文）

- §7 补前置条件（§2.1）；
- §4 补「平安卡是否可关」（§4 表第 2 项）；
- §2 补「复用窗口与重发周期的关系」（§3.1）；
- 建议追加 §9「评审结论」并指向本文件。

> 文档本体我没动 —— 需要的话我按上面四条就地补，并在头部加一行修订说明。
