# 变更日志 · AIOps 告警事件中心（Event Gateway / Correlation / Incident）

按日期倒序。每条写清「改了什么 / 为什么 / 怎么验证 / 影响面」。
设计与评审文档见 `design-doc/`；已实测口径见 `README.md` §7。

---

## 2026-09-16

### 1. 修复：恢复探测的 `up{}` 判据永远查不到序列（PromQL `=~` 两端自动锚定）

- **现象**：GPU XID 工单（`INC-20260915-039`）的告警早已停止、GPU 也确认恢复，却一直挂 `OPEN`，
  探测结论恒为 `unverified`，每天 09:00 汇总都还在提醒它。
- **根因**：09-14 把兜底判据"改精确"成 `up{instance=~"^<ip>:"}` —— PromQL 的正则匹配是
  **两端自动锚定**（等价于 `^(?:...)$`），这等于要求 instance 以冒号**结尾**，
  而真实值是 `198.51.100.9:9400` → 永远 0 条序列 → `unverified` → **永不自动关单**。
  影响面：所有走 `up{}` 兜底判据的工单（GPU/DCGM 这类没有节点判据的）；
  节点类因为先走 K8s Ready 判据，所以这个 bug 一直没暴露。
- **修复**：改成 `^{re.escape(ip)}(:.*)?`（尾部必须留 `(:.*)?`）。
  真实指标源对照（同一实例）：

  | 写法 | 结果 |
  | --- | --- |
  | `up{instance=~"^198.51.100.9:"}` | 0 条（错的写法） |
  | `up{instance=~"^172\.24\.207\.9(:.*)?"}` | 1 条（`198.51.100.9:9400 = 1`，修好后） |
  | `up{instance="198.51.100.9:9400"}` | 1 条（等值对照） |

- **效果**：部署后巡检自动判定该工单 `recovered`
  （`{reason: metrics_recovered, verified_by: prometheus}`）→ 自动关单 + 自动推「工单恢复」卡
  （09-16 15:20:42），全程不需要人工干预。
- 回归用例 `test_recovery_up_query_matches_real_instance`（断言查询必须带 `(:.*)?`）。

### 2. 修复：GPU/DCGM 类告警的节点为空

- 实体节点的取值优先级改为：**payload 显式字段 → 告警标签（`node`/`nodename`/`kubernetes_node`/`Hostname`）
  → `target_ident`/`hostname` 兜底**。
- 原因：夜莺不少规则的 `target_ident`/`hostname` 实际是 instance（形如 `198.51.100.9:9400`），
  而 DCGM 的 tags 里本来就带 `Hostname` / `kubernetes_node` —— 之前被 instance 抢先取到，
  导致这类告警 `node` 为空：卡片「节点」列空白、无法按机器聚合。
- 实测效果：GPU XID 告警 `node` 由 `None` → `bm-example-zone1-d-a100-40g-2-99`，
  实体 id 同步变为 `bm-example-zone1-d-a100-40g-2-99:gpunvidia6`（身份更稳，换 IP 不变）。
- 注意：节点类告警（master NotReady）的 node/hostname/ip 与改动前**逐一比对完全一致**（无回归）。

### 3. 恢复判据坚持「同类型」

- 节点类（`root_entity_type=node`）→ 继续用 `kube_node_status_condition{condition="Ready"}`；
- GPU/Pod 等**挂在节点上的实体** → 优先用采集端 `up{}`（dcgm-exporter / node-exporter），
  因为"节点 Ready"**不能代表 GPU 好了**；只有完全没有 IP 时才退回节点状态兜底，
  并在证据里标注 `node_condition_fallback`（卡片上能看到判定依据）。
- 反例用例 `test_non_node_entity_uses_exporter_up_not_node_ready`：
  节点 Ready=true 但采集端 up=0 → 必须判 `not_recovered`。

### 验证

- `pytest` **76 passed**（新增 3 条：up{} 锚定 / DCGM 标签抬节点 / 非节点实体判据）。
- 真实环境双向复核：指标源实测三种写法对照（见上表）；归档里的真实 payload 重跑 normalizer
  对照改动前后的 node/hostname/ip（节点类无变化、GPU 类补齐节点）。

---

## 2026-09-15

### 1. 噪声治理：同一故障收敛到一张工单 + 每天最多一张卡

- **stale 窗口 900 → 7200 秒**（`app/config.py`）：该值必须大于告警源的重发周期（实测夜莺 3600 秒），
  否则"持续未恢复"会被判成"结束后又新发"。此前一场四天未恢复的 master 故障被切成 17 张工单。
- **按 fingerprint 复用旧工单**（`app/services/pipeline.py` + `queries.incident_by_fingerprint()`）：
  关联引擎无候选时，若 24 小时内有过同 fingerprint（同规则+同资源）的未合并工单则挂回去，`action=RECURRENCE`。
- **允许 RESOLVED 复开**（`incident_service.attach_alert`）：复开时清 `resolved_at`，事件记 `from_status`。
- **卡片按天封顶**（`pipeline.sent_today()`）：同一工单当天只发第一张卡（按 `incident_id` 计数，恢复卡豁免）。
- **每日 09:00 汇总**（`app/services/digest.py`，搭巡检车，不引 cron/APScheduler）：
  判据是"当天本地日有没有成功发过"→ 重启/错过整点都能补发，不漏不重。
- **修 3 个既有缺陷**：enricher 4 处 PromQL 注入（改用 `promql_string`）、
  `up{instance=~"^ip:"}` 未做正则转义（补 `re.escape`）、事件通道卡片未 `flush()` 导致留档缺行。

> 评审意见：`design-doc/aiops-noise-reduction-2026-09-15-review.md`（12 条断言全部核对通过）。

### 2. 主动恢复探测（运维处理完 → 系统自己发现"好了没有"）

- 触发条件：**告警静默 ≥ 5 分钟**（不是"告警已过期"——否则修好后最长要等 2 小时 stale 窗口），
  之后每 5 分钟探测一次；同一工单按 `last_probe_at` 限流。
- 判据（同类型 + 精确）：节点类查 `kube_node_status_condition{node,condition="Ready",status="true"}`；
  通用兜底 `up{instance=~"^<IP>:"}`；**查不到序列 = unverified（不给结论）**。
- 结论处理：`recovered` → 关单 + 群里通报恢复卡；`not_recovered` / `unverified` → 保持 OPEN，等次日 09:00 汇总。
- **没配指标源就不探测**（探测不出 ≠ 已恢复；静默不等于恢复）。
- 新增字段 `last_probe_at` / `last_probe_result`（限流 + 留痕；`_ensure_columns` 自动迁移）。
- 新增配置：`AIOPS_RECOVERY_PROBE_SECONDS`(300) / `AIOPS_RECOVERY_PROBE_AFTER_SECONDS`(300)。

### 3. 汇总卡增强

- 每条未恢复工单带上「当前疑似根因（截断）+ AI 置信度 + 已持续时长」——
  卡片被按天压掉时，这是群里唯一能看到最新判断的地方。
- 默认**只在有事时发**（没有未恢复工单不发），避免一年 365 张"平安卡"；
  要每天固定报到用 `AIOPS_DAILY_DIGEST_ALWAYS=true`。

### 4. Web 工单页面（只读、无鉴权）

- `GET /ui/incidents`：未恢复工单列表，**点标题行就地展开**详情（纯 `<details>`，零 JS、零 CDN，离线可用）；
  `?scope=all` 看最近全部。`GET /ui/incidents/{id}`：进来即展开指定工单。`/ui` 跳列表。
- 详情含：字段区 / AI 判断与证据 / 建议排查 / 节点详情表 / 关联告警表 / 时间线 / 原始 JSON 链接。
- 实现：`app/ui.py`（服务端渲染）+ `app/api/routes.py` 三个 GET 路由；只注册 GET，不带写操作。

### 5. 飞书卡片跳转按钮

- 工单卡带 `[查看工单详情]` → `/ui/incidents/{id}`、`[未恢复工单列表]` → `/ui/incidents`；汇总卡带后者。
- 按钮必须是**绝对地址**（卡片在飞书客户端渲染），故 `AIOPS_PUBLIC_BASE_URL` 默认为
  `http://192.0.2.115:8701`，可用环境变量覆盖成域名/其它网段地址。

### 6. 修复：工单号复用导致新告警的卡片被静默压掉

- **现象**：人工清理掉某张测试工单后，新告警建单**复用了同一个工单号**，
  而飞书留档（append-only）里还留着那个号的旧卡记录 → `already_notified` 判定"已推过创建卡" → 卡片不发。
  实测影响：用户手动触发的 GPU XID 告警建单后群里没看到卡。
- **修复**：`incident_service.next_incident_id()` 改为**单调递增** —— 序号取
  `incidents ∪ feishu_messages ∪ incident_events` 里用过的最大号 +1，删工单也不再回退。
- 回归用例：`test_incident_id_never_reuses_a_notified_number`。

### 7. 运维数据治理

- **合并 11 张同故障重复工单** → `INC-20260915-035`（4 台 master NotReady，共享同一组告警指纹；
  用应用自带 `merge_incidents()`，被合并方置 `MERGED` 并留时间线事件）。合并后 45 条告警、已持续 25 小时。
- **清理验证用测试工单**及其告警/关联/时间线/原始事件、归档行与撞号的孤儿飞书留档。
- 开发期原文另存 `data/legacy-raw-events-devphase-20260911.jsonl`（38KB，12 行；
  验证发现备份里有 5 条当前数据没有的原文，按"原文不丢"保留）。
- 删除开发期数据快照 `data-backup-20260911-165902`（5.3M）。

### 验证

- `pytest` **73 passed**（本日新增：探测三态 / 封顶 / 汇总时机 / Web 页面 / 号段单调 / 卡片按钮 / 节点详情）。
- 真实指标源复核探测判据：对仍未恢复的 4 台 master 返回 `not_recovered`（保持 OPEN），
  不再出现此前"静默即判恢复"的假恢复。
- 真实数据核对 fingerprint：归档里同一节点同一规则的 22 次重发 → 指纹只有 1 种（复用生效的前提）。

---

## 2026-09-14

### 修复：丢告警（`database is locked`）

- **根因**：webhook 侧"先读幂等、后写"的事务在快照失效时立刻报锁（`busy_timeout` 不参与），
  而旧代码把重试放在 **savepoint 内层** —— savepoint 不换快照，重试必然继续失败（实测 4 次全败，间隔等于退避）。
- **修复**：每条事件**独立事务** + 失败 `rollback()` 重开再重试；事务以 `BEGIN IMMEDIATE` 起步（冲突变排队）；
  同一事件重试期间归档只写一次；失败日志带 `event_id`；新增 `aiops_events_locked_retry_total`。
- 新增补录工具 `tools/backfill_raw_events.py`（把归档里"有原文、未入库"的事件重投回网关，默认只报告）。

### 修复：飞书 dry-run 指标口径

- 干跑也计入了 `aiops_feishu_sent_total`，监控显示"事件通道已发 31 条"而实际一条没发。
  拆出 `aiops_feishu_dry_run_total`，`sent_total` 只在真发时记。

### 修复：假恢复（静默被当成恢复）

- **节点 IP 取错**：节点告警的 `instance` 可能是采集端地址（实测 kube-state-metrics `198.51.100.210:8080`，
  4 台节点共用）→ normalizer 不再把集群级采集端 instance 当实体 IP，enricher 用 `kube_node_info.internal_ip` 覆盖。
- **恢复判据太弱**：改为 `recovered / not_recovered / unverified` 三态，节点类查 K8s Ready 条件，
  `up` 精确匹配 `^<ip>:`，**查不到证据只判 unverified**（旧代码空结果即判 recovered）。
- **stale ≠ 恢复**：只有夜莺显式恢复（`is_recovered`）才进恢复观察期；仅静默的工单保持 OPEN。
- 恢复卡新增「恢复依据」一行。

### 卡片格式

- 标题 = 告警等级 + 短故障标题（模型输出 `title` 字段，≤20 字；不再把 228 字的 AI 摘要当标题）。
- 新增「节点详情」段：合并工单把**所有机器**写全（节点名 + IP + 告警类数 + 根因标记）。
- 时间口径统一北京时间（喂给模型的上下文也改成本地时间，避免同一张卡里两个差 8 小时的时间）。
- 时间线去掉元信息、聚合重复事件、补上「AI 分析完成（引擎，置信度）」。

---

## 2026-09-11

### 一期交付

- Event Gateway / Alert 去重 / Correlation Engine / Incident 引擎 / Context Collector / AI 诊断 全部落地，
  与真实夜莺（n9e v8.5.1）、Prometheus、K8s 只读、DeepSeek、飞书工单通道打通。
- **A 方案（快回 200 + 副作用出锁）**上线：webhook 入库 11~15 秒 → **约 100ms**。
- 两轮代码评审修复（共 40+ 真 bug），48 个用例全绿。
- 代码推送 Gitee monorepo `git@e.gitee.com:baai-infra/ai-ops.git` 子目录 `aiops-alarm-self-healing/`。
