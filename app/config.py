"""全局配置：全部通过环境变量注入，启动时一次性解析。"""
from __future__ import annotations

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent  # 仓库根目录


def _env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name)
    if value is None:
        return default
    value = value.strip()
    return value if value != "" else default


def _env_bool(name: str, default: bool = False) -> bool:
    raw = _env(name)
    if raw is None:
        return default
    return raw.lower() in ("1", "true", "yes", "on")


def _env_int(name: str, default: int) -> int:
    raw = _env(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = _env(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


class Settings:
    def __init__(self) -> None:
        # --- 存储 ---
        # raw_events 用本地 JSONL 归档；alerts/incidents 等结构数据走 SQLite
        self.data_dir: Path = Path(_env("AIOPS_DATA_DIR", str(BASE_DIR / "data")))
        self.raw_dir: Path = self.data_dir / "raw_events"
        self.outbox_dir: Path = self.data_dir / "outbox"
        self.db_path: Path = Path(_env("AIOPS_DB_PATH", str(self.data_dir / "aiops.db")))
        self.sqlite_url: str = f"sqlite+pysqlite:///{self.db_path}"

        # --- 规则 ---
        # 关联阈值在 rules/correlation.yaml 里（单一来源），不要在这里再放一份
        self.rules_dir: Path = Path(_env("AIOPS_RULES_DIR", str(BASE_DIR / "rules")))

        # --- Alert / Incident 生命周期 ---
        # ⚠️ stale 窗口必须**大于告警源的重发周期**（线上实测夜莺是 3600 秒）。
        # 原来是 900 秒：一个持续未恢复的故障每小时被判一次 stale、下一次重发又被
        # 当成新告警建新单 —— 09-14~09-15 一场四天没恢复的 master 故障因此被切成
        # 17 张工单、24 次 LLM 调用、24 张卡片，且每条 Alert 的 occurrence_count 都是 1，
        # 完全看不出「同一个故障已经烧了四天」。
        self.alert_stale_seconds: int = _env_int("AIOPS_ALERT_STALE_SECONDS", 7200)
        self.sweep_interval_seconds: int = _env_int("AIOPS_SWEEP_INTERVAL_SECONDS", 30)
        self.recovery_observe_seconds: int = _env_int("AIOPS_RECOVERY_OBSERVE_SECONDS", 300)
        # 同类故障复发：在这个窗口内、同一 fingerprint 再次触发，挂回原工单（必要时复开），
        # 而不是新建一张。stale 窗口只能防「连续重发」，这一层防「有间隔的复发」。
        self.incident_recurrence_hours: int = _env_int("AIOPS_INCIDENT_RECURRENCE_HOURS", 24)

        # --- 主动恢复探测（用户 2026-09-15 要求：等人工处理完要能自动发现恢复）---
        # 告警静默只是"没消息"，不能当恢复；夜莺也不一定发 is_recovered。
        # 所以对「已无 FIRING 告警 + 静默够久」的工单主动查权威判据（K8s Ready / node-exporter up）：
        # 探测到真恢复 → 关单 + 群里通报恢复卡；没探测到 → 保持 OPEN，等次日 09:00 汇总。
        self.recovery_probe_seconds: int = _env_int("AIOPS_RECOVERY_PROBE_SECONDS", 300)  # 同一工单探测间隔
        self.recovery_probe_after_seconds: int = _env_int("AIOPS_RECOVERY_PROBE_AFTER_SECONDS", 300)  # 静默多久后开始探

        # --- 上下文采集 ---
        self.context_lookback_seconds: int = _env_int("AIOPS_CONTEXT_LOOKBACK_SECONDS", 600)
        self.context_step_seconds: int = _env_int("AIOPS_CONTEXT_STEP_SECONDS", 15)
        self.context_max_queries: int = _env_int("AIOPS_CONTEXT_MAX_QUERIES", 60)
        self.context_deadline_seconds: int = _env_int("AIOPS_CONTEXT_DEADLINE_SECONDS", 30)

        # --- 保留策略（防止 SQLite 与归档目录无界增长）---
        self.raw_retention_days: int = _env_int("AIOPS_RAW_RETENTION_DAYS", 30)
        self.event_retention_days: int = _env_int("AIOPS_EVENT_RETENTION_DAYS", 90)

        # --- 外部系统 ---
        self.prometheus_url: str | None = _env("AIOPS_PROMETHEUS_URL")
        self.prometheus_timeout: float = _env_float("AIOPS_PROMETHEUS_TIMEOUT", 5)
        self.machine_info_ttl_seconds: int = _env_int("AIOPS_MACHINE_INFO_TTL_SECONDS", 300)

        self.k8s_api_url: str | None = _env("AIOPS_K8S_API_URL")
        self.k8s_token: str | None = _env("AIOPS_K8S_TOKEN")
        self.k8s_ca_file: str | None = _env("AIOPS_K8S_CA_FILE")
        self.k8s_verify: bool = _env_bool("AIOPS_K8S_VERIFY", True)
        self.k8s_timeout: float = _env_float("AIOPS_K8S_TIMEOUT", 8)

        self.deepseek_api_key: str | None = _env("DEEPSEEK_API_KEY")
        self.deepseek_base_url: str = _env("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
        self.deepseek_model: str = _env("DEEPSEEK_MODEL", "deepseek-chat")
        self.llm_timeout: float = _env_float("AIOPS_LLM_TIMEOUT", 60)
        self.llm_max_retries: int = _env_int("AIOPS_LLM_MAX_RETRIES", 2)
        # 走公网模型时脱敏资产信息（IP/主机名）
        self.mask_assets: bool = _env_bool("AIOPS_MASK_ASSETS", True)

        # 飞书双通道：事件通道（Alert 粒度）/ 工单通道（Incident 粒度）
        self.feishu_event_webhook: str | None = _env("AIOPS_FEISHU_EVENT_WEBHOOK")
        self.feishu_incident_webhook: str | None = _env("AIOPS_FEISHU_INCIDENT_WEBHOOK")
        self.feishu_timeout: float = _env_float("AIOPS_FEISHU_TIMEOUT", 8)

        # --- 噪声抑制（用户 2026-09-15 要求）---
        # 同一工单的告警卡片每个自然日最多一张：事件驱动照旧（工单该建就建、
        # 该关联就关联），只是卡片按天收口。恢复卡不受限（终态、每单一次、是好消息）。
        self.daily_card_cap: bool = _env_bool("AIOPS_DAILY_CARD_CAP", True)
        # 每天这个整点（本地时区）把「仍未恢复」的工单汇总推一遍
        self.daily_digest_hour: int = _env_int("AIOPS_DAILY_DIGEST_HOUR", 9)
        self.daily_digest_enabled: bool = _env_bool("AIOPS_DAILY_DIGEST_ENABLED", True)
        # 没有未恢复工单时是否也发汇总卡（默认不发：只在有事时汇报，避免每天一张平安卡）。
        self.daily_digest_always: bool = _env_bool("AIOPS_DAILY_DIGEST_ALWAYS", False)
        # 卡片里 Web 详情链接的对外根地址（如 https://aiops.example.com）。
        # 默认指向网关自身（内网直接访问）：卡片上的「查看工单详情」按钮需要**绝对地址**，
        # 而飞书卡片是在客户端渲染的，相对路径点不动 —— 所以这里必须有值按钮才会出现。
        # 换域名/别的网段地址时用环境变量 AIOPS_PUBLIC_BASE_URL 覆盖即可（不用改代码）。
        self.public_base_url: str | None = (
            _env("AIOPS_PUBLIC_BASE_URL") or "http://192.0.2.115:8701"
        ).rstrip("/") or None

        self.log_level: str = (_env("AIOPS_LOG_LEVEL", "INFO") or "INFO").upper()
        self.display_timezone: str = _env("AIOPS_DISPLAY_TZ", "Asia/Shanghai")

        # 调试开关：跳过事件幂等（同一份 body 可反复处理）。
        # ⚠️ 仅供联调反复触发整条链路用，生产必须关闭 ——
        # 打开后夜莺的 HTTP 重试会重复建工单。状态在 /readyz 可见。
        self.debug_skip_dedup: bool = _env_bool("AIOPS_DEBUG_SKIP_DEDUP", False)

        # 副作用出锁的回退开关：true 时采集/诊断/推卡在 webhook 内同步执行
        # （行为与改动前一致，用于对照与回滚）。默认 false = 后台执行。
        self.inline_analysis: bool = _env_bool("AIOPS_INLINE_ANALYSIS", False)
        # 卡住的分析多久算超时（sweeper 兜底重排），以及最多重试几次
        self.analysis_stale_seconds: int = _env_int("AIOPS_ANALYSIS_STALE_SECONDS", 300)
        self.analysis_max_attempts: int = _env_int("AIOPS_ANALYSIS_MAX_ATTEMPTS", 3)

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.outbox_dir.mkdir(parents=True, exist_ok=True)


settings = Settings()
