"""Prometheus 客户端 + machine_info 轻量 CMDB。

指标一律用 range query 回溯（见 context_collector）：节点故障时 exporter 已经掉线，
即时查询只能拿到 0。

客户端做成进程级单例：machine_info 的 TTL 缓存是实例属性，
每次调用都新建实例会让缓存永不命中。
"""
from __future__ import annotations

import math
import threading
import time
from datetime import datetime
from typing import Any

import httpx

from app.config import settings
from app.logging_setup import get_logger, log

logger = get_logger("app.integrations.prometheus")

_MACHINE_INFO_SELECTORS = (
    'machine_info{{ip="{ip}"}}',
    'machine_info{{instance=~"{ip}(:.*)?"}}',
    'machine_info{{hostname="{hostname}"}}',
)

# summarize_series 每次最多返回多少条序列，避免一个查询把 prompt 撑爆
MAX_SERIES_PER_QUERY = 12


class PrometheusClient:
    def __init__(self, base_url: str | None = None, timeout: float | None = None) -> None:
        self.base_url = (base_url or settings.prometheus_url or "").rstrip("/")
        self.timeout = timeout or settings.prometheus_timeout
        self._cache: dict[str, tuple[float, dict[str, str]]] = {}
        # 持久连接：一次工单要串行发 4~60 条查询，每次 httpx.get 都新建连接/TLS 握手，
        # 固定开销很可观（K8s 客户端早就是这么做的）。
        self._client: httpx.Client | None = None

    @property
    def configured(self) -> bool:
        return bool(self.base_url)

    def _http(self) -> httpx.Client:
        if self._client is None:
            self._client = httpx.Client(timeout=self.timeout)
        return self._client

    def _get(self, path: str, params: dict[str, Any]) -> dict[str, Any] | None:
        if not self.configured:
            return None
        url = f"{self.base_url}{path}"
        try:
            response = self._http().get(url, params=params)
            response.raise_for_status()
            return response.json()
        except Exception as exc:  # 外部依赖异常不能影响主流程
            log(logger, 30, "prometheus_query_failed", url=url, error=str(exc))
            return None

    def instant(self, query: str) -> list[dict[str, Any]]:
        data = self._get("/api/v1/query", {"query": query})
        return (data or {}).get("data", {}).get("result", []) if (data or {}).get("status") == "success" else []

    def range(self, query: str, start: datetime, end: datetime, step: int = 15) -> list[dict[str, Any]]:
        data = self._get(
            "/api/v1/query_range",
            {"query": query, "start": start.timestamp(), "end": end.timestamp(), "step": step},
        )
        return (data or {}).get("data", {}).get("result", []) if (data or {}).get("status") == "success" else []

    def machine_info(self, ip: str | None, hostname: str | None) -> dict[str, str]:
        """查询 machine_info 作为一期轻量 CMDB。失败返回空 dict，绝不抛异常。"""
        cache_key = f"{ip}|{hostname}"
        now = time.time()
        cached = self._cache.get(cache_key)
        if cached is not None and now - cached[0] < settings.machine_info_ttl_seconds:
            return cached[1]

        result: dict[str, str] = {}
        for template in _MACHINE_INFO_SELECTORS:
            if "{ip}" in template and not ip:
                continue
            if "{hostname}" in template and not hostname:
                continue
            rows = self.instant(template.format(ip=ip or "", hostname=hostname or ""))
            if rows:
                result = {key: value for key, value in rows[0].get("metric", {}).items() if key != "__name__"}
                break

        self._cache[cache_key] = (now, result)
        return result


def summarize_series(result: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """把 range query 结果压成 min/max/first/last 摘要，控制喂给模型的体积。"""
    summaries: list[dict[str, Any]] = []
    for series in result[:MAX_SERIES_PER_QUERY]:
        numeric = []
        for _ts, raw in series.get("values") or []:
            try:
                value = float(raw)
            except (TypeError, ValueError):
                continue
            if math.isfinite(value):
                numeric.append(value)
        if not numeric:
            continue
        summaries.append(
            {
                "metric": series.get("metric", {}),
                "samples": len(numeric),
                "min": round(min(numeric), 4),
                "max": round(max(numeric), 4),
                "first": round(numeric[0], 4),
                "last": round(numeric[-1], 4),
            }
        )
    return summaries


_client_lock = threading.Lock()
_shared_client: PrometheusClient | None = None


def get_client() -> PrometheusClient:
    """进程级单例，保证 machine_info 的 TTL 缓存真的生效。"""
    global _shared_client
    if _shared_client is None:
        with _client_lock:
            if _shared_client is None:
                _shared_client = PrometheusClient()
    return _shared_client
