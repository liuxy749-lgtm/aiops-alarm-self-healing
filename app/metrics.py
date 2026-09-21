"""极简指标暴露（Prometheus 文本格式），不引入 prometheus_client。"""
from __future__ import annotations

import threading
from collections import defaultdict

_LOCK = threading.Lock()
_COUNTERS: dict[tuple[str, tuple[tuple[str, str], ...]], float] = defaultdict(float)
_GAUGES: dict[str, float] = {}


def _key(name: str, labels: dict[str, str] | None) -> tuple[str, tuple[tuple[str, str], ...]]:
    return name, tuple(sorted((labels or {}).items()))


def inc(name: str, labels: dict[str, str] | None = None, value: float = 1.0) -> None:
    with _LOCK:
        _COUNTERS[_key(name, labels)] += value


def set_gauge(name: str, value: float) -> None:
    with _LOCK:
        _GAUGES[name] = value


def get_counter(name: str, labels: dict[str, str] | None = None) -> float:
    """读计数器当前值（测试断言与排查用；render() 是给 Prometheus 抓的）。"""
    with _LOCK:
        return _COUNTERS.get(_key(name, labels), 0.0)


def render() -> str:
    lines: list[str] = []
    with _LOCK:
        for (name, labels), value in sorted(_COUNTERS.items()):
            label_str = ",".join(f'{key}="{item}"' for key, item in labels)
            lines.append(f"{name}{{{label_str}}} {value}" if labels else f"{name} {value}")
        for name, value in sorted(_GAUGES.items()):
            lines.append(f"{name} {value}")
    return "\n".join(lines) + "\n"
