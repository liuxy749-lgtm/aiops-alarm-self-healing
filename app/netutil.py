"""跨模块共享的小工具：IP 判定与 PromQL 字符串转义。

单独成模块的原因：这两件事在 normalizer / enricher / context_collector / sweeper
里各写过一份，改一处漏一处的风险太高。
"""
from __future__ import annotations

import re

IPV4_RE = re.compile(r"\d{1,3}(?:\.\d{1,3}){3}")


def is_ipv4(value: str | None) -> bool:
    """整个字符串就是一个 IPv4（不是「包含」）。"""
    return bool(value and IPV4_RE.fullmatch(value))


def promql_string(value: str | None) -> str:
    """把值安全地放进 PromQL 双引号字符串里。

    必须同时转义反斜杠和双引号：
      · 只转义反斜杠 → 标签值里带 `"` 会截断字符串，查询语法错误（该条查询返空）
      · 不转义反斜杠 → PromQL/Go 解析器可能报非法转义（曾出现过：re.escape 的 `\\.` 被拒）
    顺序很重要：先反斜杠后双引号，否则会把刚加上的转义符再转一次。
    """
    return str(value or "").replace("\\", "\\\\").replace('"', '\\"')
