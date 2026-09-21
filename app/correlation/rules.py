"""关联规则加载（force_link / never_link / causal_rules / 时间窗口 / 权重）。"""
from __future__ import annotations

from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

from app.config import settings


@dataclass
class CausalRule:
    id: str
    name: str
    cause: str
    symptoms: list[str]
    same_node: bool = True
    before_seconds: int = 60
    after_seconds: int = 300
    weight: int = 50
    relation: str = "SYMPTOM"

    def matches(self, new_alertname: str, seen_alertnames: set[str]) -> str | None:
        """返回新告警在规则中的角色：'cause' / 'symptom'，不匹配返回 None。"""
        if new_alertname == self.cause and seen_alertnames & set(self.symptoms):
            return "cause"
        if new_alertname in self.symptoms and self.cause in seen_alertnames:
            return "symptom"
        return None


@dataclass
class RuleSet:
    weights: dict[str, int] = field(default_factory=dict)
    threshold: int = 70
    require_strong_link: bool = True
    strong_link_signals: list[str] = field(default_factory=list)
    correlation_windows: dict[str, int] = field(default_factory=dict)
    force_link: dict[str, list[str]] = field(default_factory=dict)
    never_link: dict[str, list[str]] = field(default_factory=dict)
    root_priority: dict[str, int] = field(default_factory=dict)
    fingerprint_dimensions: dict[str, list[str]] = field(default_factory=dict)
    causal_rules: list[CausalRule] = field(default_factory=list)
    context_queries: dict[str, list[str]] = field(default_factory=dict)

    # --- 查询接口 ---
    def window_for(self, alertname: str) -> int:
        if alertname in self.correlation_windows:
            return int(self.correlation_windows[alertname])
        return int(self.correlation_windows.get("default", 300))

    def max_window(self) -> int:
        values = [int(v) for v in self.correlation_windows.values()] or [300]
        return max(values)

    def never(self, a: str, b: str) -> bool:
        return b in self.never_link.get(a, []) or a in self.never_link.get(b, [])

    def force(self, a: str, b: str) -> bool:
        return b in self.force_link.get(a, []) or a in self.force_link.get(b, [])

    def priority(self, alertname: str) -> int:
        return int(self.root_priority.get(alertname, self.root_priority.get("default", 10)))

    def fingerprint_dimensions_for(self, entity_type: str) -> list[str]:
        """注意：不能叫 fingerprint_dimensions —— 会与同名 dataclass 字段冲突，
        实例属性(dict)会覆盖方法，调用时报 'dict' object is not callable。
        """
        return list(self.fingerprint_dimensions.get(entity_type, self.fingerprint_dimensions.get("default", [])))

    def causal_match(self, new_alertname: str, seen: set[str]) -> tuple[CausalRule, str] | None:
        best: tuple[CausalRule, str] | None = None
        for rule in self.causal_rules:
            role = rule.matches(new_alertname, seen)
            if role is None:
                continue
            if best is None or rule.weight > best[0].weight:
                best = (rule, role)
        return best

    def linkable(self, signals: set[str]) -> bool:
        if not self.require_strong_link:
            return True
        return bool(signals & set(self.strong_link_signals))


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    return data if isinstance(data, dict) else {}


def _load(rules_dir: Path) -> RuleSet:
    correlation = _read_yaml(rules_dir / "correlation.yaml")
    causal_doc = _read_yaml(rules_dir / "causal_rules.yaml")
    context_doc = _read_yaml(rules_dir / "context_queries.yaml")

    causal_rules: list[CausalRule] = []
    for item in causal_doc.get("rules", []) or []:
        try:
            causal_rules.append(
                CausalRule(
                    id=str(item["id"]),
                    name=str(item.get("name", item["id"])),
                    cause=str(item["cause"]["alertname"]),
                    symptoms=[str(s) for s in item.get("symptoms", [])],
                    same_node=bool(item.get("match", {}).get("same_node", True)),
                    before_seconds=int(item.get("window", {}).get("before_seconds", 60)),
                    after_seconds=int(item.get("window", {}).get("after_seconds", 300)),
                    weight=int(item.get("weight", 50)),
                    relation=str(item.get("relation", {}).get("type", "SYMPTOM")),
                )
            )
        except (KeyError, TypeError, ValueError):
            continue

    return RuleSet(
        weights={k: int(v) for k, v in (correlation.get("weights") or {}).items()},
        threshold=int(correlation.get("threshold", 70)),
        require_strong_link=bool(correlation.get("require_strong_link", True)),
        strong_link_signals=[str(s) for s in correlation.get("strong_link_signals", [])],
        correlation_windows={k: int(v) for k, v in (correlation.get("correlation_windows") or {}).items()},
        force_link={str(k): [str(x) for x in (v or [])] for k, v in (correlation.get("force_link") or {}).items()},
        never_link={str(k): [str(x) for x in (v or [])] for k, v in (correlation.get("never_link") or {}).items()},
        root_priority={k: int(v) for k, v in (correlation.get("root_priority") or {}).items()},
        fingerprint_dimensions={
            str(k): [str(x) for x in (v or [])] for k, v in (correlation.get("fingerprint_dimensions") or {}).items()
        },
        causal_rules=causal_rules,
        context_queries={str(k): [str(x) for x in (v or [])] for k, v in (context_doc.get("queries") or {}).items()},
    )


@lru_cache(maxsize=1)
def _cached(rules_dir_str: str) -> RuleSet:
    return _load(Path(rules_dir_str))


def get_rules() -> RuleSet:
    return _cached(str(settings.rules_dir))


def reload_rules() -> RuleSet:
    _cached.cache_clear()
    return get_rules()
