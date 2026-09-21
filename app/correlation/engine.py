"""关联引擎。

执行顺序：Never Link → Force Link → Causal Rule → Topology → Score ≥ 阈值。

两处与设计文档不同、但必须这么做的地方：
  1) 加了「强关联信号」门槛：文档评分表里 same_node(40)+same_cluster(10)+时间(20)=70
     恰好等于阈值，会让「同节点但互不相关」的两条告警被错误合并。
  2) relation 不写死 SYMPTOM：新告警是因果链里的 cause 时提升为 ROOT，
     否则「症状先到、根因后到」会把根因永久判错。
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.correlation.rules import RuleSet, get_rules
from app.correlation.topology import TopologyService
from app.db import queries
from app.db.models import Alert, Incident
from app.logging_setup import get_logger, log
from app.timeutil import ensure_utc

logger = get_logger("app.correlation.engine")


@dataclass
class LinkDecision:
    incident_id: str | None = None
    action: str = "NEW_INCIDENT"
    score: int = 0
    reasons: list[str] = field(default_factory=list)
    relation: str = "UNKNOWN"
    rule_id: str | None = None
    strong: bool = False
    gated: bool = False
    evaluated: list[dict] = field(default_factory=list)


class CorrelationEngine:
    def __init__(self, session: Session, rules: RuleSet | None = None) -> None:
        self.session = session
        self.rules = rules or get_rules()
        self.topology = TopologyService(session)

    # ------------------------------------------------------------------
    def _candidate_incidents(self, alert: Alert) -> list[Incident]:
        """候选工单按 incident.last_seen 过滤（不是 first_seen）。"""
        from datetime import timedelta

        window = max(self.rules.max_window(), self.rules.window_for(alert.alertname))
        since = alert.first_seen - timedelta(seconds=window)
        conditions = [
            Incident.status.in_(queries.OPEN_STATES),
            Incident.last_seen >= since,
            Incident.merged_into.is_(None),
        ]
        if alert.cluster:
            conditions.append((Incident.cluster == alert.cluster) | (Incident.cluster.is_(None)))
        return list(
            self.session.scalars(select(Incident).where(*conditions).order_by(Incident.last_seen.desc())).all()
        )

    def _node_compatible(self, alert: Alert, incident: Incident) -> bool:
        """强制关联 / 因果规则的同一节点约束。

        NodeNotReady 与 KubeletDown 必须落在同一台节点才算同一条故障链；
        不做这个校验，node01 的 NodeNotReady 会把 node02 的 KubeletDown 也吞进来。
        两边都有节点信息才做否定判断，缺信息时不下结论。
        """
        if not alert.node or not incident.node:
            return True
        return alert.node == incident.node

    def _time_score(self, alert: Alert, incident: Incident) -> tuple[int, str | None]:
        reference = ensure_utc(incident.last_seen) or ensure_utc(incident.first_seen)
        current = ensure_utc(alert.first_seen)
        if reference is None or current is None:
            return 0, None
        delta = abs((current - reference).total_seconds())
        weights = self.rules.weights
        if delta <= 60:
            return weights.get("time_1m", 20), "within_1m"
        if delta <= 180:
            return weights.get("time_3m", 15), "within_3m"
        if delta <= 300:
            return weights.get("time_5m", 10), "within_5m"
        return 0, None

    def _score(self, alert: Alert, incident: Incident, attached: list[Alert]) -> tuple[int, list[str], set[str]]:
        weights = self.rules.weights
        score = 0
        reasons: list[str] = []
        signals: set[str] = set()

        incident_entities = {(incident.root_entity_type, incident.root_entity_id)}
        incident_entities |= {(item.entity_type, item.entity_id) for item in attached}

        if (alert.entity_type, alert.entity_id) in incident_entities:
            value = weights.get("exact_entity", 50)
            score += value
            reasons.append(f"same_entity:+{value}")
            signals.add("exact_entity")
        elif alert.node and incident.node and alert.node == incident.node:
            value = weights.get("same_physical_node", 40)
            score += value
            reasons.append(f"same_node:+{value}")
            signals.add("same_node")

        best_distance: int | None = None
        if alert.entity_type and alert.entity_id:
            for entity in incident_entities:
                if not entity[0] or not entity[1]:
                    continue
                distance = self.topology.distance((alert.entity_type, alert.entity_id), entity)
                if distance in (1, 2) and (best_distance is None or distance < best_distance):
                    best_distance = distance
        if best_distance == 1:
            value = weights.get("topology_distance_1", 30)
            score += value
            reasons.append(f"topology_d1:+{value}")
            signals.add("topology")
        elif best_distance == 2:
            value = weights.get("topology_distance_2", 15)
            score += value
            reasons.append(f"topology_d2:+{value}")
            signals.add("topology")

        if alert.cluster and incident.cluster and alert.cluster == incident.cluster:
            value = weights.get("same_cluster", 10)
            score += value
            reasons.append(f"same_cluster:+{value}")

        if alert.namespace and any(item.namespace == alert.namespace for item in attached):
            value = weights.get("same_namespace", 10)
            score += value
            reasons.append(f"same_namespace:+{value}")

        alert_project = (alert.labels or {}).get("project")
        if alert_project and alert_project in {(item.labels or {}).get("project") for item in attached}:
            value = weights.get("same_project", 5)
            score += value
            reasons.append(f"same_project:+{value}")

        time_value, time_reason = self._time_score(alert, incident)
        if time_value:
            score += time_value
            reasons.append(f"{time_reason}:+{time_value}")

        return score, reasons, signals

    # ------------------------------------------------------------------
    def correlate(self, alert: Alert) -> LinkDecision:
        decision = LinkDecision()
        forced: tuple[Incident, list[str]] | None = None
        best: tuple[Incident, int, list[str], set[str], dict] | None = None

        for incident in self._candidate_incidents(alert):
            attached = queries.attached_alerts(self.session, incident.incident_id)
            attached_names = {item.alertname for item in attached}
            node_ok = self._node_compatible(alert, incident)

            # 1) Never Link 优先级最高
            if any(self.rules.never(alert.alertname, name) for name in attached_names):
                decision.evaluated.append(
                    {"incident_id": incident.incident_id, "verdict": "never_link", "reasons": ["never_link"]}
                )
                continue

            # 2) Force Link：命中即合并，不看分数，但必须同一节点
            forced_names = [name for name in attached_names if self.rules.force(alert.alertname, name)]
            if forced_names:
                if node_ok:
                    forced = (incident, [f"force_link:{name}" for name in forced_names])
                    break
                decision.evaluated.append(
                    {
                        "incident_id": incident.incident_id,
                        "verdict": "force_link_rejected_different_node",
                        "reasons": [f"alert.node={alert.node} vs incident.node={incident.node}"],
                    }
                )
                continue

            # 3) Causal / 4) Topology / 5) Score
            score, reasons, signals = self._score(alert, incident, attached)
            causal = self.rules.causal_match(alert.alertname, attached_names)
            if causal is not None and causal[0].same_node and not node_ok:
                causal = None  # 因果规则声明了 same_node，跨节点不成立
            role = None
            rule = None
            if causal is not None:
                rule, role = causal
                score += rule.weight
                reasons.append(f"causal_rule:{rule.id}:+{rule.weight}")
                signals.add("causal_rule")

            linkable = self.rules.linkable(signals)
            decision.evaluated.append(
                {
                    "incident_id": incident.incident_id,
                    "score": score,
                    "reasons": reasons,
                    "signals": sorted(signals),
                    "linkable": linkable,
                }
            )
            if linkable and (best is None or score > best[1]):
                best = (incident, score, reasons, signals, {"role": role, "rule": rule})

        if forced is not None:
            incident, reasons = forced
            decision.incident_id = incident.incident_id
            decision.action = "ATTACH_FORCED"
            decision.score = self.rules.threshold
            decision.reasons = reasons
            decision.relation = "SYMPTOM"
            decision.strong = True
            log(logger, logging.INFO, "alert_attached", alert_id=alert.alert_id, incident_id=incident.incident_id, score=decision.score, reasons=reasons)
            return decision

        if best is not None and best[1] >= self.rules.threshold:
            incident, score, reasons, signals, meta = best
            role = meta["role"]
            if role == "cause":
                relation = "ROOT"
            elif role == "symptom":
                relation = "SYMPTOM"
            elif "exact_entity" in signals:
                relation = "SAME_RESOURCE"
            elif "topology" in signals:
                relation = "TOPOLOGY"
            else:
                relation = "UNKNOWN"

            decision.incident_id = incident.incident_id
            decision.action = "ATTACH"
            decision.score = score
            decision.reasons = reasons
            decision.relation = relation
            decision.rule_id = meta["rule"].id if meta["rule"] else None
            decision.strong = True
            log(
                logger,
                logging.INFO,
                "alert_attached",
                alert_id=alert.alert_id,
                incident_id=incident.incident_id,
                score=score,
                reasons=reasons,
                relation=relation,
            )
            return decision

        decision.action = "NEW_INCIDENT"
        if best is not None:
            decision.score = best[1]
            decision.reasons = best[2]
            decision.gated = best[1] >= self.rules.threshold
        log(logger, logging.INFO, "new_incident", alert_id=alert.alert_id, score=decision.score, reasons=decision.reasons)
        return decision
