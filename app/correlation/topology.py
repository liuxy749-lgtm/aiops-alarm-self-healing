"""拓扑服务：一期用关系表，不用图数据库。

关系模型统一为 SOURCE --RELATION--> TARGET，例如：
    pod-a     --RUNS_ON-->      node01
    gpu0      --BELONGS_TO-->   node01
    node01    --MEMBER_OF-->    cluster-a
    job-a     --USES-->         pod-a

一次请求内会反复问同一个实体的邻居（关联评分 + 工单合并 + 上下文采集），
所以这里做了 per-session 邻接缓存，避免同一实体的重复 SQL。
"""
from __future__ import annotations

from sqlalchemy import or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.db.models import ResourceRelation


class TopologyService:
    def __init__(self, session: Session) -> None:
        self.session = session
        self._neighbors: dict[tuple[str, str], set[tuple[str, str]]] = {}

    @property
    def _pending(self) -> set[tuple[str, str, str, str, str]]:
        """本 session 内已 add 但还没 flush 的关系键。

        必须记在 session.info 上而不是实例属性：同一个 session 会被多个
        TopologyService 实例使用（富化里指标侧和 K8s API 侧各建一个），
        实例级的集合去不了重。实测踩过：kube-state-metrics 与 K8s API 都会登记
        Pod RUNS_ON Node，autoflush=False 让 upsert 的 SELECT 看不到未 flush 的那条，
        结果重复 add，下一次 flush 撞 resource_relations 的唯一约束，
        整个事件处理失败（Can't operate on closed transaction inside context manager）。
        """
        return self.session.info.setdefault("topology_pending_keys", set())

    def neighbors(self, entity_type: str, entity_id: str) -> set[tuple[str, str]]:
        """双向邻居集合（带 per-session 缓存）。"""
        key = (entity_type, entity_id)
        cached = self._neighbors.get(key)
        if cached is not None:
            return cached

        rows = self.session.scalars(
            select(ResourceRelation).where(
                or_(
                    (ResourceRelation.source_type == entity_type) & (ResourceRelation.source_id == entity_id),
                    (ResourceRelation.target_type == entity_type) & (ResourceRelation.target_id == entity_id),
                )
            )
        ).all()
        result: set[tuple[str, str]] = set()
        for row in rows:
            if row.source_type == entity_type and row.source_id == entity_id:
                result.add((row.target_type, row.target_id))
            else:
                result.add((row.source_type, row.source_id))
        self._neighbors[key] = result
        return result

    def distance(self, a: tuple[str, str], b: tuple[str, str]) -> int | None:
        """1 / 2 跳关系；不相连返回 None。"""
        if a == b:
            return 0
        a_neighbors = self.neighbors(*a)
        if b in a_neighbors:
            return 1
        if a_neighbors & self.neighbors(*b):
            return 2
        return None

    def upsert(
        self,
        source_type: str,
        source_id: str,
        relation: str,
        target_type: str,
        target_id: str,
        metadata: dict | None = None,
    ) -> None:
        key = (source_type, source_id, relation, target_type, target_id)
        # ① 本 session 内重复登记（未 flush 的那条 SELECT 也看不见）→ 直接跳过
        if key in self._pending:
            return
        existing = self.session.scalar(
            select(ResourceRelation).where(
                ResourceRelation.source_type == source_type,
                ResourceRelation.source_id == source_id,
                ResourceRelation.relation == relation,
                ResourceRelation.target_type == target_type,
                ResourceRelation.target_id == target_id,
            )
        )
        if existing is not None:
            if metadata:
                existing.metadata_json = metadata
            return
        # ② 跨会话并发插入同一关系：用 savepoint 兜住唯一约束冲突，不让它毁掉整个事件
        try:
            with self.session.begin_nested():
                self.session.add(
                    ResourceRelation(
                        source_type=source_type,
                        source_id=source_id,
                        relation=relation,
                        target_type=target_type,
                        target_id=target_id,
                        metadata_json=metadata,
                    )
                )
        except IntegrityError:
            return
        self._pending.add(key)
        # 新增关系会让缓存失真，直接失效，避免后续判断用旧拓扑
        self._neighbors.clear()
