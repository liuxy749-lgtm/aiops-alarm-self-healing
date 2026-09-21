"""Kubernetes 只读客户端。

安全边界：只有 GET / LIST，没有任何写操作；ServiceAccount 走最小权限 RBAC。
未配置时不报错，只标记 PARTIAL。客户端做成进程级单例（复用连接、避免 fd 泄漏）。
"""
from __future__ import annotations

import logging
import threading
from typing import Any

import httpx

from app.config import settings
from app.logging_setup import get_logger, log

logger = get_logger("app.integrations.kubernetes")


class KubernetesClient:
    def __init__(self) -> None:
        self.base_url = (settings.k8s_api_url or "").rstrip("/")
        self.token = settings.k8s_token
        self.verify: bool | str = settings.k8s_ca_file or settings.k8s_verify
        self._client: httpx.Client | None = None

    @property
    def configured(self) -> bool:
        return bool(self.base_url and self.token)

    def _get_client(self) -> httpx.Client | None:
        if not self.configured:
            return None
        if self._client is None:
            self._client = httpx.Client(
                base_url=self.base_url,
                headers={"Authorization": f"Bearer {self.token}"},
                verify=self.verify,
                timeout=settings.k8s_timeout,
            )
        return self._client

    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any] | None:
        client = self._get_client()
        if client is None:
            return None
        try:
            response = client.get(path, params=params)
            response.raise_for_status()
            return response.json()
        except Exception as exc:  # 外部依赖异常不能影响主流程
            log(logger, logging.WARNING, "k8s_request_failed", path=path, error=str(exc))
            return None

    def node_facts(self, node_name: str) -> dict[str, Any]:
        node = self._get(f"/api/v1/nodes/{node_name}")
        if not node:
            return {}
        conditions = {item.get("type"): item.get("status") for item in (node.get("status", {}).get("conditions") or [])}
        return {
            "name": node_name,
            "conditions": conditions,
            "ready": conditions.get("Ready"),
            "unschedulable": bool(node.get("spec", {}).get("unschedulable")),
            "taints": [taint.get("key") for taint in (node.get("spec", {}).get("taints") or [])],
            "kubelet_version": node.get("status", {}).get("nodeInfo", {}).get("kubeletVersion"),
        }

    def pods_on_node(self, node_name: str, limit: int = 200) -> dict[str, Any]:
        data = self._get("/api/v1/pods", params={"fieldSelector": f"spec.nodeName={node_name}", "limit": limit})
        if not data:
            return {}
        items = data.get("items") or []
        not_ready = [
            pod.get("metadata", {}).get("name")
            for pod in items
            if any(not status.get("ready", False) for status in pod.get("status", {}).get("containerStatuses") or [])
        ]
        return {"pod_count": len(items), "not_ready": not_ready[:50], "not_ready_count": len(not_ready)}

    def pod_facts(self, namespace: str, pod_name: str) -> dict[str, Any]:
        pod = self._get(f"/api/v1/namespaces/{namespace}/pods/{pod_name}")
        if not pod:
            return {}
        statuses = pod.get("status", {}).get("containerStatuses") or []
        waiting = [
            status.get("state", {}).get("waiting", {}).get("reason") for status in statuses if status.get("state", {}).get("waiting")
        ]
        return {
            "namespace": namespace,
            "name": pod_name,
            "node": pod.get("spec", {}).get("nodeName"),
            "phase": pod.get("status", {}).get("phase"),
            "restarts": sum(status.get("restartCount", 0) for status in statuses),
            "waiting_reason": waiting[0] if waiting else None,
            "owned_by": [
                {"kind": owner.get("kind"), "name": owner.get("name")}
                for owner in (pod.get("metadata", {}).get("ownerReferences") or [])
            ],
        }

    def events_on_node(self, node_name: str, limit: int = 30) -> list[dict[str, Any]]:
        data = self._get("/api/v1/events", params={"fieldSelector": f"involvedObject.name={node_name}", "limit": limit})
        if not data:
            return []
        return [
            {
                "reason": item.get("reason"),
                "type": item.get("type"),
                "message": (item.get("message") or "")[:300],
                "count": item.get("count"),
            }
            for item in (data.get("items") or [])
        ]


_client_lock = threading.Lock()
_shared_client: KubernetesClient | None = None


def get_client() -> KubernetesClient:
    """进程级单例：避免每次调用新建 httpx.Client 导致连接/fd 泄漏。"""
    global _shared_client
    if _shared_client is None:
        with _client_lock:
            if _shared_client is None:
                _shared_client = KubernetesClient()
    return _shared_client
