"""进程内全局锁。

单独成模块是为了避免 pipeline ↔ worker 的循环依赖：
两者都要拿这把锁（webhook 的 DB 段、worker 的写库段），
而 worker 还要调用 pipeline 里的分析实现。

约束（单副本 + SQLite）：外部 I/O 一律不持锁，只包 DB 读-判-写。
"""
from __future__ import annotations

import threading

# 可重入：同一线程内嵌套获取（例如 process_items 里逐条处理）不会自锁
PROCESS_LOCK = threading.RLock()
