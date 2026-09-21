"""SQLite 会话管理。

一期单副本 + SQLite：写操作由进程内全局锁串行化，数据库侧再用
部分唯一索引做硬兜底。真要开多副本，必须换 PostgreSQL。
"""
from __future__ import annotations

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from app.config import settings
from app.db.base import Base

engine = create_engine(
    settings.sqlite_url,
    future=True,
    connect_args={"check_same_thread": False, "timeout": 30},
)


@event.listens_for(engine, "connect")
def _sqlite_pragmas(dbapi_connection, _record) -> None:  # pragma: no cover - 驱动回调
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.execute("PRAGMA busy_timeout=30000")
    cursor.close()


SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)

# 后台任务专用会话：AUTOCOMMIT（每条语句立即提交）。
# 原因：后台要跨 HTTP（Prometheus/K8s/LLM，10 余秒）干活，如果放在一个事务里，
# 这个连接会一直持有读快照，回写时升级成写锁会与 webhook 的写冲突，
# 而 SQLite 在「已读过的连接升级写」场景下会**立即**返回 database is locked
# （busy_timeout 不生效），把 webhook 的告警入库打挂（曾出现过：过）。
# 后台任务每一步都是独立语句，不需要跨语句原子性；进度由 analysis_status 记录。
# 注意：isolation_level 是连接/引擎级选项，只能通过 execution_options 传，不能给 sessionmaker。
WorkerSessionLocal = sessionmaker(
    bind=engine.execution_options(isolation_level="AUTOCOMMIT"),
    autoflush=False,
    expire_on_commit=False,
    future=True,
)


def _ensure_columns() -> None:
    """SQLite 轻量迁移：Base.metadata.create_all() 只建表，不会给已有表加列。

    每加一次字段就得同步这里，否则老库升级上来会报 no such column。
    """
    wanted = {
        "incidents": {
            "analysis_status": "VARCHAR(16) DEFAULT 'NONE'",
            "analysis_kind": "VARCHAR(32)",
            "analysis_attempts": "INTEGER DEFAULT 0",
            "analysis_error": "TEXT",
            "analysis_updated_at": "DATETIME",
            "last_probe_at": "DATETIME",
            "last_probe_result": "VARCHAR(32)",
        }
    }
    with engine.begin() as connection:
        for table, columns in wanted.items():
            existing = {row[1] for row in connection.exec_driver_sql(f"PRAGMA table_info({table})")}
            if not existing:
                continue  # 新库：create_all 已经建好了
            for name, ddl in columns.items():
                if name not in existing:
                    connection.exec_driver_sql(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")
        # 索引也要单独补（create_all 不会给已存在的表加索引）
        connection.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_incidents_analysis_status ON incidents (analysis_status)"
        )


def init_db() -> None:
    settings.ensure_dirs()
    from app.db import models  # noqa: F401  确保模型已注册

    Base.metadata.create_all(engine)
    _ensure_columns()
