"""测试环境准备。

必须在导入 app 之前设置环境变量：config.settings 在 import 时解析一次。
默认关闭所有外部依赖（Prometheus / K8s / DeepSeek / 飞书），
这样测试验证的是「降级路径也能走通」，同时不产生任何外部影响。
"""
from __future__ import annotations

import os
import shutil
import tempfile

_TMP_ROOT = tempfile.mkdtemp(prefix="aiops-test-")
os.environ["AIOPS_DATA_DIR"] = _TMP_ROOT
os.environ["AIOPS_DB_PATH"] = os.path.join(_TMP_ROOT, "aiops-test.db")
os.environ["AIOPS_ALERT_STALE_SECONDS"] = "900"
os.environ["AIOPS_RECOVERY_OBSERVE_SECONDS"] = "300"
os.environ["AIOPS_SWEEP_INTERVAL_SECONDS"] = "3600"
os.environ["AIOPS_MASK_ASSETS"] = "true"
# 测试里让采集/诊断/推卡同步执行：绝大多数用例断言的是「处理完之后」的状态。
# 异步（副作用出锁）行为由 test_async_* 用例显式打开 settings.inline_analysis=False 覆盖。
os.environ["AIOPS_INLINE_ANALYSIS"] = "true"
for key in (
    "AIOPS_PROMETHEUS_URL",
    "AIOPS_K8S_API_URL",
    "AIOPS_K8S_TOKEN",
    "DEEPSEEK_API_KEY",
    "AIOPS_FEISHU_EVENT_WEBHOOK",
    "AIOPS_FEISHU_INCIDENT_WEBHOOK",
):
    os.environ.pop(key, None)

import pytest  # noqa: E402

from app.db.session import SessionLocal, engine, init_db  # noqa: E402


@pytest.fixture(scope="session", autouse=True)
def _prepare_db():
    init_db()
    yield
    engine.dispose()
    shutil.rmtree(_TMP_ROOT, ignore_errors=True)


@pytest.fixture(autouse=True)
def _reset_state(_prepare_db):
    """每个测试用例前清空所有表，保证计数断言是确定的。"""
    from app.db.base import Base

    with engine.begin() as connection:
        for table in reversed(Base.metadata.sorted_tables):
            connection.execute(table.delete())
    yield


@pytest.fixture()
def session():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@pytest.fixture()
def client():
    from fastapi.testclient import TestClient

    from app.main import app

    with TestClient(app) as test_client:
        yield test_client
