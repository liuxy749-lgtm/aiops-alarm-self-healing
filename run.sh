#!/usr/bin/env bash
# 启动 AIOps Gateway（单副本）
#
# 注意：
#   1. 必须单 worker —— SQLite + 进程内锁，多副本会导致状态不一致与重复通知
#   2. 本机 PYTHONHOME/PYTHONPATH 被 Hermes runtime 污染，必须 unset，否则 python 起不来
set -euo pipefail
cd "$(dirname "$0")"

unset PYTHONHOME PYTHONPATH

export AIOPS_PORT="${AIOPS_PORT:-8701}"
# 数据目录默认在仓库下 ./data，可覆盖为 /data/aiops-platform
export AIOPS_DATA_DIR="${AIOPS_DATA_DIR:-$(pwd)/data}"

echo "[aiops-gateway] data_dir=${AIOPS_DATA_DIR} port=${AIOPS_PORT}"
exec .venv/bin/uvicorn app.main:app \
  --host 0.0.0.0 \
  --port "${AIOPS_PORT}" \
  --workers 1 \
  --log-level info
