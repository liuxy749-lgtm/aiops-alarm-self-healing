#!/usr/bin/env python3
"""补录工具：把 raw_events 归档里「有原文但没入库」的事件重新投回网关。

用途：Webhook 处理失败（如 database is locked）时，事件不会入库，
夜莺也不一定重投 → 这条告警就永久丢了（没工单、没诊断、没人知道）。
但归档文件是先写文件的、**写失败不会回滚**，所以原文通常还在
`data/raw_events/raw-events-YYYY-MM-DD.jsonl` 里，可以补录重放。

用法（在项目根目录执行）：
  # 1) 只报告，不动任何数据（默认）
  env -u PYTHONHOME -u PYTHONPATH .venv/bin/python tools/backfill_raw_events.py

  # 2) 真的补投（走本地网关入口，服务端有幂等，重复投递安全）
  env -u PYTHONHOME -u PYTHONPATH .venv/bin/python tools/backfill_raw_events.py --apply

  # 指定网关地址 / 数据目录
  ... tools/backfill_raw_events.py --url http://127.0.0.1:8701 --data-dir data
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sqlite3
import sys
import urllib.error
import urllib.request
from collections import OrderedDict

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.services import normalizer  # noqa: E402


def archived_payloads(data_dir: pathlib.Path) -> OrderedDict[str, dict]:
    """扫归档，返回 event_id → payload（同事件多行时保留第一条）。"""
    found: OrderedDict[str, dict] = OrderedDict()
    duplicates = 0
    files = sorted((data_dir / "raw_events").glob("*.jsonl"))
    for path in files:
        for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except ValueError:
                print(f"  ⚠️ {path.name}:{line_no} 不是合法 JSON，跳过")
                continue
            payload = record.get("payload") if isinstance(record, dict) else None
            if not isinstance(payload, dict):
                continue
            try:
                dedup_key = normalizer.build_dedup_key(payload)
                event_id = normalizer.derive_event_id(payload, dedup_key)
            except Exception as exc:  # 归档里可能有历史格式
                print(f"  ⚠️ {path.name}:{line_no} 无法计算 event_id（{exc}），跳过")
                continue
            if event_id in found:
                duplicates += 1
                continue
            found[event_id] = {**payload, "_archive_file": path.name, "_archive_line": line_no}
    if duplicates:
        print(f"  （归档里有 {duplicates} 行是同一事件的重复留档，已忽略）")
    return found


def stored_event_ids(db_path: pathlib.Path) -> set[str]:
    if not db_path.exists():
        return set()
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        return {row[0] for row in conn.execute("SELECT event_id FROM raw_events")}
    finally:
        conn.close()


def describe(payload: dict) -> str:
    alert = payload.get("rule_name") or payload.get("alertname") or "-"
    # 夜莺原生 payload：目标在 tags 里（target_ident 常为空）
    target = payload.get("target_ident") or "-"
    tags = payload.get("tags") or []
    if isinstance(tags, list):
        for item in tags:
            text = str(item)
            if text.startswith("instance=") or text.startswith("node=") or text.startswith("host="):
                target = text.split("=", 1)[1].strip('"')
                break
    group = payload.get("group_name") or payload.get("cluster") or "-"
    return f"{alert} | 目标 {target} | 业务组 {group}"


def post(url: str, payload: dict, timeout: float = 30.0) -> tuple[bool, str]:
    clean = {key: value for key, value in payload.items() if not key.startswith("_archive_")}
    request = urllib.request.Request(
        url,
        data=json.dumps(clean).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = json.loads(response.read() or b"{}")
            return True, f"HTTP {response.status} processed={body.get('processed')} duplicated={body.get('duplicated')}"
    except urllib.error.HTTPError as exc:  # 422 等
        return False, f"HTTP {exc.code} {exc.read()[:200].decode('utf-8', 'replace')}"
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def main() -> int:
    parser = argparse.ArgumentParser(description="把归档里有原文、但未入库的事件补投回网关")
    parser.add_argument("--apply", action="store_true", help="真的补投（默认只报告）")
    parser.add_argument("--url", default="http://127.0.0.1:8701", help="网关地址")
    parser.add_argument("--data-dir", default=str(ROOT / "data"), help="数据目录（含 raw_events/ 与 aiops.db）")
    args = parser.parse_args()

    data_dir = pathlib.Path(args.data_dir)
    db_path = data_dir / "aiops.db"

    print(f"归档目录: {data_dir / 'raw_events'}")
    print(f"状态库  : {db_path}")
    archived = archived_payloads(data_dir)
    stored = stored_event_ids(db_path)
    missing = {event_id: payload for event_id, payload in archived.items() if event_id not in stored}

    print(f"\n归档事件 {len(archived)} 个，库里 {len(stored)} 个，**缺失 {len(missing)} 个**")
    for event_id, payload in missing.items():
        where = f"{payload['_archive_file']}:{payload['_archive_line']}"
        print(f"  {event_id}  {describe(payload)}  ← {where}")

    if not missing:
        print("\n没有需要补录的事件 ✓")
        return 0
    if not args.apply:
        print("\n（这是只读报告；确认无误后加 --apply 真正补投）")
        return 1

    print(f"\n开始补投到 {args.url} ...")
    ok_count = fail_count = 0
    for event_id, payload in missing.items():
        ok, detail = post(f"{args.url.rstrip('/')}/api/v1/events/nightingale", payload)
        print(f"  {'✅' if ok else '❌'} {event_id}  {detail}")
        ok_count += ok
        fail_count += not ok
    print(f"\n补投完成：成功 {ok_count}，失败 {fail_count}")
    print("提示：入库后可在 /api/v1/incidents 看到对应工单；若仍是 DUPLICATE 说明此前已入库。")
    return 0 if fail_count == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
