"""recovery 核心逻辑测试：本地副本恢复演练。

运行（仓库根目录）：
    /tmp/dvenv/bin/python -I -m pytest tests -q
"""

from __future__ import annotations

import json
import os
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import pytest

from app.recovery import (
    MissingFilesError,
    RestoreConflict,
    RestoreError,
    _write_commit_atomic,
    build_restore_plan,
    execute_restore,
    latest_commit_version,
)

PROTO = {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}
META = {"metaData": {
    "id": "t1",
    "format": {"provider": "parquet", "options": {}},
    "schemaString": "{\"type\":\"struct\",\"fields\":[]}",
    "partitionColumns": [],
    "configuration": {},
}}

ALL_FILES = ["data/o0.parquet", "data/o1.parquet", "data/o2.parquet",
             "data/bad3.parquet", "data/bad4.parquet", "data/o5.parquet"]


def add(path: str, size: int = 1024, **extra) -> dict:
    action = {"add": {"path": path, "partitionValues": {}, "size": size,
                      "modificationTime": 1700000000000, "dataChange": True}}
    action["add"].update(extra)
    return action


def remove(path: str) -> dict:
    return {"remove": {"path": path, "deletionTimestamp": 1700000100000,
                       "dataChange": True}}


def make_table(tmp_path: Path, commits: dict[int, list[dict]],
               files=(), name: str = "orders") -> Path:
    table = tmp_path / name
    log_dir = table / "_delta_log"
    log_dir.mkdir(parents=True)
    for version, actions in commits.items():
        payload = "\n".join(json.dumps(a) for a in actions) + "\n"
        (log_dir / f"{version:020d}.json").write_text(payload, encoding="utf-8")
    for rel in files:
        target = table / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"parquet-bytes:" + rel.encode())
    return table


def orders_commits() -> dict[int, list[dict]]:
    """与样例 bad_orders_table 同构：v2 是错单批次，v3 是错单后的正常变更。"""
    return {
        0: [PROTO, META, add("data/o0.parquet", size=2048), add("data/o1.parquet")],
        1: [add("data/o2.parquet")],
        2: [add("data/bad3.parquet"), add("data/bad4.parquet")],
        3: [add("data/o5.parquet"), remove("data/o0.parquet")],
    }


def read_commit(table: Path, version: int) -> list[dict]:
    path = table / "_delta_log" / f"{version:020d}.json"
    return [json.loads(line) for line in
            path.read_text(encoding="utf-8").splitlines() if line.strip()]


def snapshot(table: Path, upto: int) -> set[str]:
    """测试侧的独立重放：按提交顺序应用 remove/add。"""
    active: set[str] = set()
    for v in range(upto + 1):
        for obj in read_commit(table, v):
            if "remove" in obj:
                active.discard(obj["remove"]["path"])
            if "add" in obj:
                active.add(obj["add"]["path"])
    return active


def tree_digest(root: Path) -> dict[str, bytes]:
    return {str(p.relative_to(root)): p.read_bytes()
            for p in sorted(root.rglob("*")) if p.is_file()}


# ---------- 预检 ----------

def test_plan_happy_path(tmp_path):
    table = make_table(tmp_path, orders_commits(), ALL_FILES)
    plan = build_restore_plan(table, 3, 1)
    assert plan["new_version"] == 4
    assert plan["current_version"] == 3
    assert plan["target_version"] == 1
    assert plan["current_file_count"] == 5   # o1,o2,bad3,bad4,o5
    assert plan["target_file_count"] == 3    # o0,o1,o2
    assert plan["to_remove"] == ["data/bad3.parquet", "data/bad4.parquet",
                                 "data/o5.parquet"]
    assert [a["path"] for a in plan["to_readd"]] == ["data/o0.parquet"]
    # 预检不写任何文件
    assert latest_commit_version(table / "_delta_log") == 3


def test_plan_readd_uses_target_add_fields(tmp_path):
    table = make_table(tmp_path, orders_commits(), ALL_FILES)
    plan = build_restore_plan(table, 3, 1)
    readd = plan["to_readd"][0]
    assert readd["size"] == 2048                       # 目标版 add 的原始字段
    assert readd["modificationTime"] == 1700000000000
    assert readd["dataChange"] is True                 # 但 dataChange 强制为 true


def test_plan_without_metadata_action_ok(tmp_path):
    commits = orders_commits()
    commits[0] = [a for a in commits[0] if "metaData" not in a]
    table = make_table(tmp_path, commits, ALL_FILES)
    plan = build_restore_plan(table, 3, 1)
    assert plan["new_version"] == 4


# ---------- 确认执行 ----------

def test_execute_appends_next_version_matching_target(tmp_path):
    table = make_table(tmp_path, orders_commits(), ALL_FILES)
    result = execute_restore(table, 3, 1, now_ms=1700000200000)
    assert result["new_version"] == 4
    assert result["commit_file"] == f"{4:020d}.json"
    assert result["removed"] == ["data/bad3.parquet", "data/bad4.parquet",
                                 "data/o5.parquet"]
    assert result["readded"] == ["data/o0.parquet"]
    # 新版本快照引用与目标版 v1 完全一致
    assert snapshot(table, 4) == snapshot(table, 1) == {
        "data/o0.parquet", "data/o1.parquet", "data/o2.parquet"}
    assert latest_commit_version(table / "_delta_log") == 4


def test_execute_marks_all_actions_datachange_true(tmp_path):
    table = make_table(tmp_path, orders_commits(), ALL_FILES)
    execute_restore(table, 3, 1)
    for obj in read_commit(table, 4):
        if "add" in obj:
            assert obj["add"]["dataChange"] is True
        if "remove" in obj:
            assert obj["remove"]["dataChange"] is True


def test_execute_readd_carries_target_add_fields(tmp_path):
    table = make_table(tmp_path, orders_commits(), ALL_FILES)
    execute_restore(table, 3, 1)
    adds = [obj["add"] for obj in read_commit(table, 4) if "add" in obj]
    assert len(adds) == 1
    assert adds[0]["path"] == "data/o0.parquet"
    assert adds[0]["size"] == 2048                     # 沿用目标版 add 字段
    assert adds[0]["modificationTime"] == 1700000000000
    assert adds[0]["partitionValues"] == {}
    assert adds[0]["dataChange"] is True


def test_execute_does_not_touch_old_logs_or_parquet(tmp_path):
    table = make_table(tmp_path, orders_commits(), ALL_FILES)
    before = tree_digest(table)
    execute_restore(table, 3, 1)
    after = tree_digest(table)
    new_files = set(after) - set(before)
    assert new_files == {f"_delta_log/{4:020d}.json"}  # 只多出恢复提交
    for name, digest in before.items():
        assert after[name] == digest                   # 旧日志与 Parquet 一律未改


def test_execute_leaves_no_temp_files(tmp_path):
    table = make_table(tmp_path, orders_commits(), ALL_FILES)
    execute_restore(table, 3, 1)
    leftovers = [n for n in os.listdir(table / "_delta_log") if ".tmp" in n]
    assert leftovers == []


def test_execute_noop_when_snapshots_already_equal(tmp_path):
    commits = orders_commits()
    commits[3] = [remove("data/o1.parquet"), add("data/o1.parquet")]  # 自抵消
    table = make_table(tmp_path, commits, ALL_FILES)
    plan = build_restore_plan(table, 3, 2)
    assert plan["to_remove"] == [] and plan["to_readd"] == []
    result = execute_restore(table, 3, 2)
    assert result["actions_written"] == 0
    assert snapshot(table, 4) == snapshot(table, 2)


# ---------- 约束拒绝（不写任何文件）----------

def test_gap_in_commits_rejected(tmp_path):
    table = make_table(tmp_path, orders_commits(), ALL_FILES)
    (table / "_delta_log" / f"{2:020d}.json").unlink()
    with pytest.raises(RestoreError, match="不连续"):
        build_restore_plan(table, 3, 1)
    assert latest_commit_version(table / "_delta_log") == 3


def test_protocol_version_rejected(tmp_path):
    commits = orders_commits()
    commits[0][0] = {"protocol": {"minReaderVersion": 2, "minWriterVersion": 5}}
    table = make_table(tmp_path, commits, ALL_FILES)
    with pytest.raises(RestoreError, match="协议"):
        build_restore_plan(table, 3, 1)


def test_protocol_extra_features_rejected(tmp_path):
    commits = orders_commits()
    commits[0][0] = {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2,
                                  "writerFeatures": ["appendOnly"]}}
    table = make_table(tmp_path, commits, ALL_FILES)
    with pytest.raises(RestoreError, match="额外特性"):
        build_restore_plan(table, 3, 1)


def test_missing_protocol_action_rejected(tmp_path):
    commits = orders_commits()
    commits[0] = [a for a in commits[0] if "protocol" not in a]
    table = make_table(tmp_path, commits, ALL_FILES)
    with pytest.raises(RestoreError, match="protocol"):
        build_restore_plan(table, 3, 1)


def test_partitioned_table_rejected(tmp_path):
    commits = orders_commits()
    commits[0][1] = {"metaData": {**META["metaData"],
                                  "partitionColumns": ["date"]}}
    table = make_table(tmp_path, commits, ALL_FILES)
    with pytest.raises(RestoreError, match="分区表"):
        build_restore_plan(table, 3, 1)


def test_add_with_partition_values_rejected(tmp_path):
    commits = orders_commits()
    commits[1] = [add("data/o2.parquet", partitionValues={"date": "2024-01-01"})]
    table = make_table(tmp_path, commits, ALL_FILES)
    with pytest.raises(RestoreError, match="分区值"):
        build_restore_plan(table, 3, 1)


def test_metadata_changed_rejected(tmp_path):
    commits = orders_commits()
    changed = {"metaData": {**META["metaData"], "schemaString": "{\"v\":2}"}}
    commits[2] = commits[2] + [changed]
    table = make_table(tmp_path, commits, ALL_FILES)
    with pytest.raises(RestoreError, match="元数据"):
        build_restore_plan(table, 3, 1)


def test_deletion_vector_rejected(tmp_path):
    commits = orders_commits()
    dv = {"storageType": "u", "pathOrInlineDv": "abc"}
    commits[1] = [add("data/o2.parquet", deletionVector=dv)]
    table = make_table(tmp_path, commits, ALL_FILES)
    with pytest.raises(RestoreError, match="删除向量"):
        build_restore_plan(table, 3, 1)


def test_path_traversal_in_log_rejected(tmp_path):
    commits = orders_commits()
    commits[1] = [add("../escape.parquet")]
    table = make_table(tmp_path, commits, ALL_FILES)
    with pytest.raises(RestoreError, match=r"\.\."):
        build_restore_plan(table, 3, 1)


# ---------- 缺件：列出路径，不写日志 ----------

def test_missing_target_files_listed_and_nothing_written(tmp_path):
    table = make_table(tmp_path, orders_commits(), ALL_FILES)
    (table / "data/o1.parquet").unlink()
    (table / "data/o2.parquet").unlink()
    before = tree_digest(table)
    with pytest.raises(MissingFilesError) as exc_info:
        build_restore_plan(table, 3, 1)
    assert exc_info.value.paths == ["data/o1.parquet", "data/o2.parquet"]
    with pytest.raises(MissingFilesError):
        execute_restore(table, 3, 1)
    assert tree_digest(table) == before               # 未写任何日志


# ---------- 当前版与并发 ----------

def test_current_version_changed_rejected(tmp_path):
    table = make_table(tmp_path, orders_commits(), ALL_FILES)
    with pytest.raises(RestoreConflict, match="当前版已变"):
        build_restore_plan(table, 2, 1)               # 实际最新已是 v3
    with pytest.raises(RestoreConflict, match="当前版已变"):
        execute_restore(table, 2, 1)
    assert latest_commit_version(table / "_delta_log") == 3


def test_same_current_version_succeeds_at_most_once(tmp_path):
    table = make_table(tmp_path, orders_commits(), ALL_FILES)
    execute_restore(table, 3, 1)
    with pytest.raises(RestoreConflict):
        execute_restore(table, 3, 1)                  # 同一当前版第二笔被拒
    commits = [n for n in os.listdir(table / "_delta_log") if n.endswith(".json")]
    assert sorted(commits) == [f"{v:020d}.json" for v in range(5)]


def _execute_in_process(table: str, current: int, target: int) -> str:
    try:
        execute_restore(table, current, target)
        return "ok"
    except RestoreConflict:
        return "conflict"
    except RestoreError as exc:                        # pragma: no cover
        return f"error:{exc}"


def test_concurrent_processes_single_success(tmp_path):
    """确认请求落在不同服务进程：同一当前版最多成功一笔。"""
    table = make_table(tmp_path, orders_commits(), ALL_FILES)
    with ProcessPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(_execute_in_process, str(table), 3, 1)
                   for _ in range(8)]
        results = [f.result() for f in futures]
    assert results.count("ok") == 1
    assert results.count("conflict") == 7
    assert latest_commit_version(table / "_delta_log") == 4
    assert snapshot(table, 4) == snapshot(table, 1)


def test_atomic_write_refuses_to_replace(tmp_path):
    table = make_table(tmp_path, orders_commits(), ALL_FILES)
    log_dir = table / "_delta_log"
    final = _write_commit_atomic(log_dir, 4, b'{"add": {}}\n')
    assert final.read_bytes() == b'{"add": {}}\n'
    with pytest.raises(RestoreConflict):
        _write_commit_atomic(log_dir, 4, b'{"remove": {}}\n')
    assert final.read_bytes() == b'{"add": {}}\n'      # 原提交未被覆盖
    assert [n for n in os.listdir(log_dir) if ".tmp" in n] == []


# ---------- 版本号校验 ----------

def test_target_must_be_older_than_current(tmp_path):
    table = make_table(tmp_path, orders_commits(), ALL_FILES)
    with pytest.raises(RestoreError, match="早于当前版"):
        build_restore_plan(table, 3, 3)
    with pytest.raises(RestoreError, match="早于当前版"):
        build_restore_plan(table, 3, 4)
    with pytest.raises(RestoreError, match="负数"):
        build_restore_plan(table, 3, -1)
    with pytest.raises(RestoreError, match="整数"):
        build_restore_plan(table, "3", 1)


def test_table_without_delta_log_rejected(tmp_path):
    table = tmp_path / "not_a_table"
    table.mkdir()
    with pytest.raises(RestoreError, match="_delta_log"):
        build_restore_plan(table, 1, 0)


def test_table_without_commits_rejected(tmp_path):
    table = make_table(tmp_path, {}, ALL_FILES)
    with pytest.raises(RestoreError, match="没有任何 JSON 提交"):
        build_restore_plan(table, 1, 0)
