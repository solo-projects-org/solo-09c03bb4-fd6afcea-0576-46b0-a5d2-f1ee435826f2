"""恢复演练核心逻辑测试：追加提交、文件缺件、当前版冲突与并发。"""

from __future__ import annotations

import json
import threading
from pathlib import Path

import pytest

from app.analyzer import analyze, InputFile
from app.recovery import (
    ERROR_MISSING_PARQUET,
    ERROR_STALE_CURRENT_VERSION,
    RecoveryError,
    recover_table,
)


# ---------- 构造工具 ----------

def commit_name(version: int) -> str:
    return f"{version:020d}.json"


def add_action(path: str) -> dict:
    return {"add": {"path": path, "partitionValues": {}, "size": 1024,
                    "modificationTime": 1700000000000, "dataChange": True}}


def remove_action(path: str) -> dict:
    return {"remove": {"path": path, "deletionTimestamp": 1700000100000,
                       "dataChange": True}}


def metadata_action(partition_columns: list[str] | None = None) -> dict:
    return {"metaData": {
        "id": "table-id",
        "format": {"provider": "parquet", "options": {}},
        "schemaString": '{"type":"struct","fields":[]}',
        "partitionColumns": partition_columns or [],
        "configuration": {},
        "createdTime": 1700000000000,
    }}


PROTOCOL = {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}


def write_commit(log_dir: Path, version: int, actions: list[dict]) -> None:
    lines = [json.dumps(a) for a in actions]
    (log_dir / commit_name(version)).write_text(
        "\n".join(lines) + "\n", encoding="utf-8")


def make_table(root: Path, versions: list[list[dict]],
               data_files: list[str], *, with_metadata: bool = True,
               protocol: dict | None = None) -> Path:
    """建一张本地副本：每条提交一组动作，data_files 为实际落盘的 Parquet 路径。"""
    log_dir = root / "_delta_log"
    log_dir.mkdir(parents=True)
    for i, actions in enumerate(versions):
        prefix = []
        if i == 0:
            prefix.append(protocol or PROTOCOL)
            if with_metadata:
                prefix.append(metadata_action())
        write_commit(log_dir, i, prefix + actions)
    for path in data_files:
        target = root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"sample parquet bytes")
    (root / "existing_files.txt").write_text(
        "\n".join(data_files) + "\n", encoding="utf-8")
    return root


def f(n: int) -> str:
    return f"data/part-{n:05d}.parquet"


def drill_table(root: Path) -> Path:
    """v0 a0,a1 -> v1 a2 -> v2 a3 -a1 -> v3 a4 -a0；当前 v3。"""
    versions = [
        [add_action(f(0)), add_action(f(1))],
        [add_action(f(2))],
        [add_action(f(3)), remove_action(f(1))],
        [add_action(f(4)), remove_action(f(0))],
    ]
    return make_table(root, versions, [f(i) for i in range(5)])


def read_commit(table_dir: Path, version: int) -> list[dict]:
    text = (table_dir / "_delta_log" / commit_name(version)).read_text("utf-8")
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def analyze_table(table_dir: Path, from_version: int, to_version: int) -> dict:
    log_files = [InputFile(filename=p.name, data=p.read_bytes())
                 for p in sorted((table_dir / "_delta_log").iterdir())
                 if p.is_file()]
    manifest = InputFile(
        filename="existing_files.txt",
        data=(table_dir / "existing_files.txt").read_bytes())
    return analyze(log_files, [manifest], from_version, to_version)


# ---------- 正常恢复 ----------

def test_restore_to_older_version_appends_next_commit(tmp_path):
    table = drill_table(tmp_path / "t")
    before = {p.name: p.read_bytes()
              for p in (table / "_delta_log").iterdir()
              if p.name.endswith(".json")}

    result = recover_table(table, 3, 2)

    assert result["new_version"] == 4
    assert result["commit_file"] == commit_name(4)
    # v3 快照 {a2,a3,a4} -> v2 快照 {a0,a2,a3}：应 remove a4、重新 add a0
    assert result["removed_files"] == [f(4)]
    assert result["re_added_files"] == [f(0)]
    assert result["referenced_files"] == sorted([f(0), f(2), f(3)])

    actions = read_commit(table, 4)
    removes = [a["remove"]["path"] for a in actions if "remove" in a]
    adds = [a["add"]["path"] for a in actions if "add" in a]
    assert removes == [f(4)]
    assert adds == [f(0)]
    for action in actions:
        if "remove" in action:
            assert action["remove"]["dataChange"] is True
        if "add" in action:
            assert action["add"]["dataChange"] is True

    # 旧日志逐字节未变
    for name, payload in before.items():
        assert (table / "_delta_log" / name).read_bytes() == payload
    # Parquet 未变
    assert (table / f(4)).read_bytes() == b"sample parquet bytes"


def test_re_add_keeps_target_add_fields(tmp_path):
    # v0: add a0；v1: remove a0（物理文件仍在）
    make_table(tmp_path / "t", [
        [add_action(f(0))],
        [remove_action(f(0))],
    ], [f(0)])
    table = tmp_path / "t"
    result = recover_table(table, 1, 0)
    assert result["re_added_files"] == [f(0)]
    v2_adds = [a["add"] for a in read_commit(table, 2) if "add" in a]
    assert v2_adds[0] == add_action(f(0))["add"]


def test_kept_files_are_not_rewritten(tmp_path):
    table = drill_table(tmp_path / "t")
    result = recover_table(table, 3, 1)
    # v3 快照 {a2,a3,a4} -> v1 快照 {a0,a1,a2}：重新纳入 a0/a1，移出 a3/a4
    assert result["referenced_files"] == sorted([f(0), f(1), f(2)])
    assert result["re_added_files"] == [f(0), f(1)]
    assert result["removed_files"] == [f(3), f(4)]
    assert result["kept_files"] == [f(2)]
    # 重新核查新版本：引用文件集合与目标版本一致
    verify = analyze_table(table, result["target_version"],
                           result["new_version"])
    assert verify["versions"][-1]["referenced_files"] == sorted([f(0), f(1), f(2)])


def test_readd_reuses_original_encoded_path(tmp_path):
    # Delta 日志里的 add path 常是 URI 编码的；重新纳入应沿用目标版原始写法
    encoded = "data/part%20with%20space.parquet"
    table = make_table(tmp_path / "t", [
        [{"add": {"path": encoded, "size": 1, "dataChange": True}}],
        [remove_action(encoded), add_action(f(0))],
    ], ["data/part with space.parquet", f(0)])
    result = recover_table(table, 1, 0)
    assert result["removed_files"] == [f(0)]
    assert result["re_added_files"] == ["data/part with space.parquet"]
    actions = read_commit(table, 2)
    added = [a["add"]["path"] for a in actions if "add" in a]
    assert added == [encoded]
    removes = [a["remove"]["path"] for a in actions if "remove" in a]
    assert removes == [f(0)]


def test_remove_action_contains_current_encoded_path(tmp_path):
    encoded = "data/part%20space.parquet"
    table = make_table(tmp_path / "t", [
        [add_action(f(0))],
        [remove_action(f(0)), {"add": {"path": encoded, "size": 1,
                                       "dataChange": True}}],
    ], [f(0), "data/part space.parquet"])
    result = recover_table(table, 1, 0)
    assert result["removed_files"] == ["data/part space.parquet"]
    actions = read_commit(table, 2)
    removes = [a["remove"]["path"] for a in actions if "remove" in a]
    assert removes == [encoded]
    adds = [a["add"]["path"] for a in actions if "add" in a]
    assert adds == [f(0)]


def test_restore_twice_requires_new_current_version(tmp_path):
    table = drill_table(tmp_path / "t")
    recover_table(table, 3, 2)
    with pytest.raises(RecoveryError) as excinfo:
        recover_table(table, 3, 2)
    assert excinfo.value.code == ERROR_STALE_CURRENT_VERSION
    assert not (table / "_delta_log" / commit_name(5)).exists()


def test_restore_current_version_conflict(tmp_path):
    table = drill_table(tmp_path / "t")
    with pytest.raises(RecoveryError) as excinfo:
        recover_table(table, 2, 1)
    err = excinfo.value
    assert err.code == ERROR_STALE_CURRENT_VERSION
    assert err.actual_current_version == 3
    assert not (table / "_delta_log" / commit_name(4)).exists()


def test_target_must_be_older_than_current(tmp_path):
    table = drill_table(tmp_path / "t")
    with pytest.raises(RecoveryError, match="高于当前版本"):
        recover_table(table, 3, 5)
    with pytest.raises(RecoveryError, match="无需恢复"):
        recover_table(table, 3, 3)


# ---------- 缺件：列出路径，不写日志 ----------

def test_missing_parquet_lists_paths_and_writes_nothing(tmp_path):
    table = drill_table(tmp_path / "t")
    (table / f(0)).unlink()  # 目标 v2 仍引用 a0
    existed = sorted(p.name for p in (table / "_delta_log").iterdir())

    with pytest.raises(RecoveryError) as excinfo:
        recover_table(table, 3, 2)
    err = excinfo.value
    assert err.code == ERROR_MISSING_PARQUET
    assert err.missing_files == [f(0)]
    # 不写任何日志文件
    assert sorted(p.name for p in (table / "_delta_log").iterdir()) == existed
    assert not (table / "_delta_log" / commit_name(4)).exists()


def test_multiple_missing_parquet_paths_all_listed(tmp_path):
    # a1 本应在 v0 快照中，a2 在 v1 被 remove 后仍物理缺失；两者都属于目标 v0 快照
    table = make_table(tmp_path / "t", [
        [add_action(f(0)), add_action(f(1)), add_action(f(2))],
        [add_action(f(3)), remove_action(f(2))],
    ], [f(0), f(3)])
    with pytest.raises(RecoveryError) as excinfo:
        recover_table(table, 1, 0)
    assert excinfo.value.missing_files == [f(1), f(2)]

def test_path_outside_table_treated_missing(tmp_path):
    table = make_table(tmp_path / "t", [
        [{"add": {"path": "../outside.parquet", "size": 1, "dataChange": True}}],
        [remove_action("../outside.parquet")],
    ], [])
    (tmp_path / "outside.parquet").write_bytes(b"x")
    with pytest.raises(RecoveryError) as excinfo:
        recover_table(table, 1, 0)
    assert excinfo.value.code == ERROR_MISSING_PARQUET
    assert excinfo.value.missing_files == ["../outside.parquet"]


# ---------- 适用范围限制 ----------

def test_requires_continuous_json_commits_from_zero(tmp_path):
    log_dir = tmp_path / "t" / "_delta_log"
    log_dir.mkdir(parents=True)
    write_commit(log_dir, 0, [PROTOCOL, metadata_action(), add_action(f(0))])
    write_commit(log_dir, 2, [add_action(f(1))])
    (tmp_path / "t" / f(0)).parent.mkdir(parents=True, exist_ok=True)
    (tmp_path / "t" / f(0)).write_bytes(b"x")
    (tmp_path / "t" / f(1)).write_bytes(b"x")
    with pytest.raises(RecoveryError, match="不连续|缺少 v1"):
        recover_table(tmp_path / "t", 2, 0)


def test_requires_commit_zero(tmp_path):
    log_dir = tmp_path / "t" / "_delta_log"
    log_dir.mkdir(parents=True)
    write_commit(log_dir, 1, [PROTOCOL, metadata_action(), add_action(f(0))])
    with pytest.raises(RecoveryError, match="缺少版本 0"):
        recover_table(tmp_path / "t", 1, 0)


def test_rejects_metadata_change(tmp_path):
    log_dir = tmp_path / "t" / "_delta_log"
    log_dir.mkdir(parents=True)
    write_commit(log_dir, 0, [PROTOCOL, metadata_action(), add_action(f(0))])
    other = metadata_action()
    other["metaData"]["configuration"] = {"delta.appendOnly": "true"}
    write_commit(log_dir, 1, [other, add_action(f(1))])
    with pytest.raises(RecoveryError, match="元数据"):
        recover_table(tmp_path / "t", 1, 0)


def test_rejects_partitioned_table(tmp_path):
    log_dir = tmp_path / "t" / "_delta_log"
    log_dir.mkdir(parents=True)
    write_commit(log_dir, 0, [PROTOCOL, metadata_action(["region"]),
                              add_action("region=cn/part-0.parquet")])
    with pytest.raises(RecoveryError, match="分区"):
        recover_table(tmp_path / "t", 0, 0)


def test_rejects_missing_metadata_action(tmp_path):
    log_dir = tmp_path / "t" / "_delta_log"
    log_dir.mkdir(parents=True)
    write_commit(log_dir, 0, [PROTOCOL, add_action(f(0))])
    (tmp_path / "t" / f(0)).parent.mkdir(parents=True, exist_ok=True)
    (tmp_path / "t" / f(0)).write_bytes(b"x")
    with pytest.raises(RecoveryError, match="metaData"):
        recover_table(tmp_path / "t", 0, 0)


def test_rejects_metadata_partition_columns_added_later(tmp_path):
    log_dir = tmp_path / "t" / "_delta_log"
    log_dir.mkdir(parents=True)
    write_commit(log_dir, 0, [PROTOCOL, metadata_action(), add_action(f(0))])
    write_commit(log_dir, 1, [metadata_action(["region"]), add_action(f(1))])
    with pytest.raises(RecoveryError, match="元数据"):
        recover_table(tmp_path / "t", 1, 0)


def test_rejects_staged_multi_part_checkpoint_only(tmp_path):
    """只有分片检查点、没有 JSON 提交的表不在恢复范围内。"""
    log_dir = tmp_path / "t" / "_delta_log"
    log_dir.mkdir(parents=True)
    (log_dir / f"{0:020d}.checkpoint.00000000000000000001.00000000000000000002.parquet")\
        .write_bytes(b"x")
    with pytest.raises(RecoveryError, match="缺少版本 0"):
        recover_table(tmp_path / "t", 0, 0)


def test_rejects_unsupported_protocol(tmp_path):
    log_dir = tmp_path / "t" / "_delta_log"
    log_dir.mkdir(parents=True)
    write_commit(log_dir, 0, [{"protocol": {"minReaderVersion": 1,
                                            "minWriterVersion": 5}},
                              metadata_action(), add_action(f(0))])
    with pytest.raises(RecoveryError, match="minWriterVersion"):
        recover_table(tmp_path / "t", 0, 0)


def test_rejects_protocol_features(tmp_path):
    log_dir = tmp_path / "t" / "_delta_log"
    log_dir.mkdir(parents=True)
    write_commit(log_dir, 0, [{"protocol": {"minReaderVersion": 1,
                                            "minWriterVersion": 2,
                                            "writerFeatures": ["appendOnly"]}},
                              metadata_action(), add_action(f(0))])
    with pytest.raises(RecoveryError, match="特性"):
        recover_table(tmp_path / "t", 0, 0)


def test_rejects_deletion_vectors(tmp_path):
    log_dir = tmp_path / "t" / "_delta_log"
    log_dir.mkdir(parents=True)
    dv_add = add_action(f(0))
    dv_add["add"]["deletionVector"] = {"storageType": "u", "pathOrInlineDv": "x"}
    write_commit(log_dir, 0, [PROTOCOL, metadata_action(), dv_add])
    with pytest.raises(RecoveryError, match="删除向量"):
        recover_table(tmp_path / "t", 0, 0)


def test_new_version_is_visible_and_replayable(tmp_path):
    table = drill_table(tmp_path / "t")
    result = recover_table(table, 3, 2)
    report = analyze_table(table, 2, result["new_version"])
    new_version = report["versions"][-1]
    assert new_version["version"] == 4
    assert new_version["status"] == "recoverable"
    assert new_version["referenced_files"] == sorted([f(0), f(2), f(3)])
    # 目标版本本身也仍可重放
    target = report["versions"][0]
    assert target["version"] == 2
    assert target["status"] == "recoverable"


def test_commit_has_no_partial_tmp_files(tmp_path):
    table = drill_table(tmp_path / "t")
    recover_table(table, 3, 2)
    leftovers = [p.name for p in (table / "_delta_log").iterdir()
                 if p.name.endswith(".tmp")]
    assert leftovers == []


# ---------- 并发：同一当前版最多成功一笔 ----------

def test_concurrent_requests_same_current_version_only_one_wins(tmp_path):
    table = drill_table(tmp_path / "t")
    outcomes = []
    barrier = threading.Barrier(8)

    def attempt():
        barrier.wait()
        try:
            outcomes.append(("ok", recover_table(table, 3, 2)["new_version"]))
        except RecoveryError as exc:
            outcomes.append(("err", exc.code))

    threads = [threading.Thread(target=attempt) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    successes = [o for o in outcomes if o[0] == "ok"]
    assert len(successes) == 1
    assert successes[0][1] == 4
    assert all(o[1] == ERROR_STALE_CURRENT_VERSION
               for o in outcomes if o[0] == "err")
    assert not (table / "_delta_log" / commit_name(5)).exists()
