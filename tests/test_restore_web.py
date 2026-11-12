"""恢复演练的 Flask 路由测试（在样表副本上进行，不污染仓库样例）。"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from app.web import app


@pytest.fixture()
def tables_root(tmp_path, monkeypatch):
    """把仓库样例复制到临时目录，作为恢复演练的 TABLE_ROOT / SAMPLE_ROOT。"""
    src = Path(app.config["SAMPLE_ROOT"])
    dst = tmp_path / "samples"
    shutil.copytree(src, dst)
    monkeypatch.setitem(app.config, "TABLE_ROOT", str(dst))
    monkeypatch.setitem(app.config, "SAMPLE_ROOT", str(dst))
    return dst


def test_list_tables(tables_root):
    resp = app.test_client().get("/api/tables")
    assert resp.status_code == 200
    tables = {t["name"]: t["latest_version"] for t in resp.get_json()["tables"]}
    assert tables == {"bad_orders_table": 3, "healthy_table": 4,
                      "incident_table": 5}


def test_restore_plan_happy(tables_root):
    client = app.test_client()
    resp = client.post("/api/restore/plan", json={
        "table": "bad_orders_table", "current_version": 3, "target_version": 1})
    assert resp.status_code == 200
    plan = resp.get_json()
    assert plan["new_version"] == 4
    assert plan["to_remove"] == ["data/orders-005.parquet",
                                 "data/orders-bad-003.parquet",
                                 "data/orders-bad-004.parquet"]
    assert [a["path"] for a in plan["to_readd"]] == ["data/orders-000.parquet"]
    # 预检不写文件
    log_dir = tables_root / "bad_orders_table" / "_delta_log"
    assert not (log_dir / f"{4:020d}.json").exists()


def test_restore_plan_accepts_string_versions(tables_root):
    resp = app.test_client().post("/api/restore/plan", json={
        "table": "bad_orders_table", "current_version": "3",
        "target_version": "1"})
    assert resp.status_code == 200


def test_restore_plan_incident_table_rejected(tables_root):
    """incident_table 缺 v3 提交：不满足“从 0 版起 JSON 提交连续”。"""
    resp = app.test_client().post("/api/restore/plan", json={
        "table": "incident_table", "current_version": 5, "target_version": 2})
    assert resp.status_code == 400
    assert "不连续" in resp.get_json()["error"]


def test_restore_plan_unknown_table_404(tables_root):
    resp = app.test_client().post("/api/restore/plan", json={
        "table": "nope", "current_version": 1, "target_version": 0})
    assert resp.status_code == 404


def test_restore_plan_bad_table_name_400(tables_root):
    for bad in ("../etc", "a/b", "", ".hidden"):
        resp = app.test_client().post("/api/restore/plan", json={
            "table": bad, "current_version": 1, "target_version": 0})
        assert resp.status_code == 400, bad


def test_restore_plan_invalid_versions_400(tables_root):
    resp = app.test_client().post("/api/restore/plan", json={
        "table": "bad_orders_table", "current_version": 3, "target_version": 3})
    assert resp.status_code == 400
    resp = app.test_client().post("/api/restore/plan", json={
        "table": "bad_orders_table", "current_version": "x",
        "target_version": 1})
    assert resp.status_code == 400


def test_restore_confirm_then_replay_on_check_page(tables_root):
    """确认恢复后显示新版本号，原核查页能重放该版本。"""
    client = app.test_client()
    resp = client.post("/api/restore/confirm", json={
        "table": "bad_orders_table", "current_version": 3, "target_version": 1})
    assert resp.status_code == 200
    result = resp.get_json()
    assert result["new_version"] == 4
    assert result["commit_file"] == f"{4:020d}.json"

    log_dir = tables_root / "bad_orders_table" / "_delta_log"
    assert (log_dir / result["commit_file"]).is_file()

    # 核查页（样例分析）重放到新版本 v4：文件齐全、快照与 v1 一致
    resp = client.post("/api/analyze-sample/bad_orders_table",
                       json={"from_version": 0, "to_version": 4})
    assert resp.status_code == 200
    report = resp.get_json()
    v4 = report["versions"][-1]
    assert v4["version"] == 4
    assert v4["status"] == "recoverable"
    assert v4["referenced_files"] == ["data/orders-000.parquet",
                                      "data/orders-001.parquet",
                                      "data/orders-002.parquet"]


def test_restore_confirm_same_current_version_only_once(tables_root):
    client = app.test_client()
    payload = {"table": "bad_orders_table", "current_version": 3,
               "target_version": 1}
    assert client.post("/api/restore/confirm", json=payload).status_code == 200
    second = client.post("/api/restore/confirm", json=payload)
    assert second.status_code == 409
    assert "当前版已变" in second.get_json()["error"]
    log_dir = tables_root / "bad_orders_table" / "_delta_log"
    commits = sorted(p.name for p in log_dir.glob("*.json"))
    assert commits == [f"{v:020d}.json" for v in range(5)]


def test_restore_confirm_current_version_moved_409(tables_root):
    resp = app.test_client().post("/api/restore/confirm", json={
        "table": "bad_orders_table", "current_version": 2, "target_version": 1})
    assert resp.status_code == 409


def test_restore_confirm_missing_files_lists_paths_no_write(tables_root):
    victim = tables_root / "bad_orders_table" / "data" / "orders-001.parquet"
    victim.unlink()
    resp = app.test_client().post("/api/restore/confirm", json={
        "table": "bad_orders_table", "current_version": 3, "target_version": 1})
    assert resp.status_code == 400
    data = resp.get_json()
    assert data["missing_files"] == ["data/orders-001.parquet"]
    log_dir = tables_root / "bad_orders_table" / "_delta_log"
    assert not (log_dir / f"{4:020d}.json").exists()   # 缺件不写日志


def test_restore_healthy_table_with_checkpoints(tables_root):
    """healthy_table 含检查点：恢复只依据 JSON 提交，也能演练成功。"""
    client = app.test_client()
    resp = client.post("/api/restore/confirm", json={
        "table": "healthy_table", "current_version": 4, "target_version": 1})
    assert resp.status_code == 200
    assert resp.get_json()["new_version"] == 5
    resp = client.post("/api/analyze-sample/healthy_table",
                       json={"from_version": 0, "to_version": 5})
    v5 = resp.get_json()["versions"][-1]
    assert v5["status"] == "recoverable"
    assert v5["referenced_files"] == [
        "data/part-00000-healthy.parquet",
        "data/part-00001-healthy.parquet",
        "data/part-00002-healthy.parquet",
    ]
