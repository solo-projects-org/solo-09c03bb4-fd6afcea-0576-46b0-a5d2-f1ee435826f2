"""Flask 路由层测试（含本地样例加载与恢复演练）。"""

from __future__ import annotations

import io
import json
import shutil

import pytest

import app.web as web_module
from app.web import app


def _commit_bytes(actions):
    return ("\n".join(json.dumps(a) for a in actions)).encode("utf-8")


@pytest.fixture()
def writable_samples(tmp_path, monkeypatch):
    """把仓库样例复制到临时目录，恢复演练测试不污染仓库内样例。"""
    src = web_module.SAMPLE_ROOT
    dst = tmp_path / "samples"
    shutil.copytree(src, dst)
    monkeypatch.setattr(web_module, "SAMPLE_ROOT", dst)
    return dst


def test_index_renders():
    client = app.test_client()
    resp = client.get("/")
    assert resp.status_code == 200
    assert "Delta Lake 版本可恢复性分析".encode() in resp.data
    assert "本地副本恢复演练".encode() in resp.data


def test_upload_analyze_flow():
    client = app.test_client()
    data = {
        "from_version": "0",
        "to_version": "1",
        "log_files": [
            (io.BytesIO(_commit_bytes([{"add": {"path": "data/a.parquet"}}])),
             "00000000000000000000.json"),
            (io.BytesIO(_commit_bytes([{"add": {"path": "data/b.parquet"}}])),
             "00000000000000000001.json"),
        ],
        "manifest_files": [
            (io.BytesIO(b"data/a.parquet\ndata/b.parquet\n"), "existing.txt"),
        ],
    }
    resp = client.post("/analyze", data=data, content_type="multipart/form-data")
    assert resp.status_code == 200
    assert "可重建 · 文件齐全".encode() in resp.data


def test_upload_missing_file_reports_400():
    client = app.test_client()
    resp = client.post("/analyze", data={
        "from_version": "0", "to_version": "1",
    }, content_type="multipart/form-data")
    assert resp.status_code == 400
    assert "至少上传".encode() in resp.data


def test_sample_api_healthy_and_incident():
    client = app.test_client()
    for name, expected_recoverable in (("healthy_table", 5),
                                       ("incident_table", None)):
        resp = client.post(f"/api/analyze-sample/{name}")
        assert resp.status_code == 200, resp.data
        report = resp.get_json()
        assert report["range"] == {"from": 0, "to": (4 if name == "healthy_table" else 5)}
        if expected_recoverable:
            assert report["summary"]["recoverable"] == expected_recoverable


def test_unknown_sample_404():
    resp = app.test_client().post("/api/analyze-sample/nope")
    assert resp.status_code == 404


def test_sample_api_list():
    resp = app.test_client().get("/api/samples")
    assert resp.status_code == 200
    names = resp.get_json()
    assert "healthy_table" in names and "incident_table" in names


# ---------- 恢复演练路由 ----------

def test_recover_sample_healthy_table(writable_samples):
    client = app.test_client()
    before_logs = {p.name: p.read_bytes()
                   for p in (writable_samples / "healthy_table" / "_delta_log")
                   .iterdir() if p.is_file()}
    resp = client.post("/api/recover-sample/healthy_table",
                       json={"current_version": 4, "target_version": 2})
    assert resp.status_code == 200, resp.data
    data = resp.get_json()
    assert data["new_version"] == 5
    assert data["previous_current_version"] == 4
    assert data["target_version"] == 2
    assert data["commit_file"] == f"{5:020d}.json"
    # v4 快照 {a0,a3,a4,a5} -> v2 快照 {a0,a2,a3}：移出 a4/a5，重新纳入 a2
    assert data["removed_files"] == [
        "data/part-00004-healthy.parquet", "data/part-00005-healthy.parquet"]
    assert data["added_files"] == ["data/part-00002-healthy.parquet"]
    assert data["referenced_files"] == [
        "data/part-00000-healthy.parquet",
        "data/part-00002-healthy.parquet",
        "data/part-00003-healthy.parquet",
    ]

    # 新提交文件已落盘，动作都标 dataChange=true
    new_log = (writable_samples / "healthy_table" / "_delta_log"
               / f"{5:020d}.json").read_text("utf-8")
    actions = [json.loads(line) for line in new_log.splitlines() if line.strip()]
    for action in actions:
        if "add" in action:
            assert action["add"]["dataChange"] is True
        if "remove" in action:
            assert action["remove"]["dataChange"] is True
    # 旧日志逐字节未变
    for name, payload in before_logs.items():
        assert (writable_samples / "healthy_table" / "_delta_log" / name
                ).read_bytes() == payload

    # 原核查页重放新版本：新版本可重建，且引用文件与目标版本一致
    verification = data["verification"]
    new_row = next(v for v in verification["versions"]
                   if v["version"] == data["new_version"])
    assert new_row["status"] == "recoverable"
    assert new_row["referenced_files"] == data["referenced_files"]
    target_row = next(v for v in verification["versions"]
                      if v["version"] == data["target_version"])
    assert target_row["referenced_files"] == data["referenced_files"]


def test_recover_sample_stale_current_version_conflict(writable_samples):
    client = app.test_client()
    client.post("/api/recover-sample/healthy_table",
                json={"current_version": 4, "target_version": 2})
    resp = client.post("/api/recover-sample/healthy_table",
                       json={"current_version": 4, "target_version": 2})
    assert resp.status_code == 409
    data = resp.get_json()
    assert data["code"] == "stale_current_version"
    assert data["actual_current_version"] == 5
    # 只成功了一笔：没有 v6
    assert not (writable_samples / "healthy_table" / "_delta_log"
                / f"{6:020d}.json").exists()
    # 目标版本重新纳入的 Parquet 被读取，但旧文件与清单未被改动
    assert (writable_samples / "healthy_table" / "data"
            / "part-00002-healthy.parquet").read_bytes() == \
        b"placeholder parquet bytes for sample"


def test_recover_sample_missing_parquet_lists_paths(writable_samples):
    client = app.test_client()
    missing_file = writable_samples / "healthy_table" / "data" / \
        "part-00002-healthy.parquet"
    missing_file.unlink()
    resp = client.post("/api/recover-sample/healthy_table",
                       json={"current_version": 4, "target_version": 2})
    assert resp.status_code == 422
    data = resp.get_json()
    assert data["code"] == "missing_parquet"
    assert data["missing_files"] == ["data/part-00002-healthy.parquet"]
    assert not (writable_samples / "healthy_table" / "_delta_log"
                / f"{5:020d}.json").exists()


def test_recover_sample_incident_table_not_drillable(writable_samples):
    resp = app.test_client().post(
        "/api/recover-sample/incident_table",
        json={"current_version": 5, "target_version": 2})
    assert resp.status_code == 400
    assert "不开放" in resp.get_json()["error"]


def test_recover_sample_unknown_404(writable_samples):
    resp = app.test_client().post("/api/recover-sample/nope", json={})
    assert resp.status_code == 404


def test_recover_sample_requires_versions(writable_samples):
    resp = app.test_client().post("/api/recover-sample/healthy_table", json={})
    assert resp.status_code == 400
    assert "请填写" in resp.get_json()["error"]


def test_recover_generic_alias(writable_samples):
    resp = app.test_client().post(
        "/api/recover",
        json={"table": "healthy_table", "current_version": 4,
              "target_version": 2})
    assert resp.status_code == 200, resp.data
    assert resp.get_json()["new_version"] == 5


def test_recover_generic_requires_table(writable_samples):
    resp = app.test_client().post(
        "/api/recover", json={"current_version": 4, "target_version": 2})
    assert resp.status_code == 400


def test_recover_sample_locks_out_concurrent_requests(writable_samples):
    """模拟两个 worker 同时收到同一当前版的确认请求：只能落一笔。"""
    import threading

    outcomes = []
    barrier = threading.Barrier(2)

    def attempt():
        barrier.wait()
        resp = app.test_client().post(
            "/api/recover-sample/healthy_table",
            json={"current_version": 4, "target_version": 2})
        outcomes.append((resp.status_code, resp.get_json()))

    threads = [threading.Thread(target=attempt) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sorted(status for status, _ in outcomes) == [200, 409]
    assert not (writable_samples / "healthy_table" / "_delta_log"
                / f"{6:020d}.json").exists()


def test_index_shows_current_version_of_writable_sample(writable_samples):
    resp = app.test_client().get("/")
    assert resp.status_code == 200
    assert "当前 v4".encode() in resp.data
