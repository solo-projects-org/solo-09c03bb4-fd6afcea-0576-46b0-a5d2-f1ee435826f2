"""Flask 网页：Delta Lake 版本可恢复性分析 + 本地副本恢复演练。

值班员在页面上填写版本区间，上传 _delta_log 留存的 JSON 提交、
经典单文件 Parquet 检查点，以及现存数据文件的表内相对路径清单，
提交后逐版展示可恢复性结论。页面也可一键加载仓库内置的本地样例。

页面另有“恢复演练”入口：选择本地副本中的表，填写工单上的当前版本与
要恢复的旧版本，工具在 `_delta_log` 追加下一版 JSON，并在成功后立即
用原核查逻辑重放新版本。
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, render_template, request

from .analyzer import AnalyzerError, InputFile, analyze
from .recovery import (
    ERROR_MISSING_PARQUET,
    ERROR_STALE_CURRENT_VERSION,
    LOCK_FILE_NAME,
    RecoveryError,
    recover_table,
)

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 256 * 1024 * 1024  # 单次请求上限 256MB

SAMPLE_ROOT = Path(os.environ.get("SAMPLE_ROOT",
                                  Path(__file__).resolve().parent.parent / "samples"))

# 样例目录名 -> 页面展示名 / 默认版本区间 / 是否可做恢复演练
SAMPLES = {
    "healthy_table": {
        "label": "健康表样例（提交连续，可写，可演练恢复）",
        "range": (0, 4),
        "drillable": True,
    },
    "incident_table": {
        "label": "误删事故样例（缺提交或缺数据文件，仅供核查）",
        "range": (0, 5),
        "drillable": False,
    },
}

_COMMIT_NAME_RE = re.compile(r"^(\d{20})\.json$")


def _read_uploads(field_name: str) -> list[InputFile]:
    files = []
    for storage in request.files.getlist(field_name):
        if not storage or storage.filename == "":
            continue
        files.append(InputFile(filename=storage.filename,
                               data=storage.read()))
    return files


def _parse_version(raw: str, field_label: str) -> int:
    raw = (raw or "").strip()
    if raw == "":
        raise AnalyzerError(f"请填写{field_label}")
    try:
        return int(raw)
    except ValueError as exc:
        raise AnalyzerError(f"{field_label}必须是整数，收到：{raw!r}") from exc


def _run_analysis(log_files, manifest_files, from_version, to_version):
    return analyze(log_files=log_files,
                   manifest_files=manifest_files,
                   from_version=from_version,
                   to_version=to_version)


def _sample_dir(sample_name: str) -> Path:
    if sample_name not in SAMPLES:
        raise RecoveryError(f"未知样例：{sample_name}")
    sample_dir = SAMPLE_ROOT / sample_name
    if not sample_dir.is_dir():
        raise RecoveryError(f"样例目录不存在：{sample_dir}")
    return sample_dir


def _analyze_logs_dir(log_dir: Path, manifest_files: list[InputFile],
                      from_version: int, to_version: int) -> dict:
    log_files = []
    for path in sorted(log_dir.iterdir()):
        if path.is_file():
            log_files.append(InputFile(filename=path.name,
                                       data=path.read_bytes()))
    return _run_analysis(log_files, manifest_files, from_version, to_version)


def _analyze_sample_dir(sample_dir: Path, from_version: int,
                        to_version: int) -> dict:
    manifest_path = sample_dir / "existing_files.txt"
    manifest_files = [InputFile(
        filename=manifest_path.name,
        data=manifest_path.read_bytes(),
    )]
    return _analyze_logs_dir(sample_dir / "_delta_log", manifest_files,
                             from_version, to_version)


def _scan_existing_files(table_dir: Path) -> list[str]:
    """扫描表目录中实际存在的文件（_delta_log 之外），生成现存清单。

    清单直接来自文件系统，避免了文本清单在演练后过期的问题；恢复锁与
    写入中的临时文件不属于数据文件，予以排除。
    """
    existing: list[str] = []
    for path in sorted(table_dir.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(table_dir).as_posix()
        if rel.startswith("_delta_log/") or rel == LOCK_FILE_NAME:
            continue
        if path.name.startswith(".") or path.name.endswith(".tmp"):
            continue
        existing.append(rel)
    return existing


def _verify_new_version(table_dir: Path, target_version: int,
                        new_version: int) -> dict:
    """用原核查逻辑重放新版本：清单取自实际文件系统，而非可能过期的文本。"""
    manifest = InputFile(
        filename="(filesystem scan)",
        data=("\n".join(_scan_existing_files(table_dir)) + "\n").encode("utf-8"),
    )
    return _analyze_logs_dir(table_dir / "_delta_log", [manifest],
                             target_version, new_version)


def _resolve_table_dir(name: Any, table_path: Any) -> tuple[Path, str]:
    """把请求里的演练表解析为 SAMPLE_ROOT 内的目录，阻止越界写入。"""
    root = SAMPLE_ROOT.resolve()
    if not name and not table_path:
        raise RecoveryError("请指定演练表", code="missing_table")
    if table_path:
        candidate = Path(str(table_path))
        if not candidate.is_absolute():
            candidate = root / candidate
    else:
        candidate = root / str(name)
    try:
        resolved = candidate.resolve()
    except OSError as exc:
        raise RecoveryError(f"无法解析表目录：{exc}") from exc
    if resolved != root and root not in resolved.parents:
        raise RecoveryError(f"表目录必须位于 {root} 之内：{candidate}",
                            code="unknown_table")

    if not resolved.is_dir():
        raise RecoveryError(f"表目录不存在：{resolved}", code="unknown_table")
    meta = SAMPLES.get(resolved.name)
    if meta is not None and not meta.get("drillable"):
        raise RecoveryError(f"样例 {resolved.name} 不开放恢复演练")
    return resolved, resolved.name


def _current_versions() -> dict[str, dict]:
    """列出可演练表及其 JSON 提交当前版本。"""
    result = {}
    if not SAMPLE_ROOT.is_dir():
        return result
    for sample_dir in sorted(SAMPLE_ROOT.iterdir()):
        if not sample_dir.is_dir() or sample_dir.name.startswith("."):
            continue
        meta = SAMPLES.get(sample_dir.name)
        if meta is not None and not meta.get("drillable"):
            continue
        log_dir = sample_dir / "_delta_log"
        if not log_dir.is_dir():
            continue
        versions = []
        for path in log_dir.iterdir():
            match = _COMMIT_NAME_RE.match(path.name)
            if match and path.is_file():
                versions.append(int(match.group(1)))
        result[sample_dir.name] = {
            "label": meta["label"] if meta else f"{sample_dir.name}（本地副本）",
            "range": list(meta["range"]) if meta else [],
            "current_version": max(versions) if versions else None,
        }
    return result


def _render_index(**kwargs):
    tables = _current_versions()
    first_current = next(iter(tables.values()), {}).get("current_version")
    return render_template("index.html", samples=SAMPLES,
                           recovery_tables=tables,
                           initial_current_version=first_current,
                           **kwargs)


@app.get("/")
def index():
    return _render_index()


@app.post("/analyze")
def analyze_upload():
    try:
        from_version = _parse_version(request.form.get("from_version"), "起始版本")
        to_version = _parse_version(request.form.get("to_version"), "结束版本")
        log_files = _read_uploads("log_files")
        manifest_files = _read_uploads("manifest_files")
        report = _run_analysis(log_files, manifest_files,
                               from_version, to_version)
    except AnalyzerError as exc:
        return _render_index(error=str(exc), form=request.form), 400
    return _render_index(report=report)


@app.get("/api/samples")
def list_samples():
    return jsonify({
        key: {"label": meta["label"], "range": list(meta["range"])}
        for key, meta in SAMPLES.items()
    })


@app.post("/api/analyze-sample/<sample_name>")
def analyze_sample(sample_name: str):
    """直接加载服务器本地样例目录，无需手动上传。"""
    if sample_name not in SAMPLES:
        return jsonify({"error": f"未知样例：{sample_name}"}), 404
    try:
        payload = request.get_json(silent=True) or {}
        meta = SAMPLES[sample_name]
        from_version = int(payload.get("from_version", meta["range"][0]))
        to_version = int(payload.get("to_version", meta["range"][1]))

        report = _analyze_sample_dir(_sample_dir(sample_name),
                                     from_version, to_version)
    except AnalyzerError as exc:
        return jsonify({"error": str(exc)}), 400
    except (OSError, ValueError) as exc:
        return jsonify({"error": f"读取样例失败：{exc}"}), 500
    return jsonify(report)


@app.post("/api/recover-sample/<sample_name>")
def recover_sample(sample_name: str):
    """对服务器本地可写样例执行一次恢复演练，并返回新版本核查报告。"""
    return _recover_response(table_name=sample_name)


@app.post("/api/recover")
def recover_generic():
    """按表名或表目录执行恢复演练。

    JSON 字段：``table``（SAMPLE_ROOT 下的目录名，必填或改用 ``table_path``）、
    ``table_path``（SAMPLE_ROOT 内的相对 / 绝对路径）、
    ``current_version``（工单当前版）、``target_version``（要恢复的旧版）。
    """
    payload = request.get_json(silent=True) or {}
    return _recover_response(table_name=payload.get("table"),
                             table_path=payload.get("table_path"))


def _recover_response(*, table_name: Any = None, table_path: Any = None):
    payload = request.get_json(silent=True) or {}
    try:
        table_dir, table_label = _resolve_table_dir(table_name, table_path)
        result = recover_table(
            table_dir,
            payload.get("current_version",
                        payload.get("expected_current_version")),
            payload.get("target_version"),
        )

        # 成功后立即用原核查逻辑重放新版本：目标文件清单应与目标版本一致
        verify = _verify_new_version(table_dir, result["target_version"],
                                     result["new_version"])
        new_report = next(v for v in verify["versions"]
                          if v["version"] == result["new_version"])
        result["table"] = table_label
        result["verification"] = verify
        result["new_version_status"] = new_report["status"]
        result["new_version_missing_commits"] = new_report["missing_commits"]
    except RecoveryError as exc:
        status = 400
        if exc.code == ERROR_STALE_CURRENT_VERSION:
            status = 409
        elif exc.code == ERROR_MISSING_PARQUET:
            status = 422
        elif exc.code == "unknown_table":
            status = 404
        return jsonify({
            "error": str(exc),
            "code": exc.code,
            "missing_files": exc.missing_files,
            "actual_current_version": exc.actual_current_version,
        }), status
    except (OSError, ValueError) as exc:
        return jsonify({"error": f"恢复演练失败：{exc}"}), 500
    return jsonify(result)


@app.errorhandler(413)
def too_large(_err):
    return _render_index(error="上传内容超过 256MB 上限"), 413


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
