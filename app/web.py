"""Flask 网页：Delta Lake 版本可恢复性分析 + 本地副本恢复演练。

核查页：值班员填写版本区间，上传 _delta_log 留存的 JSON 提交、
经典单文件 Parquet 检查点，以及现存数据文件的表内相对路径清单，
提交后逐版展示可恢复性结论。页面也可一键加载仓库内置的本地样例。

恢复演练：错单写入后，值班员按工单在页面填写“当前版”和“要恢复的旧版”，
预检通过后确认，工具向本地副本的 _delta_log 追加下一版 JSON 提交，
使新版本快照与目标旧版一致；旧日志与 Parquet 一律不改。
"""

from __future__ import annotations

import os
import re
from pathlib import Path

from flask import Flask, jsonify, render_template, request

from .analyzer import AnalyzerError, InputFile, analyze
from .recovery import (
    MissingFilesError,
    RestoreConflict,
    RestoreError,
    build_restore_plan,
    execute_restore,
    latest_commit_version,
)

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 256 * 1024 * 1024  # 单次请求上限 256MB

SAMPLE_ROOT = Path(os.environ.get("SAMPLE_ROOT",
                                  Path(__file__).resolve().parent.parent / "samples"))

# 样例 / 演练表根目录（容器内路径，可用环境变量覆盖；测试可改 app.config）
app.config["SAMPLE_ROOT"] = str(SAMPLE_ROOT)
# 恢复演练写入的表根目录；默认与样例目录一致（docker-compose 以可写方式挂载）
app.config["TABLE_ROOT"] = os.environ.get("TABLE_ROOT", str(SAMPLE_ROOT))

# 样例目录名 -> 页面展示名 / 默认版本区间
SAMPLES = {
    "healthy_table": {
        "label": "健康表样例（0-4 版均可重建）",
        "range": (0, 4),
    },
    "incident_table": {
        "label": "误删事故样例（部分版本缺提交或缺数据文件）",
        "range": (0, 5),
    },
    "bad_orders_table": {
        "label": "错单演练表（v2 写入错单批次，可演练恢复到 v1）",
        "range": (0, 3),
    },
}

# 表名只允许简单字符，杜绝路径穿越
_TABLE_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


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


@app.get("/")
def index():
    return render_template("index.html", samples=SAMPLES)


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
        return render_template("index.html", samples=SAMPLES, error=str(exc),
                               form=request.form), 400
    return render_template("index.html", samples=SAMPLES, report=report)


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
    sample_dir = Path(app.config["SAMPLE_ROOT"]) / sample_name
    if not sample_dir.is_dir():
        return jsonify({"error": f"样例目录不存在：{sample_dir}"}), 404

    try:
        payload = request.get_json(silent=True) or {}
        meta = SAMPLES[sample_name]
        from_version = int(payload.get("from_version", meta["range"][0]))
        to_version = int(payload.get("to_version", meta["range"][1]))

        log_dir = sample_dir / "_delta_log"
        log_files = []
        for path in sorted(log_dir.iterdir()):
            if path.is_file():
                log_files.append(InputFile(filename=path.name,
                                           data=path.read_bytes()))
        manifest_path = sample_dir / "existing_files.txt"
        manifest_files = [InputFile(
            filename=manifest_path.name,
            data=manifest_path.read_bytes(),
        )]
        report = _run_analysis(log_files, manifest_files,
                               from_version, to_version)
    except AnalyzerError as exc:
        return jsonify({"error": str(exc)}), 400
    except (OSError, ValueError) as exc:
        return jsonify({"error": f"读取样例失败：{exc}"}), 500
    return jsonify(report)


# ---------- 恢复演练 ----------

def _table_root() -> Path:
    return Path(app.config["TABLE_ROOT"])


def _resolve_table_dir(name) -> Path:
    if not isinstance(name, str) or not _TABLE_NAME_RE.match(name):
        raise RestoreError(f"非法表名：{name!r}（只允许字母、数字、点、下划线、连字符）")
    root = _table_root().resolve()
    table_dir = (root / name).resolve()
    if table_dir.parent != root or not table_dir.is_dir():
        raise LookupError(f"未知演练表：{name}")
    return table_dir


def _json_version(value, label: str) -> int:
    if isinstance(value, bool):
        raise RestoreError(f"{label}必须是整数")
    if isinstance(value, int):
        return value
    if isinstance(value, str) and re.fullmatch(r"-?\d+", value.strip()):
        return int(value.strip())
    raise RestoreError(f"{label}必须是整数，收到：{value!r}")


def _restore_request():
    payload = request.get_json(silent=True) or {}
    table_dir = _resolve_table_dir(payload.get("table"))
    current_version = _json_version(payload.get("current_version"), "当前版")
    target_version = _json_version(payload.get("target_version"), "要恢复的旧版")
    return table_dir, current_version, target_version


def _restore_error_response(exc: Exception):
    if isinstance(exc, LookupError):
        return jsonify({"error": str(exc)}), 404
    if isinstance(exc, MissingFilesError):
        return jsonify({"error": str(exc), "missing_files": exc.paths}), 400
    if isinstance(exc, RestoreConflict):
        return jsonify({"error": str(exc)}), 409
    if isinstance(exc, RestoreError):
        return jsonify({"error": str(exc)}), 400
    raise exc


@app.get("/api/tables")
def list_tables():
    """列出可演练的本地副本表（TABLE_ROOT 下含 _delta_log 的目录）。"""
    root = _table_root()
    tables = []
    if root.is_dir():
        for child in sorted(root.iterdir()):
            log_dir = child / "_delta_log"
            if child.is_dir() and log_dir.is_dir():
                tables.append({
                    "name": child.name,
                    "latest_version": latest_commit_version(log_dir),
                })
    return jsonify({"tables": tables})


@app.post("/api/restore/plan")
def restore_plan():
    """预检：校验约束并返回恢复方案，不写任何文件。"""
    try:
        table_dir, current_version, target_version = _restore_request()
        plan = build_restore_plan(table_dir, current_version, target_version)
    except (RestoreError, LookupError) as exc:
        return _restore_error_response(exc)
    return jsonify(plan)


@app.post("/api/restore/confirm")
def restore_confirm():
    """确认恢复：向 _delta_log 追加下一版 JSON 提交。"""
    try:
        table_dir, current_version, target_version = _restore_request()
        result = execute_restore(table_dir, current_version, target_version)
    except (RestoreError, LookupError) as exc:
        return _restore_error_response(exc)
    except OSError as exc:
        return jsonify({"error": f"写入恢复提交失败：{exc}"}), 500
    return jsonify(result)


@app.errorhandler(413)
def too_large(_err):
    return render_template("index.html", samples=SAMPLES,
                           error="上传内容超过 256MB 上限"), 413


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
