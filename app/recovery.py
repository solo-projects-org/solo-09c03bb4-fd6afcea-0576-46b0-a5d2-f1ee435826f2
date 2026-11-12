"""Delta Lake 本地副本恢复演练核心逻辑。

在工单指定的“当前版本”之后追加一个新提交，把表重新指回“目标旧版本”的
文件快照：当前快照里有、目标快照里没有的文件写 ``remove``；目标快照里有、
当前快照里没有的文件写 ``add``（沿用目标版本该文件的 ``add`` 字段）。
新旧动作都标 ``dataChange=true``，旧日志与 Parquet 数据文件一律不修改。

适用面刻意收窄（与网页提示一致）：

* ``_delta_log`` 中从 v0 起 JSON 提交连续，不依赖检查点；
* 表元数据（``metaData``）在所有提交中保持一致，且 ``partitionColumns`` 为空；
* 协议固定为 ``minReaderVersion=1`` / ``minWriterVersion=2``，且无协议特性；
* 不含删除向量（Deletion Vector）。

并发控制：对表根目录下的 ``.recovery.lock`` 加 ``flock`` 排它锁，并在锁内
重新读取当前版本；提交文件先写同目录临时文件、``fsync`` 后再原子链接为
``NNN.json``。因此即便确认请求落到不同服务进程，同一个当前版本也最多
成功一笔，且半截文件不会被读者看到；临时文件以 ``.`` 开头、不以
``NNN.json`` 结尾，按文件名扫描日志的读者不会把它当成提交。
"""

from __future__ import annotations

import copy
import errno
import fcntl
import json
import os
import re
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

from .analyzer import (
    AnalyzerError,
    DV_FEATURE_NAME,
    _decode_utf8,
    _extract_protocol_features,
    _iter_json_objects,
    normalize_data_path,
)

_COMMIT_RE = re.compile(r"^(\d{20})\.json$")
LOCK_FILE_NAME = ".recovery.lock"

SUPPORTED_MIN_READER_VERSION = 1
SUPPORTED_MIN_WRITER_VERSION = 2

ERROR_STALE_CURRENT_VERSION = "stale_current_version"
ERROR_MISSING_PARQUET = "missing_parquet"


class RecoveryError(Exception):
    """恢复演练的输入 / 能力限制错误。

    ``code`` 供 HTTP 层区分冲突（当前版已变）与普通校验失败；
    ``missing_files`` 在目标快照引用文件缺件时给出表内相对路径；
    ``actual_current_version`` 在当前版已变时给出实际扫描到的版本。
    """

    def __init__(self, message: str, *,
                 code: str = "invalid_request",
                 missing_files: list[str] | None = None,
                 actual_current_version: int | None = None):
        super().__init__(message)
        self.code = code
        self.missing_files = missing_files or []
        self.actual_current_version = actual_current_version


@dataclass
class _ParsedCommit:
    version: int
    source: str
    adds: list[tuple[str, dict[str, Any]]] = field(default_factory=list)
    removes: list[str] = field(default_factory=list)
    protocol: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = None
    protocol_features: list[str] = field(default_factory=list)
    dv_detected: bool = False


def _parse_commit_file(version: int, path: Path) -> _ParsedCommit:
    source = path.name
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise RecoveryError(f"无法读取提交 {source}：{exc}") from exc
    try:
        text = _decode_utf8(raw, source)
    except AnalyzerError as exc:
        raise RecoveryError(str(exc)) from exc

    parsed = _ParsedCommit(version=version, source=source)
    try:
        objects = list(_iter_json_objects(text, source))
    except AnalyzerError as exc:
        raise RecoveryError(str(exc)) from exc

    for obj in objects:
        if not isinstance(obj, dict):
            continue
        action = obj.get("protocol")
        if isinstance(action, dict):
            parsed.protocol = copy.deepcopy(action)
            parsed.protocol_features.extend(_extract_protocol_features(action))
        action = obj.get("metaData")
        if isinstance(action, dict):
            parsed.metadata = copy.deepcopy(action)
        action = obj.get("add")
        if isinstance(action, dict):
            try:
                data_path = normalize_data_path(action.get("path"))
            except AnalyzerError as exc:
                raise RecoveryError(f"{source}：{exc}") from exc
            parsed.adds.append((data_path, copy.deepcopy(action)))
            if action.get("deletionVector") is not None:
                parsed.dv_detected = True
        action = obj.get("remove")
        if isinstance(action, dict):
            try:
                data_path = normalize_data_path(action.get("path"))
            except AnalyzerError as exc:
                raise RecoveryError(f"{source}：{exc}") from exc
            parsed.removes.append(data_path)
            if action.get("deletionVector") is not None:
                parsed.dv_detected = True
    return parsed


def _scan_json_commits(log_dir: Path) -> dict[int, Path]:
    """扫描 JSON 提交，要求 v0 起连续，返回版本 -> 路径。"""
    try:
        entries = list(os.scandir(log_dir))
    except OSError as exc:
        raise RecoveryError(f"无法读取 _delta_log 目录：{exc}") from exc

    commits: dict[int, Path] = {}
    for entry in entries:
        match = _COMMIT_RE.match(entry.name)
        if not match or not entry.is_file():
            continue
        commits[int(match.group(1))] = Path(entry.path)

    if 0 not in commits:
        raise RecoveryError(
            "缺少版本 0 的 JSON 提交，无法从 v0 起连续重放；"
            "本恢复工具不处理仅靠检查点起点的表"
        )
    missing: list[int] = []
    for expected, version in enumerate(sorted(commits)):
        if version != expected:
            missing.extend(range(expected, version))
            break
    if missing:
        shown = ", ".join(f"v{v}" for v in missing[:20])
        more = " …" if len(missing) > 20 else ""
        raise RecoveryError(
            f"JSON 提交不连续，缺少 {shown}{more}；"
            "本恢复工具只处理从 v0 起提交连续的表"
        )
    return commits


def _validate_protocol_and_metadata(commits: dict[int, _ParsedCommit]) -> None:
    protocols: list[tuple[int, dict[str, Any]]] = []
    metadata_actions: list[tuple[int, dict[str, Any]]] = []

    for version in sorted(commits):
        parsed = commits[version]
        if parsed.dv_detected or DV_FEATURE_NAME in parsed.protocol_features:
            raise RecoveryError(
                f"表包含删除向量：提交 {parsed.source} 带 deletionVector 字段或 "
                f"{DV_FEATURE_NAME} 协议特性。本工具只处理无删除向量的表。"
            )
        if parsed.protocol is not None:
            protocols.append((version, parsed.protocol))
        if parsed.metadata is not None:
            metadata_actions.append((version, parsed.metadata))

    if not protocols:
        raise RecoveryError("日志中没有 protocol 动作，无法确认表协议")
    for version, protocol in protocols:
        min_reader = protocol.get("minReaderVersion")
        min_writer = protocol.get("minWriterVersion")
        if (min_reader != SUPPORTED_MIN_READER_VERSION
                or min_writer != SUPPORTED_MIN_WRITER_VERSION):
            raise RecoveryError(
                f"提交 v{version} 的协议为 minReaderVersion={min_reader!r}、"
                f"minWriterVersion={min_writer!r}；本恢复工具只支持 "
                f"minReaderVersion=1、minWriterVersion=2"
            )
        features = _extract_protocol_features(protocol)
        if features:
            raise RecoveryError(
                f"提交 v{version} 声明了协议特性 {sorted(set(features))}；"
                "本恢复工具只支持 minReaderVersion=1、minWriterVersion=2 且无特性的表"
            )

    if not metadata_actions:
        raise RecoveryError("日志中没有 metaData 动作，无法确认表结构")
    first_version, first_metadata = metadata_actions[0]
    for version, metadata in metadata_actions[1:]:
        if metadata != first_metadata:
            raise RecoveryError(
                f"表元数据发生变更：提交 v{version} 的 metaData 与 v{first_version} 不一致；"
                "本恢复工具只处理元数据不变的表"
            )
    partition_columns = first_metadata.get("partitionColumns")
    if not isinstance(partition_columns, list) or partition_columns:
        raise RecoveryError(
            f"表带有分区列 {partition_columns!r}；本恢复工具只处理无分区表"
        )


def _snapshot(commits: dict[int, _ParsedCommit], through: int) -> dict[str, dict[str, Any]]:
    """从 v0 重放到 ``through``，返回 路径 -> 最新 add 动作（深拷贝）。"""
    active: dict[str, dict[str, Any]] = {}
    for version in range(0, through + 1):
        parsed = commits[version]
        for data_path in parsed.removes:
            active.pop(data_path, None)
        for data_path, add_action in parsed.adds:
            active[data_path] = copy.deepcopy(add_action)
    return active


def _path_is_inside_table(table_dir: Path, data_path: str) -> bool:
    if not data_path or "\x00" in data_path:
        return False
    parts = Path(data_path).parts
    if data_path.startswith("/") or any(part == ".." for part in parts):
        return False
    candidate = table_dir / data_path
    try:
        root = table_dir.resolve()
        resolved = candidate.resolve()
    except OSError:
        return False
    return resolved == root or root in resolved.parents


def _file_exists(table_dir: Path, data_path: str) -> bool:
    if not _path_is_inside_table(table_dir, data_path):
        return False
    return (table_dir / data_path).is_file()


def _missing_target_files(table_dir: Path,
                          target_snapshot: dict[str, dict[str, Any]]) -> list[str]:
    return sorted(p for p in target_snapshot if not _file_exists(table_dir, p))


def _build_actions(target_snapshot: dict[str, dict[str, Any]],
                   current_snapshot: dict[str, dict[str, Any]],
                   *, now_ms: int) -> tuple[list[dict[str, Any]],
                                            list[str], list[str]]:
    """生成把当前快照切回目标快照所需的 remove / add 动作。"""
    current_paths = set(current_snapshot)
    target_paths = set(target_snapshot)

    removed_files = sorted(current_paths - target_paths)
    re_added_files = sorted(target_paths - current_paths)

    actions: list[dict[str, Any]] = []
    for data_path in removed_files:
        # remove 的 path 沿用当前快照中该 add 的原始写法（含 URI 编码），
        # 保证与历史 add 逐字可比，而不是用归一化后的展示路径
        raw_path = current_snapshot[data_path].get("path", data_path)
        actions.append({"remove": {
            "path": raw_path,
            "deletionTimestamp": now_ms,
            "dataChange": True,
        }})
    for data_path in re_added_files:
        # 沿用目标版本该文件的完整 add 字段，只把 dataChange 强制为 true
        add_action = copy.deepcopy(target_snapshot[data_path])
        add_action["dataChange"] = True
        actions.append({"add": add_action})
    return actions, removed_files, re_added_files


def _serialize_actions(actions: list[dict[str, Any]]) -> bytes:
    lines = [json.dumps(action, ensure_ascii=False, separators=(",", ":"))
             for action in actions]
    return ("\n".join(lines) + "\n").encode("utf-8")


def _write_commit_atomically(log_dir: Path, version: int,
                             actions: list[dict[str, Any]]) -> Path:
    """先写临时文件并 fsync，再原子链接为 NNN.json，保证不会看到半截提交。

    临时文件名以 ``.`` 开头且不以 20 位版本号 + ``.json`` 结尾，任何按
    文件名扫描 ``_delta_log`` 的读者都不会把它当成提交。
    """
    final_path = log_dir / f"{version:020d}.json"
    tmp_path = log_dir / f".{version:020d}.json.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    payload = _serialize_actions(actions)

    try:
        fd = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    except OSError as exc:
        raise RecoveryError(f"无法创建临时提交文件 {tmp_path}：{exc}") from exc
    try:
        try:
            written = 0
            while written < len(payload):
                written += os.write(fd, payload[written:])
            os.fsync(fd)
        finally:
            os.close(fd)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

    placeholder_msg = ("目标提交文件已存在，可能有其他恢复请求刚刚成功；"
                       "请刷新当前版本后重试")
    try:
        try:
            os.link(tmp_path, final_path)
        except FileExistsError as exc:
            raise RecoveryError(placeholder_msg,
                                code=ERROR_STALE_CURRENT_VERSION) from exc
        except OSError as exc:
            # 不支持硬链接的文件系统退回“先查存在、再原子 rename”。
            # 所有写者都在表级 flock 内，因此查与换之间没有本地竞争者。
            if exc.errno not in (errno.EPERM, errno.EOPNOTSUPP, errno.EXDEV):
                raise RecoveryError(f"写入新提交失败：{exc}") from exc
            if os.path.exists(final_path):
                raise RecoveryError(placeholder_msg,
                                    code=ERROR_STALE_CURRENT_VERSION) from exc
            os.replace(tmp_path, final_path)
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass

    try:
        dir_fd = os.open(log_dir, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except OSError:
        pass
    return final_path


@contextmanager
def _table_lock(table_dir: Path) -> Iterator[None]:
    """表根目录 flock 排它锁：同机不同进程 / 线程也串行化确认与写入。"""
    lock_path = table_dir / LOCK_FILE_NAME
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR | getattr(os, "O_CLOEXEC", 0),
                     0o644)
    except OSError as exc:
        raise RecoveryError(f"无法创建恢复锁文件 {lock_path}：{exc}") from exc
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
        except OSError as exc:
            raise RecoveryError(f"无法锁定表目录 {table_dir}：{exc}") from exc
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


def _coerce_version(value: Any, label: str) -> int:
    if isinstance(value, bool):
        raise RecoveryError(f"{label}必须是整数")
    if isinstance(value, int):
        version = value
    else:
        text = str(value or "").strip()
        if text == "":
            raise RecoveryError(f"请填写{label}")
        try:
            version = int(text)
        except ValueError as exc:
            raise RecoveryError(f"{label}必须是整数，收到：{text!r}") from exc
    if version < 0:
        raise RecoveryError(f"{label}不能为负数")
    return version


def recover_table(table_dir: str | os.PathLike[str],
                  expected_current_version: Any,
                  target_version: Any) -> dict[str, Any]:
    """在本地副本上执行一次恢复演练，成功时返回新版本信息。

    失败时抛出 :class:`RecoveryError`，且不会改动任何日志或数据文件。
    """
    table_dir = Path(table_dir)
    current_expected = _coerce_version(expected_current_version, "当前版本")
    target = _coerce_version(target_version, "要恢复的旧版本")

    if not table_dir.is_dir():
        raise RecoveryError(f"表目录不存在：{table_dir}")
    log_dir = table_dir / "_delta_log"
    if not log_dir.is_dir():
        raise RecoveryError(f"表目录中没有 _delta_log：{table_dir}")

    with _table_lock(table_dir):
        commit_paths = _scan_json_commits(log_dir)
        commits = {version: _parse_commit_file(version, path)
                   for version, path in sorted(commit_paths.items())}
        actual_current = max(commits)
        _validate_protocol_and_metadata(commits)

        if current_expected != actual_current:
            raise RecoveryError(
                f"当前版本已变化：工单填写 v{current_expected}，"
                f"本地副本实际当前为 v{actual_current}；已拒绝写入",
                code=ERROR_STALE_CURRENT_VERSION,
                actual_current_version=actual_current,
            )
        if target > actual_current:
            raise RecoveryError(
                f"要恢复的旧版本 v{target} 高于当前版本 v{actual_current}"
            )
        if target == actual_current:
            raise RecoveryError(
                f"要恢复的旧版本必须低于当前版本；当前已是 v{actual_current}，无需恢复"
            )

        current_snapshot = _snapshot(commits, actual_current)
        target_snapshot = _snapshot(commits, target)

        missing = _missing_target_files(table_dir, target_snapshot)
        if missing:
            raise RecoveryError(
                "目标版本 v{} 引用的 Parquet 文件在本副本中缺件，已拒绝写入日志：{}"
                .format(target, "、".join(missing)),
                code=ERROR_MISSING_PARQUET,
                missing_files=missing,
            )

        now_ms = int(time.time() * 1000)
        actions, removed_files, re_added_files = _build_actions(
            target_snapshot, current_snapshot, now_ms=now_ms)

        new_version = actual_current + 1
        # 开头的 commitInfo 让空差异提交也是一份合法、可读的完整提交；
        # 它不改变新版本引用的文件集合。
        commit_actions: list[dict[str, Any]] = [{"commitInfo": {
            "timestamp": now_ms,
            "operation": "RESTORE",
            "operationParameters": {"version": target},
            "readVersion": actual_current,
            "engineInfo": "delta-recovery-drill",
        }}]
        commit_actions.extend(actions)

        try:
            final_path = _write_commit_atomically(log_dir, new_version, commit_actions)
        except RecoveryError:
            raise
        except OSError as exc:
            raise RecoveryError(f"写入新提交失败：{exc}") from exc

    kept_files = sorted(set(target_snapshot) & set(current_snapshot))
    return {
        "table": table_dir.name,
        "previous_current_version": actual_current,
        "current_version": actual_current,
        "target_version": target,
        "new_version": new_version,
        "commit_file": final_path.name,
        "removed_files": removed_files,
        "re_added_files": re_added_files,
        # 兼容更直白的命名：added_files 即重新纳入的文件
        "added_files": list(re_added_files),
        "kept_files": kept_files,
        "referenced_files": sorted(target_snapshot),
        "referenced_file_count": len(target_snapshot),
    }
