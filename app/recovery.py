"""本地副本恢复演练：向 _delta_log 追加恢复提交。

一批错单写进 Delta Lake 后，值班员在本地副本上演练恢复：按工单填写
“当前版”和“要恢复的旧版”，工具在 ``_delta_log`` 追加下一版 JSON 提交，
使新版本的快照引用与目标旧版完全一致。错单所在版本保留在日志中供追查，
旧提交与 Parquet 数据文件一律不改。

适用范围（任一不满足即拒绝，且不写任何文件）：

- 从 0 版起 JSON 提交连续（检查点不参与恢复重放）；
- 无分区表：metaData.partitionColumns 为空，且所有 add 的 partitionValues 为空；
- 元数据不变：全部版本的 metaData 动作完全一致；
- 协议限 minReaderVersion=1、minWriterVersion=2，且无额外 reader/writer 特性；
- 无删除向量（与核查页一致的硬拦截）。

并发与可见性：

- 确认时重新校验“当前版”，与工单不符即拒绝；
- 新提交先完整写入临时文件并 fsync，再以硬链接原子生效（目标已存在则失败），
  因此即使确认请求落在不同服务进程，同一当前版也最多成功一笔，
  且新提交只有写完整才对外可见。
"""

from __future__ import annotations

import json
import os
import re
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

from .analyzer import (
    DV_FEATURE_NAME,
    AnalyzerError,
    iter_json_objects,
    normalize_data_path,
)

_COMMIT_RE = re.compile(r"^(\d{20})\.json$")
REQUIRED_PROTOCOL = (1, 2)  # (minReaderVersion, minWriterVersion)


class RestoreError(Exception):
    """恢复被拒绝（输入或表不满足约束），未写任何文件。"""


class RestoreConflict(RestoreError):
    """当前版已变 / 并发冲突：同一当前版最多成功一笔。"""


class MissingFilesError(RestoreError):
    """目标版引用的 Parquet 在副本中缺失：列出路径，不写日志。"""

    def __init__(self, paths: list[str]):
        self.paths = list(paths)
        super().__init__(
            f"目标版引用的 {len(self.paths)} 个 Parquet 文件在副本中缺失，"
            "未写入任何日志。缺失路径：" + ", ".join(self.paths)
        )


@dataclass
class CommitActions:
    """一个 JSON 提交中对恢复有用的全部动作。"""

    version: int
    adds: list[tuple[str, dict]] = field(default_factory=list)  # (归一化路径, 原始 add 字典)
    removes: list[str] = field(default_factory=list)            # 归一化路径
    protocols: list[dict] = field(default_factory=list)
    metadatas: list[dict] = field(default_factory=list)
    dv_detected: bool = False


@dataclass
class _RestorePlan:
    table_dir: Path
    current_version: int
    target_version: int
    current_active: dict[str, dict]  # 归一化路径 -> 当前版 add 字典
    target_active: dict[str, dict]   # 归一化路径 -> 目标版 add 字典
    to_remove: list[str]             # 归一化路径，已排序
    readd_paths: list[str]           # 归一化路径，已排序


def latest_commit_version(log_dir: Path) -> int | None:
    """_delta_log 中最新的 JSON 提交版本号；没有提交时返回 None。"""
    versions = []
    for entry in os.listdir(log_dir):
        m = _COMMIT_RE.match(entry)
        if m:
            versions.append(int(m.group(1)))
    return max(versions) if versions else None


def _reject_unsafe_path(path: str, source: str) -> None:
    if ".." in Path(path).parts:
        raise RestoreError(
            f"{source} 中的路径 {path!r} 含有 '..'，越出表目录，拒绝处理"
        )


def _parse_commit_full(version: int, path: Path) -> CommitActions:
    parsed = CommitActions(version=version)
    try:
        text = path.read_bytes().decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RestoreError(f"{path.name} 不是合法的 UTF-8 文本：{exc}") from exc
    except OSError as exc:
        raise RestoreError(f"无法读取提交 {path.name}：{exc}") from exc
    try:
        objects = list(iter_json_objects(text, path.name))
    except AnalyzerError as exc:
        raise RestoreError(str(exc)) from exc
    for obj in objects:
        if not isinstance(obj, dict):
            continue
        try:
            if isinstance(obj.get("add"), dict):
                add = obj["add"]
                norm = normalize_data_path(add.get("path"))
                _reject_unsafe_path(norm, path.name)
                parsed.adds.append((norm, add))
                if add.get("deletionVector") is not None:
                    parsed.dv_detected = True
            if isinstance(obj.get("remove"), dict):
                remove = obj["remove"]
                norm = normalize_data_path(remove.get("path"))
                _reject_unsafe_path(norm, path.name)
                parsed.removes.append(norm)
                if remove.get("deletionVector") is not None:
                    parsed.dv_detected = True
        except AnalyzerError as exc:
            raise RestoreError(f"{path.name}：{exc}") from exc
        if isinstance(obj.get("protocol"), dict):
            parsed.protocols.append(obj["protocol"])
        if isinstance(obj.get("metaData"), dict):
            parsed.metadatas.append(obj["metaData"])
    return parsed


def _validate_protocols(parsed_commits: list[CommitActions]) -> None:
    seen = False
    for pc in parsed_commits:
        for proto in pc.protocols:
            seen = True
            reader = proto.get("minReaderVersion")
            writer = proto.get("minWriterVersion")
            if (reader, writer) != REQUIRED_PROTOCOL:
                raise RestoreError(
                    f"协议不满足：v{pc.version} 的 protocol 为 "
                    f"minReaderVersion={reader}、minWriterVersion={writer}，"
                    "本工具只处理 minReaderVersion=1、minWriterVersion=2 的表"
                )
            features: list[str] = []
            for key in ("readerFeatures", "writerFeatures"):
                value = proto.get(key)
                if isinstance(value, list):
                    features.extend(str(v) for v in value)
            if DV_FEATURE_NAME in features:
                raise RestoreError(
                    f"v{pc.version} 的 protocol 启用了删除向量"
                    f"（{DV_FEATURE_NAME}），本工具只处理无删除向量的表"
                )
            if features:
                raise RestoreError(
                    f"v{pc.version} 的 protocol 带有额外特性 {features}，"
                    "超出 minReaderVersion=1、minWriterVersion=2 的范围"
                )
    if not seen:
        raise RestoreError(
            "日志中没有任何 protocol 动作，无法确认协议为 "
            "minReaderVersion=1、minWriterVersion=2"
        )


def _validate_metadata(parsed_commits: list[CommitActions]) -> None:
    metas = [(pc.version, md) for pc in parsed_commits for md in pc.metadatas]
    if not metas:
        return
    first_version, first_md = metas[0]
    first_canonical = json.dumps(first_md, sort_keys=True, ensure_ascii=False)
    for version, md in metas[1:]:
        if json.dumps(md, sort_keys=True, ensure_ascii=False) != first_canonical:
            raise RestoreError(
                f"元数据在 v{version} 发生变化（与 v{first_version} 不一致），"
                "本工具只处理元数据不变的表"
            )
    partition_columns = first_md.get("partitionColumns")
    if partition_columns:
        raise RestoreError(
            f"分区表（partitionColumns={partition_columns}）不在支持范围，"
            "本工具只处理无分区表"
        )


def _validate_no_dv_no_partitions(parsed_commits: list[CommitActions]) -> None:
    for pc in parsed_commits:
        if pc.dv_detected:
            raise RestoreError(
                f"v{pc.version} 的 add/remove 动作带有 deletionVector，"
                "本工具只处理无删除向量的表"
            )
        for _path, add in pc.adds:
            partition_values = add.get("partitionValues")
            if partition_values:
                raise RestoreError(
                    f"v{pc.version} 的 add({add.get('path')}) 带有分区值 "
                    f"{partition_values}，分区表不在支持范围"
                )


def _replay(parsed_commits: list[CommitActions], upto: int) -> dict[str, dict]:
    """重放 0..upto，返回快照：归一化路径 -> 该版本生效的 add 字典。"""
    active: dict[str, dict] = {}
    for pc in parsed_commits:
        if pc.version > upto:
            break
        for path in pc.removes:
            active.pop(path, None)
        for path, add in pc.adds:
            active[path] = add
    return active


def _validate_version_numbers(current_version, target_version) -> None:
    for label, value in (("当前版", current_version), ("要恢复的旧版", target_version)):
        if isinstance(value, bool) or not isinstance(value, int):
            raise RestoreError(f"{label}必须是整数，收到：{value!r}")
        if value < 0:
            raise RestoreError(f"{label}不能为负数")
    if target_version >= current_version:
        raise RestoreError(
            f"要恢复的旧版（v{target_version}）必须早于当前版（v{current_version}）"
        )


def _plan_restore(table_dir: Path,
                  current_version: int,
                  target_version: int) -> _RestorePlan:
    """全量校验并计算恢复方案；任何不满足都抛异常，不写任何文件。"""
    table_dir = Path(table_dir)
    _validate_version_numbers(current_version, target_version)

    log_dir = table_dir / "_delta_log"
    if not log_dir.is_dir():
        raise RestoreError(f"{table_dir} 下没有 _delta_log，不是一张 Delta 表")

    commits: dict[int, Path] = {}
    for entry in os.listdir(log_dir):
        m = _COMMIT_RE.match(entry)
        if m:
            commits[int(m.group(1))] = log_dir / entry
    if not commits:
        raise RestoreError("_delta_log 中没有任何 JSON 提交，无法演练恢复")

    latest = max(commits)
    if latest != current_version:
        raise RestoreConflict(
            f"工单当前版 v{current_version} 与副本实际最新版 v{latest} 不一致："
            "当前版已变，请重新核对工单后再预检"
        )

    missing_commits = [v for v in range(current_version + 1) if v not in commits]
    if missing_commits:
        missing_text = ", ".join(f"v{v}" for v in missing_commits)
        raise RestoreError(
            f"JSON 提交从 0 版起不连续，缺 {missing_text}；"
            "本工具只处理提交连续的表"
        )

    parsed = [_parse_commit_full(v, commits[v])
              for v in range(current_version + 1)]
    _validate_protocols(parsed)
    _validate_metadata(parsed)
    _validate_no_dv_no_partitions(parsed)

    target_active = _replay(parsed, target_version)
    current_active = _replay(parsed, current_version)

    missing_files = sorted(p for p in target_active
                           if not (table_dir / p).is_file())
    if missing_files:
        raise MissingFilesError(missing_files)

    to_remove = sorted(p for p in current_active if p not in target_active)
    readd_paths = sorted(p for p in target_active if p not in current_active)
    return _RestorePlan(
        table_dir=table_dir,
        current_version=current_version,
        target_version=target_version,
        current_active=current_active,
        target_active=target_active,
        to_remove=to_remove,
        readd_paths=readd_paths,
    )


def _public_plan(plan: _RestorePlan) -> dict:
    return {
        "table": plan.table_dir.name,
        "current_version": plan.current_version,
        "target_version": plan.target_version,
        "new_version": plan.current_version + 1,
        "current_file_count": len(plan.current_active),
        "target_file_count": len(plan.target_active),
        "to_remove": [str(plan.current_active[p].get("path", p))
                      for p in plan.to_remove],
        "to_readd": [dict(plan.target_active[p], dataChange=True)
                     for p in plan.readd_paths],
    }


def build_restore_plan(table_dir, current_version: int,
                       target_version: int) -> dict:
    """预检：校验约束并返回恢复方案（不写任何文件）。"""
    return _public_plan(_plan_restore(Path(table_dir),
                                      current_version, target_version))


def _write_commit_atomic(log_dir: Path, version: int, payload: bytes) -> Path:
    """把新提交写完整后才让其可见；已存在同名提交则失败。

    先写临时文件并 fsync，再用硬链接原子生效：链接是“不存在才成功”的
    单点判定，即使确认请求落在不同服务进程，同一当前版也最多成功一笔。
    """
    final = log_dir / f"{version:020d}.json"
    tmp = log_dir / f".{final.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    try:
        with open(tmp, "xb") as fh:
            fh.write(payload)
            fh.flush()
            os.fsync(fh.fileno())
        try:
            os.link(tmp, final)
        except FileExistsError as exc:
            raise RestoreConflict(
                f"版本 {version} 的提交已存在（可能有另一笔恢复同时写入），"
                "本次恢复未生效"
            ) from exc
    finally:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass
    # 尽力而为的目录 fsync：崩溃后新提交依然可见
    try:
        dir_fd = os.open(log_dir, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except OSError:
        pass
    return final


def execute_restore(table_dir, current_version: int, target_version: int,
                    now_ms: int | None = None) -> dict:
    """确认恢复：追加下一版 JSON 提交，使新版本快照与目标版一致。

    重新纳入的文件沿用目标版的 add 字段（仅 dataChange 置为 true），
    移除动作同样标记 dataChange=true；旧日志与 Parquet 一律不改。
    """
    table_dir = Path(table_dir)
    # 确认时全量重新校验：当前版可能已变、文件可能新缺失
    plan = _plan_restore(table_dir, current_version, target_version)
    if now_ms is None:
        now_ms = int(time.time() * 1000)

    actions: list[dict] = []
    removed: list[str] = []
    for norm in plan.to_remove:
        raw_path = plan.current_active[norm].get("path", norm)
        actions.append({"remove": {"path": raw_path,
                                   "deletionTimestamp": now_ms,
                                   "dataChange": True}})
        removed.append(str(raw_path))
    readded: list[str] = []
    for norm in plan.readd_paths:
        add = dict(plan.target_active[norm])  # 沿用目标版 add 字段
        add["dataChange"] = True
        actions.append({"add": add})
        readded.append(str(add.get("path", norm)))

    payload = ("\n".join(json.dumps(action, ensure_ascii=False)
                         for action in actions) + "\n").encode("utf-8")
    new_version = plan.current_version + 1
    final = _write_commit_atomic(table_dir / "_delta_log", new_version, payload)

    return {
        "table": table_dir.name,
        "current_version": plan.current_version,
        "target_version": plan.target_version,
        "new_version": new_version,
        "commit_file": final.name,
        "actions_written": len(actions),
        "removed": removed,
        "readded": readded,
    }
