# Delta Lake 版本可恢复性分析网页

对象存储误删文件后，湖仓值班员用本工具查清一张 Delta Lake 表还有哪些
版本可以完整回退。值班员在网页上填写版本区间，上传三类材料：

1. `_delta_log` 中留存的 **JSON 提交**（`NNN.json`，20 位补零版本号）；
2. 留存的**经典单文件 Parquet 检查点**（`NNN.checkpoint.parquet`，用 PyArrow 读取）；
3. **现存数据文件的表内相对路径清单**（每行一个路径，如 `data/part-00000-….parquet`）。

工具逐版从 **版本 0 的提交**或**不晚于该版的最新检查点**出发，连续重放
`add` / `remove` 动作到目标版本，再把重建出的快照引用文件与现存清单核对：

- 🟢 **可重建 · 文件齐全**：重放连续无断档，且快照引用的数据文件全部现存；
- 🔴 **缺失提交，重放中断**：明确给出缺失的提交号（版本号列表）；
- 🟠 **重放完整但缺数据文件**：明确给出每个缺失的数据文件表内相对路径。

**仅处理无删除向量（Deletion Vector）的表**：协议特性或任何 add/remove
动作中出现 `deletionVector` 时直接拒绝分析，并指出来源文件。

页面另有**本地副本恢复演练**入口：在可写样例表上填写工单上的当前版本
和要恢复的旧版本，工具会在 `_delta_log` 追加下一版 JSON，让新版本引用的
文件与目标版本一致。详见下文“恢复演练”。

## 目录结构

```
app/
  analyzer.py            # 核心逻辑：日志解析、逐版重放、文件核对、DV 拦截
  recovery.py            # 恢复演练：版本确认、flock 并发控制、原子追加提交
  web.py                 # Flask 路由：上传分析 + 本地样例 API + 恢复 API
  templates/index.html   # 单页界面（服务端渲染与 JS 渲染共用）
scripts/generate_samples.py  # 用 PyArrow 构造两套样例 Delta 表
samples/                 # 本地样例（健康表 / 误删事故表；健康表可写可演练）
tests/                   # pytest：核心逻辑、恢复演练与路由测试
Dockerfile / docker-compose.yml
```

## 用 Docker Compose 启动

```bash
docker compose up --build
# 打开 http://localhost:8000
```

`docker-compose.yml` 把宿主机 `samples/` **可写**挂载到容器内
`/srv/samples`，恢复演练写入的新提交会同步回宿主机，容器重启后仍可
在“原核查页”重放。镜像默认以 root 运行；若改为非 root 用户，需保证
挂载目录对该用户可写。

页面下半部有两个按钮可**直接加载本地样例**，无需手动选择文件：

- **健康表样例（v0–v4）**：含 v2 检查点，5 个版本全部可重建；
- **误删事故样例（v0–v5）**：v3 提交丢失、数据文件 `part-00004` /
  `part-00006` 被删，但 v2/v4 检查点留存。预期结论：

  | 版本 | 结论 | 说明 |
  |---|---|---|
  | v0–v2 | 🟢 可重建 | v2 起以 `checkpoint-2` 为基线 |
  | v3 | 🔴 缺提交 | 缺提交号 v3 |
  | v4 | 🟠 缺数据文件 | 缺 `data/part-00004-incident.parquet`（v4 检查点仍引用它） |
  | v5 | 🟠 缺数据文件 | 缺 `part-00004` 与 `part-00006` |

  目录中还放了一个分片检查点残片，工具会明确列为“已忽略：仅支持经典
  单文件检查点”。

## 恢复演练

页面第 3 节选择**可写样例表**（健康表），填写工单上的“当前版本”和
“要恢复的旧版本”，点击“执行恢复演练”。也可直接调用接口：

```bash
# 按表名（SAMPLE_ROOT 下的目录名）
curl -X POST http://localhost:8000/api/recover-sample/healthy_table \
  -H 'Content-Type: application/json' \
  -d '{"current_version": 4, "target_version": 2}'

# 或统一入口，table_path 允许指向挂载进 SAMPLE_ROOT 的其他本地副本
curl -X POST http://localhost:8000/api/recover \
  -H 'Content-Type: application/json' \
  -d '{"table": "healthy_table", "current_version": 4, "target_version": 2}'
```

适用面（不满足时明确拒绝，不写任何日志）：

- `_delta_log` 中从 **v0 起 JSON 提交连续**，不依赖检查点作为恢复起点；
- **表元数据（`metaData`）不变**，且无分区列（`partitionColumns` 为空）；
- 协议固定为 `minReaderVersion=1`、`minWriterVersion=2`，且无协议特性；
- 无删除向量。

恢复动作：

1. 在表目录 `.recovery.lock` 上加 `flock` 排它锁，锁内重新扫描并解析
   全部 JSON 提交，算出实际当前版本；
2. 工单填写的当前版本与实际不一致 → `409 stale_current_version` 拒绝；
   目标版本必须严格小于当前版本；
3. 重放目标版本快照，逐路径核对 Parquet 是否在本副本中实际存在；
   缺件时返回 `422 missing_parquet` 并列出**每个缺失的表内相对路径**，
   **不写日志**（不会为了让日志“一致”而引用不存在的文件）；
4. 计算当前快照与目标快照的差异：
   - 当前有、目标没有 → 写 `remove`（`deletionTimestamp` 为当前时间）；
   - 目标有、当前没有 → 写 `add`，**沿用目标版本该文件的完整 add 字段**，
     只把 `dataChange` 置为 `true`；
   - 两类动作都标 `dataChange=true`；
   - 已相同的文件不重写；
5. 新提交先写临时文件并 `fsync`，再以硬链接（不支持时退回原子 rename）
   落成 `NNNNNNNNNNNNNNNNNNNN.json`，目标文件已存在则拒绝；任何一步失败
   都不会留下半截提交或覆盖旧日志；
6. 响应给出新版本号、重新纳入 / 移出的文件清单，并立即用**原核查逻辑**
   重放 `目标版本 – 新版本`，返回 `verification` 报告（页面同步刷新，
   “原核查页能重放该版本”）。

并发保证：锁文件与原子落盘在**同一台机器上的服务进程之间**生效。
`docker-compose.yml` 用单副本、单挂载目录部署；确认请求无论落到哪个
worker（gunicorn 多 worker 亦同），同一个当前版本最多成功一笔，其余
请求收到 `409`。接口成功后把页面“工单当前版本”推进到新版本，下一次
演练需按新版本填写。若部署成跨主机的多副本共享同一目录，需要底层
文件系统提供可靠的 `flock` 与原子链接语义（普通本地卷 / NFSv4 可以，
部分对象存储 FUSE 挂载不保证）。

旧日志文件与 Parquet 数据文件在整个恢复过程中只读。

## 手工上传分析

1. 填写“起始版本 / 结束版本”；
2. 多选上传 `_delta_log` 里留存的 `.json` 与 `.checkpoint.parquet`
   （文件名必须保持 20 位补零版本号，上传时带不带目录前缀都可以）；
3. 上传现存数据文件清单（支持 `#` 注释、空行、`./` 前缀、URI 编码路径，
   两侧路径会做同一归一化）；不上传清单则所有引用文件都判为缺失；
4. 提交后得到总览统计、输入材料盘点和逐版核对明细（可展开查看每个
   引用文件的 ✓ / ✗ 状态）。

也可以只上传检查点：对不晚于检查点版本的目标版本，直接以检查点中的
add 列为快照基线；目标版本更新时，则从检查点版本 +1 起连续重放提交，
中途缺任一提交即报缺失提交号。检查点里的 remove 是墓碑，不属于快照。

恢复演练中的并发与原子性也依赖本地文件系统的 `flock` / 硬链接（或原子
`rename`）语义；Docker Compose 本地卷满足这些前提。

## 本地开发（无 Docker 时）

```bash
python -c 'import ensurepip' || echo '需先安装 python3-venv 或再引导 pip'
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt pytest
python -I scripts/generate_samples.py
gunicorn --bind 0.0.0.0:8000 app.web:app
python -I -m pytest tests -q
```

## 设计取舍

- **基线选择**：优先使用不晚于目标版本的最新检查点（重放距离最短），
  无检查点时才要求版本 0 的提交；
- **逐版独立重放**：每个目标版本都从基线完整重放一次，缺提交不会污染
  其他版本的结论，报告中的“重放起点”对每版透明；
- **只认经典单文件检查点**：分片检查点（`N.checkpoint.K.M.parquet`）
  与 `_last_checkpoint` 指针不参与重放，只在页面登记忽略原因；
- **DV 硬拦截**：删除向量会让“文件存在 ≠ 行数据完整”，超出本工具
  基于文件清单的核对能力，因此在协议层和动作层双重检测并拒绝；
- **恢复只追加、不回滚**：新提交让最新快照与目标版本一致，旧日志和
  Parquet 原样保留，因此错单所在版本与目标版本都仍可回查 / 重放；
- **文件缺件不将就**：目标版本引用的 Parquet 缺失时直接拒绝，
  不写一份引用不存在文件的日志；本地副本演练以“可重放”为前提；
- **确认与写入同锁**：版本确认不是前端传来的“信任输入”，而是在持锁
  后重新扫描日志得到，避免并发请求各自基于过期版本写入；
- **恢复即“原地换快照”**：新版本只描述文件集合差异，不重建检查点，
  也不移动数据文件；核查页仍按原规则重放，所以旧版可回查、新版可复核。
