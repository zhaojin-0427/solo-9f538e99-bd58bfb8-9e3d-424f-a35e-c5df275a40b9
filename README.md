# 病例数差分隐私发布 API

公共卫生数据员向研究方发布某日各区病例数的服务：FastAPI + SQLite，
支持 CSV 上传（含修订与批次来源）、按「发布键 + ε」的差分隐私发布、结果与预算查询，
以及批次隔离（停用来源失真批次并找出受影响的已发布计数）。
发布结果落库即固定，后续修订、批次隔离与服务重启均不改变已发布结果。

## 快速开始

```bash
docker compose up --build
# 打开 http://localhost:8000/docs （Swagger UI，可直接上传、隔离、发布、查询）
```

本地运行（Python 3.11+）：

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

自测（125 项关键情形检查）：

```bash
pip install -r requirements.txt -r requirements-dev.txt
python3 tests/selftest.py
```

## CSV 格式

四列：`date, patient_id, district, revision`（日期、患者代号、区号、修订号）。

- 表头可选：支持英文（`date,patient_id,district,revision`）或中文
  （`日期,患者代号,区号,修订号`）表头，列顺序不限；无表头时按上述固定顺序解析。
- `date`：`YYYY-MM-DD`；`patient_id`：非空字符串（≤128 字符）；
- `district`：限 `东/西/南`（兼容 `E/W/S`、`east/west/south`，大小写不敏感，入库归一化）；
- `revision`：正整数。同一患者同一日期可多次修订，**统计时只采用最高修订号**。
- 文件须为 UTF-8（兼容 BOM），空行忽略；上限 20MB / 20 万行。

**整批原子性**：任何一行非法，或出现「同一患者同日同修订号但区号不同」
（批内或与库内已有记录冲突），整批拒绝（分别为 400 / 409），不产生部分入库。
内容完全相同的重复行幂等跳过。

## 批次与来源

每次上传都记为一个批次。批次号通过 multipart 表单字段 `batch_id` 提供
（`curl` 用 `-F "batch_id=..."`；也接受查询参数或 `X-Batch-Id` 请求头）；
不提供时服务端生成 `batch-<uuid>`，并在响应中返回。

- **同批同内容重试沿用原批次**：返回 `batch_reused: true`，不重复写入记录，
  也不改变该批原有的隔离状态。
- **同批异文冲突**：同一批次号的内容指纹（行集合规范化后）不同 → 409，整批不入库。
- **相同记录来自多批**：记录只存一份（`同修订异文` 仍会整批拒绝），但保留每个批次的
  来源关联；隔离其中一批时，只要还有未隔离来源，该记录继续供数。
- **入队即定**：批次建立、记录写入与来源关联在同一 `BEGIN IMMEDIATE` 事务内完成；
  冲突或任何异常整体回滚，不留下批次元数据。

## 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/upload` | multipart 上传 CSV（+ 可选 `batch_id`），整批校验入库 |
| POST | `/batches/{batch_id}/isolate` | 隔离批次，返回受影响的发布键（幂等） |
| GET | `/batches` | 批次列表（隔离状态、记录数、受影响发布数） |
| GET | `/batches/{batch_id}` | 批次详情（含受影响发布键） |
| GET | `/batches/{batch_id}/releases` | 受影响发布结果明细 |
| POST | `/publish` | JSON `{release_key, date, epsilon}`，发布三区及全市计数 |
| GET | `/releases` | 发布结果列表，可选 `?date=` / `?batch_id=` 过滤 |
| GET | `/releases/{release_key}` | 按发布键查询 |
| GET | `/budget/{date}` | 查询该数据日期的 ε 预算使用情况 |

### 发布语义

- **差分隐私**：以「增删一名患者」为相邻数据集。一名患者同日只计一次，
  故三区计数向量的 L1 敏感度为 1；对东/西/南分别叠加独立的 Laplace(1/ε)
  噪声，取整并截断为非负整数，整组结果满足 ε-差分隐私。
  **全市 = 三区公布值之和**（后处理，不额外消耗预算，保证恒等）。
- **预算**：`0 < ε ≤ 1`；每个数据日期总额度为 1，按日发布键的 ε 逐个扣减。
  预算检查与扣减在同一 `BEGIN IMMEDIATE` 事务内完成，**并发发布不会超额**；
  发布失败（含预算不足、服务内部错误）整体回滚，**不消耗额度、不占用发布键**。
- **幂等**：同一发布键以相同参数（日期、ε 数值相等，如 0.5 与 0.50）重试，
  返回首次发布的结果（`replay: true`），不重复扣额度；同键异参返回 409。
- **独立取噪**：不同发布键各自独立采样噪声，并各自消耗当日额度。
- **快照**：发布固定受理时刻已提交的数据；之后的修订、新增、批次隔离与重启
  均不影响已发布结果。
- **来源快照**：发布时把实际采用记录所关联的、当时未隔离的批次记入
  `release_sources`；响应的 `sources` 列出这些批次号。
  旧版本（无批次表）留下的发布记录 `source_known: false`、`sources: null`。

### 批次隔离语义

- **新发布重新选源**：隔离后，新发布在**未隔离来源**中重新选取每位患者当日的最高
  修订；某记录的全部来源批次都被隔离后，该记录不再供数。
- **已发布不变**：已发布的数值与已用 ε 额度都不改。
- **受影响清单**：隔离响应与批次查询列出「发布时曾采用该批记录」的发布键
  （`affected_release_keys` / `?batch_id=` 过滤），供数据员复核；这只是线索，
  不代表该发布的数值一定改变。
- **既有无来源记录继续供数**：旧库迁移而来的记录（`legacy=1`，无任何批次关联）
  不受隔离影响。
- **幂等**：重复隔离返回 `already_isolated: true`，清单仍可查询。
- **失败无部分隔离**：隔离标记更新与受影响清单读取在同一事务内；
  任何异常整体回滚，批次保持未隔离。

### 交错一致性

上传、隔离、发布都是单条 `BEGIN IMMEDIATE` 写事务：

- 发布先提交 → 该发布的 `sources` 含此批，随后隔离的受影响清单包含该发布；
- 隔离先提交 → 该发布的快照已看不到此批，`sources` 不含它，也不在受影响清单中。

两种情况都以完整已提交状态为准，不会出现「归属撕裂」或部分隔离。
查询接口在单连接锁内读取，清单与实际提交状态一致。

### 示例

```bash
curl -F "file=@cases.csv" -F "batch_id=batch-20261118-a" http://localhost:8000/upload
curl -X POST -H 'Content-Type: application/json' \
     -d '{"release_key":"daily-2026-11-18-a","date":"2026-11-18","epsilon":"0.5"}' \
     http://localhost:8000/publish
# => {"release_key":"...","date":"2026-11-18","epsilon":"0.5",
#     "counts":{"东":..,"西":..,"南":..,"全市":..},"created_at":"...",
#     "source_known":true,"sources":["batch-20261118-a"],"replay":false}
curl -X POST http://localhost:8000/batches/batch-20261118-a/isolate
# => {"batch_id":"...","isolated":true,"already_isolated":false,"isolated_at":"...",
#     "affected_release_keys":["daily-2026-11-18-a"],"affected_release_count":1,
#     "unknown_provenance_release_count":0}
curl "http://localhost:8000/releases?batch_id=batch-20261118-a"
curl http://localhost:8000/budget/2026-11-18
```

## 持久化与部署

- SQLite 文件路径由环境变量 `DB_PATH` 指定（容器内默认 `/data/dp.db`，
  compose 挂载命名卷 `dp_data`）；本地默认 `./dp.db`。
- 预算以十进制字符串存储、用 `Decimal` 精确计算，避免浮点误差导致超额。
- 启动时自动迁移旧库：为既有 `records` 补 `legacy=1`（来源未知、始终供数），
  为既有 `releases` 补 `source_known=0`（来源未知）。
- 单实例部署（单 uvicorn 进程）；多副本共享同一 SQLite 文件时靠
  `BEGIN IMMEDIATE` 串行化写事务仍能保证不超额与无部分隔离，但未就此做专门压力测试。

## 已知限制（未实现项）

- 未做认证/授权与 HTTPS，面向研究方发布前需自行加网关或反向代理。
- 未提供记录删除/订正接口（修订通过更高修订号的新记录表达）。
- 隔离只停用来源，不物理删除记录；不支持解除隔离（重试上传不会解除）。
- 旧版本发布的来源不可追溯，只能标记为未知，不参与受影响清单。
- 无表头 CSV 的列顺序固定为 `date,patient_id,district,revision`，不做自动嗅探。

