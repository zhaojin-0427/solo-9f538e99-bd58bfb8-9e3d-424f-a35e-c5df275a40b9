# 病例数差分隐私发布 API

公共卫生数据员向研究方发布某日各区病例数的服务：FastAPI + SQLite，
支持 CSV 上传（含修订）、按「发布键 + ε」的差分隐私发布、结果与预算查询。
发布结果落库即固定，后续修订与服务重启均不改变已发布结果。

## 快速开始

```bash
docker compose up --build
# 打开 http://localhost:8000/docs （Swagger UI，可直接上传、发布、查询）
```

本地运行（Python 3.11+）：

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

自测（69 项关键情形检查）：

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

## 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/upload` | multipart 上传 CSV，整批校验入库 |
| POST | `/publish` | JSON `{release_key, date, epsilon}`，发布三区及全市计数 |
| GET | `/releases` | 发布结果列表，可选 `?date=YYYY-MM-DD` 过滤 |
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
- **快照**：发布固定受理时刻已提交的数据；之后的修订、新增与重启均不影响已发布结果。

### 示例

```bash
curl -F "file=@cases.csv" http://localhost:8000/upload
curl -X POST -H 'Content-Type: application/json' \
     -d '{"release_key":"daily-2026-11-18-a","date":"2026-11-18","epsilon":"0.5"}' \
     http://localhost:8000/publish
# => {"release_key":"...","date":"2026-11-18","epsilon":"0.5",
#     "counts":{"东":..,"西":..,"南":..,"全市":..},"created_at":"...","replay":false}
curl http://localhost:8000/budget/2026-11-18
```

## 持久化与部署

- SQLite 文件路径由环境变量 `DB_PATH` 指定（容器内默认 `/data/dp.db`，
  compose 挂载命名卷 `dp_data`）；本地默认 `./dp.db`。
- 预算以十进制字符串存储、用 `Decimal` 精确计算，避免浮点误差导致超额。
- 单实例部署（单 uvicorn 进程）；多副本共享同一 SQLite 文件时靠
  `BEGIN IMMEDIATE` 串行化写事务仍能保证不超额，但未就此做专门测试。

## 已知限制（未实现项）

- 未做认证/授权与 HTTPS，面向研究方发布前需自行加网关或反向代理。
- 未提供记录删除/订正接口（修订通过更高修订号的新记录表达）。
- 无表头 CSV 的列顺序固定为 `date,patient_id,district,revision`，不做自动嗅探。
