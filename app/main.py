"""公共卫生病例数差分隐私发布 API（FastAPI + SQLite）。

接口：
- POST /upload                    上传病例 CSV（整批校验，冲突整批拒绝；可带批次号）
- POST /batches/{batch_id}/isolate 隔离批次，返回受影响发布键
- GET  /batches                   批次列表
- GET  /batches/{batch_id}        批次详情（含受影响发布键）
- GET  /batches/{batch_id}/releases 受影响发布结果明细
- POST /publish                   按发布键与 ε 发布三区及全市计数（ε-差分隐私）
- GET  /releases                  发布结果列表（可按 date / batch_id 过滤）
- GET  /releases/{key}            按发布键查询
- GET  /budget/{date}             查询某数据日期的 ε 预算使用情况
"""
from __future__ import annotations

import datetime as dt
import os
import re
from contextlib import asynccontextmanager
from decimal import Decimal

from fastapi import FastAPI, File, Form, Header, HTTPException, Query, UploadFile
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field, field_validator

from .csv_import import CsvError, parse_csv
from .dp import LaplaceSampler
from .store import (
    BatchConflictError,
    BatchContentConflictError,
    BatchNotFoundError,
    BudgetExceededError,
    ReleaseParamConflictError,
    Store,
)

MAX_UPLOAD_BYTES = 20 * 1024 * 1024
MAX_BATCH_ID_LEN = 128
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

DESCRIPTION = """面向研究方的分区病例数发布服务。

**流程**：上传 CSV（可多次上传、可带修订号修订、可指定批次号）→ 按「发布键 + ε」发布
→ 查询发布结果；发现某批来源失真时可隔离该批并查询受影响的已发布计数。

**批次来源**：每次上传都是一个批次（可显式提供批次号，否则服务端生成）。
同批同内容重试沿用原批次；同批异文返回 409。相同记录可来自多个批次，保留各自来源；
同修订号异文仍整批拒绝（批内或与库内冲突）。

**差分隐私**：以“增删一名患者”为相邻数据集；对东/西/南三区计数分别叠加独立的
Laplace(1/ε) 噪声后取整并截断为非负整数；全市计数 = 三区公布值之和（后处理，
不额外消耗隐私预算）。整组结果满足 ε-差分隐私。

**预算**：每个数据日期总额度为 1；每个新发布键按其 ε 扣减当日额度，并发安全；
发布失败不扣额度。

**幂等**：同一发布键以相同参数（日期、ε）重试返回原结果；同键异参返回 409。

**快照与隔离**：发布固定受理时刻已提交的数据。隔离批次后，新发布按未隔离来源
重新选取每患者当日最高修订；已发布数值和预算额度不变。查询可列出发布时曾采用
该批记录的发布键。既无来源记录（旧库）继续供数；旧版本发布的来源标记为未知。

隔离与上传、发布交错时，来源归属、后续供数和受影响清单均以完整已提交状态为准；
隔离失败不留下部分隔离。
"""


class PublishIn(BaseModel):
    release_key: str = Field(
        min_length=1, max_length=128,
        description="发布键（幂等键）",
        examples=["daily-2026-11-01-a"],
    )
    date: dt.date = Field(description="数据日期 YYYY-MM-DD", examples=["2026-11-01"])
    epsilon: Decimal = Field(
        description="本次发布使用的隐私预算 ε（0<ε≤1）；每个数据日期总额度为 1",
        examples=["0.5"],
    )

    @field_validator("release_key", mode="before")
    @classmethod
    def _strip_key(cls, v):
        if isinstance(v, str):
            v = v.strip()
        return v

    @field_validator("epsilon", mode="before")
    @classmethod
    def _coerce_eps(cls, v):
        # JSON 浮点数按最短十进制表示转换，避免 0.1 变成二进制展开式
        if isinstance(v, float):
            return Decimal(str(v))
        return v

    @field_validator("epsilon")
    @classmethod
    def _check_eps(cls, v: Decimal):
        if not v.is_finite() or v <= 0 or v > 1:
            raise ValueError("epsilon 须满足 0 < ε ≤ 1")
        if float(v) == 0.0:
            raise ValueError("epsilon 过小，超出可处理范围")
        return v


def _check_date(date: str) -> None:
    if not DATE_RE.match(date):
        raise HTTPException(400, f"日期格式须为 YYYY-MM-DD：{date!r}")
    try:
        dt.date.fromisoformat(date)
    except ValueError:
        raise HTTPException(400, f"日期非法：{date!r}")


def _check_batch_id(batch_id: str) -> str:
    batch_id = batch_id.strip()
    if not batch_id:
        raise HTTPException(400, "批次号不能为空")
    if len(batch_id) > MAX_BATCH_ID_LEN:
        raise HTTPException(400, f"批次号超过 {MAX_BATCH_ID_LEN} 字符")
    return batch_id


def create_app(db_path: str | None = None) -> FastAPI:
    store = Store(db_path or os.environ.get("DB_PATH", "./dp.db"))

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        store.close()

    app = FastAPI(
        title="病例数差分隐私发布 API",
        version="1.1.0",
        description=DESCRIPTION,
        lifespan=lifespan,
    )
    app.state.store = store
    app.state.sampler = LaplaceSampler()

    @app.get("/", include_in_schema=False)
    def root():
        return RedirectResponse("/docs")

    @app.post("/upload", tags=["上传"], summary="上传病例 CSV（整批校验，冲突整批拒绝）")
    def upload(
        file: UploadFile = File(..., description="UTF-8 CSV：date, patient_id, district, revision"),
        batch_id: str | None = Form(
            None,
            description="批次号；不传则服务端生成。同批同内容重试沿用原批，异文冲突(409)",
        ),
        batch_id_query: str | None = Query(
            None, alias="batch_id", include_in_schema=False,
            description="同 batch_id，也可以查询参数形式提供",
        ),
        x_batch_id: str | None = Header(
            None, alias="X-Batch-Id", include_in_schema=False,
            description="同 batch_id，也可以请求头形式提供",
        ),
    ):
        supplied = batch_id or batch_id_query or x_batch_id
        if supplied is not None:
            supplied = _check_batch_id(supplied)
        data = file.file.read(MAX_UPLOAD_BYTES + 1)
        if not data:
            raise HTTPException(400, "上传文件为空")
        if len(data) > MAX_UPLOAD_BYTES:
            raise HTTPException(400, f"文件超过 {MAX_UPLOAD_BYTES // (1024 * 1024)}MB 上限")
        try:
            rows = parse_csv(data)
        except UnicodeDecodeError:
            raise HTTPException(400, "文件须为 UTF-8 编码的 CSV")
        except CsvError as e:
            raise HTTPException(400, detail={"message": "CSV 校验失败，整批未入库", "errors": e.errors})
        try:
            result = store.insert_batch(rows, batch_id=supplied)
        except BatchContentConflictError as e:
            raise HTTPException(409, detail={
                "message": f"批次号 {e.batch_id!r} 已存在且内容不同，整批未入库",
                "batch_id": e.batch_id,
            })
        except BatchConflictError as e:
            raise HTTPException(409, detail={
                "message": "与库内记录存在“同修订号异文”冲突，整批未入库",
                "conflicts": e.conflicts,
            })
        return {"status": "ok", "rows_received": len(rows), **result}

    @app.post("/batches/{batch_id}/isolate", tags=["批次"],
              summary="隔离批次（停用该批来源），返回受影响的发布键")
    def isolate_batch(batch_id: str):
        batch_id = _check_batch_id(batch_id)
        try:
            return store.isolate_batch(batch_id)
        except BatchNotFoundError:
            raise HTTPException(404, f"批次号 {batch_id!r} 不存在")

    @app.get("/batches", tags=["批次"], summary="批次列表（含隔离状态与受影响发布数）")
    def list_batches():
        return {"batches": store.list_batches()}

    @app.get("/batches/{batch_id}", tags=["批次"],
             summary="批次详情（含发布时曾采用该批的发布键）")
    def get_batch(batch_id: str):
        batch_id = _check_batch_id(batch_id)
        info = store.get_batch(batch_id)
        if info is None:
            raise HTTPException(404, f"批次号 {batch_id!r} 不存在")
        return info

    @app.get("/batches/{batch_id}/releases", tags=["批次"],
             summary="受影响发布结果明细（发布时曾采用该批记录）")
    def get_batch_releases(batch_id: str):
        batch_id = _check_batch_id(batch_id)
        info = store.get_batch_releases(batch_id)
        if info is None:
            raise HTTPException(404, f"批次号 {batch_id!r} 不存在")
        return info

    @app.post("/publish", tags=["发布"], summary="按发布键与 ε 发布三区及全市计数")
    def publish(req: PublishIn):
        date_s = req.date.isoformat()
        try:
            release, replay = store.publish(
                req.release_key, date_s, req.epsilon, app.state.sampler.sample
            )
        except ReleaseParamConflictError as e:
            raise HTTPException(409, detail=str(e))
        except BudgetExceededError as e:
            raise HTTPException(429, detail={
                "message": str(e),
                "date": e.date,
                "requested_epsilon": str(e.requested),
                "remaining_budget": str(e.remaining),
            })
        return {**release, "replay": replay}

    @app.get("/releases", tags=["查询"], summary="发布结果列表（可按日期、来源批次过滤）")
    def list_releases(date: str | None = None, batch_id: str | None = None):
        if date is not None:
            _check_date(date)
        if batch_id is not None:
            batch_id = _check_batch_id(batch_id)
            if store.get_batch(batch_id) is None:
                raise HTTPException(404, f"批次号 {batch_id!r} 不存在")
        return {"releases": store.list_releases(date, batch_id=batch_id)}

    @app.get("/releases/{release_key}", tags=["查询"], summary="按发布键查询结果")
    def get_release(release_key: str):
        rel = store.get_release(release_key)
        if rel is None:
            raise HTTPException(404, f"发布键 {release_key!r} 不存在")
        return rel

    @app.get("/budget/{date}", tags=["查询"], summary="查询某数据日期的 ε 预算使用情况")
    def get_budget(date: str):
        _check_date(date)
        return store.get_budget(date)

    return app


app = create_app()
