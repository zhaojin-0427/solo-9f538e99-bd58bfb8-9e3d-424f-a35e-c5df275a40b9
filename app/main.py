"""公共卫生病例数差分隐私发布 API（FastAPI + SQLite）。

接口：
- POST /upload            上传病例 CSV（整批校验，冲突整批拒绝）
- POST /publish           按发布键与 ε 发布三区及全市计数（ε-差分隐私）
- GET  /releases          发布结果列表（可按 date 过滤）
- GET  /releases/{key}    按发布键查询
- GET  /budget/{date}     查询某数据日期的 ε 预算使用情况
"""
from __future__ import annotations

import datetime as dt
import os
import re
from contextlib import asynccontextmanager
from decimal import Decimal

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field, field_validator

from .csv_import import CsvError, parse_csv
from .dp import LaplaceSampler
from .store import (
    BatchConflictError,
    BudgetExceededError,
    ReleaseParamConflictError,
    Store,
)

MAX_UPLOAD_BYTES = 20 * 1024 * 1024
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

DESCRIPTION = """面向研究方的分区病例数发布服务。

**流程**：上传 CSV（可多次上传、可带修订号修订）→ 按「发布键 + ε」发布 → 查询发布结果。

**差分隐私**：以“增删一名患者”为相邻数据集；对东/西/南三区计数分别叠加独立的
Laplace(1/ε) 噪声后取整并截断为非负整数；全市计数 = 三区公布值之和（后处理，
不额外消耗隐私预算）。整组结果满足 ε-差分隐私。

**预算**：每个数据日期总额度为 1；每个新发布键按其 ε 扣减当日额度，并发安全；
发布失败不扣额度。

**幂等**：同一发布键以相同参数（日期、ε）重试返回原结果；同键异参返回 409。

**快照**：发布固定受理时刻已提交的数据；之后的修订与服务重启均不改变已发布结果。
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


def create_app(db_path: str | None = None) -> FastAPI:
    store = Store(db_path or os.environ.get("DB_PATH", "./dp.db"))

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        store.close()

    app = FastAPI(
        title="病例数差分隐私发布 API",
        version="1.0.0",
        description=DESCRIPTION,
        lifespan=lifespan,
    )
    app.state.store = store
    app.state.sampler = LaplaceSampler()

    @app.get("/", include_in_schema=False)
    def root():
        return RedirectResponse("/docs")

    @app.post("/upload", tags=["上传"], summary="上传病例 CSV（整批校验，冲突整批拒绝）")
    def upload(file: UploadFile = File(..., description="UTF-8 CSV：date, patient_id, district, revision")):
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
            result = store.insert_batch(rows)
        except BatchConflictError as e:
            raise HTTPException(409, detail={
                "message": "与库内记录存在“同修订号异文”冲突，整批未入库",
                "conflicts": e.conflicts,
            })
        return {"status": "ok", "rows_received": len(rows), **result}

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

    @app.get("/releases", tags=["查询"], summary="发布结果列表（可按日期过滤）")
    def list_releases(date: str | None = None):
        if date is not None:
            _check_date(date)
        return {"releases": store.list_releases(date)}

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
