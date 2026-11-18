"""关键情形自测脚本。

运行：python3 tests/selftest.py
覆盖：上传校验、最高修订去重、同修订异文整批拒绝、幂等发布、预算并发、
      失败回滚、重启持久化、快照语义、真实噪声性质、拉普拉斯采样统计。
"""
from __future__ import annotations

import os
import socket
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi.testclient import TestClient

from app.dp import LaplaceSampler
from app.main import create_app

DONE = []


def check(name, cond, extra=""):
    if not cond:
        raise AssertionError(f"FAILED: {name} | {extra}")
    DONE.append(name)
    print(f"PASS  {name}")


class ZeroSampler:
    def sample(self, scale):
        return 0.0


class SequenceSampler:
    """按预设序列返回噪声（线程安全），用于验证独立取噪与取整截断。"""

    def __init__(self, values):
        self._values = list(values)
        self._i = 0
        self._lock = threading.Lock()

    def sample(self, scale):
        with self._lock:
            v = self._values[self._i % len(self._values)]
            self._i += 1
        return v


class RaisingSampler:
    def sample(self, scale):
        raise RuntimeError("模拟噪声发生器故障")


def make_client(sampler=None, db=None, **kw):
    tmp = tempfile.TemporaryDirectory()
    app = create_app(db or os.path.join(tmp.name, "t.db"))
    if sampler is not None:
        app.state.sampler = sampler
    return tmp, app, TestClient(app, **kw)


def post_csv(client, text):
    return client.post("/upload", files={"file": ("data.csv", text.encode("utf-8"), "text/csv")})


def pub(client, key, date, eps):
    return client.post("/publish", json={"release_key": key, "date": date, "epsilon": eps})


D1, D2 = "2026-11-01", "2026-11-02"

# ---------- 1. 文档与接口清单 ----------
tmp, app, c = make_client()
check("/docs 可访问", c.get("/docs").status_code == 200)
r = c.get("/openapi.json")
check("/openapi.json 可访问", r.status_code == 200)
paths = r.json()["paths"]
check("上传/发布/查询接口齐全",
      all(p in paths for p in ("/upload", "/publish", "/releases", "/releases/{release_key}", "/budget/{date}")))

# ---------- 2. 上传与“同一患者同日只采用最高修订” ----------
tmp, app, c = make_client()
csv1 = (
    "date,patient_id,district,revision\n"
    f"{D1},P1,东,1\n"
    f"{D1},P1,西,2\n"   # P1 最高修订为 2 → 计入西
    f"{D1},P2,西,1\n"
    f"{D1},P3,南,1\n"
    f"{D2},P1,东,1\n"
)
r = post_csv(c, csv1)
check("合法 CSV 上传成功", r.status_code == 200 and r.json()["inserted"] == 5, r.text)
snap = app.state.store.snapshot_counts(D1)
check("同一患者同日只采用最高修订", snap == {"东": 0, "西": 2, "南": 1}, snap)
check("不同日期独立统计", app.state.store.snapshot_counts(D2) == {"东": 1, "西": 0, "南": 0})

# ---------- 3. 中文表头 / 英文区号别名 / 无表头 / BOM ----------
tmp, app, c = make_client()
r = post_csv(c, "日期,患者代号,区号,修订号\n2026-11-05,P1,E,1\n2026-11-05,P2,west,1\n")
check("中文表头+英文区号别名", r.status_code == 200 and r.json()["inserted"] == 2, r.text)
check("别名归一化", app.state.store.snapshot_counts("2026-11-05") == {"东": 1, "西": 1, "南": 0})
r = post_csv(c, "2026-11-06,P9,S,3\n")
check("无表头按固定顺序解析", r.status_code == 200
      and app.state.store.snapshot_counts("2026-11-06")["南"] == 1, r.text)
r = c.post("/upload", files={"file": ("d.csv", "﻿date,patient_id,district,revision\n2026-11-07,P1,东,1\n".encode("utf-8"), "text/csv")})
check("带 BOM 的 UTF-8 CSV", r.status_code == 200, r.text)

# ---------- 4. 同修订异文：批内与跨批，均整批拒绝 ----------
tmp, app, c = make_client()
r = post_csv(c, f"{D1},P1,东,1\n{D1},P1,西,1\n{D1},P2,东,1\n")
check("批内同修订异文整批拒绝(400)", r.status_code == 400, r.text)
check("整批拒绝后库内无残留", app.state.store.dump_records() == [])
check("首批入库", post_csv(c, f"{D1},P1,东,1\n").status_code == 200)
r = post_csv(c, f"{D1},P1,西,1\n{D1},P3,南,1\n")
check("与库内同修订异文冲突(409)", r.status_code == 409, r.text)
recs = app.state.store.dump_records()
check("冲突批整体回滚（含合法行）", len(recs) == 1 and recs[0]["district"] == "东", recs)
r = post_csv(c, f"{D1},P1,东,1\n")
check("相同记录重复上传幂等", r.status_code == 200
      and r.json()["inserted"] == 0 and r.json()["skipped_duplicates"] == 1, r.text)

# ---------- 5. 非法行整批拒绝 ----------
tmp, app, c = make_client()
bad = (
    "date,patient_id,district,revision\n"
    "2026-13-01,P1,东,1\n"   # 非法日期
    f"{D1},P2,北,1\n"        # 非法区号
    f"{D1},P3,东,0\n"        # 修订号 0
    f"{D1},P4,东,-2\n"       # 负数修订号
    f"{D1},P5,东,1.5\n"      # 非整数修订号
    f"{D1},,东,1\n"          # 空患者代号
)
r = post_csv(c, bad)
check("非法行整批拒绝(400)", r.status_code == 400, r.text)
check("错误信息覆盖各类问题", len(r.json()["detail"]["errors"]) >= 6, r.text)
check("非法批无残留", app.state.store.dump_records() == [])

# ---------- 6/7. 发布（零噪声）、幂等重试、异参冲突 ----------
tmp, app, c = make_client(sampler=ZeroSampler())
post_csv(c, f"{D1},P1,东,1\n{D1},P1,西,2\n{D1},P2,西,1\n{D1},P3,南,1\n")
r = pub(c, "K1", D1, "0.5")
j = r.json()
check("发布成功", r.status_code == 200, r.text)
check("零噪声发布等于真实计数", j["counts"] == {"东": 0, "西": 2, "南": 1, "全市": 3}, j)
check("全市等于三区之和", j["counts"]["全市"] == j["counts"]["东"] + j["counts"]["西"] + j["counts"]["南"])
check("首次发布非重放", j["replay"] is False)
b = c.get(f"/budget/{D1}").json()
check("预算扣减 0.5", b["used"] == "0.5" and b["remaining"] == "0.5", b)

r = pub(c, "K1", D1, "0.5")
check("同键同参重试返回原结果", r.status_code == 200
      and r.json()["counts"] == j["counts"] and r.json()["replay"] is True, r.text)
r = pub(c, "K1", D1, "0.50")
check("ε 写法不同但数值相等仍视为同参", r.status_code == 200 and r.json()["replay"] is True, r.text)
check("重试不重复扣额度", c.get(f"/budget/{D1}").json()["used"] == "0.5")
check("同键异 ε 冲突(409)", pub(c, "K1", D1, "0.4").status_code == 409)
check("同键异日期冲突(409)", pub(c, "K1", D2, "0.5").status_code == 409)

# ---------- 8. 预算上限、失败不耗额度、日期间独立 ----------
tmp, app, c = make_client(sampler=ZeroSampler())
check("ε=0.6 成功", pub(c, "A", D1, "0.6").status_code == 200)
r = pub(c, "B", D1, "0.5")
check("超额被拒(429)", r.status_code == 429, r.text)
check("失败不耗额度", c.get(f"/budget/{D1}").json()["used"] == "0.6")
check("失败键未留存", c.get("/releases/B").status_code == 404)
check("剩余额度内可发布", pub(c, "C", D1, "0.4").status_code == 200)
b = c.get(f"/budget/{D1}").json()
check("额度恰好用完(=1)", Decimal(b["used"]) == 1 and Decimal(b["remaining"]) == 0, b)
check("额度用尽后新发布被拒", pub(c, "D", D1, "0.000001").status_code == 429)
check("其他日期预算独立", pub(c, "E", D2, "1").status_code == 200)

# ---------- 9. 不同键独立取噪、取整截断 ----------
tmp, app, c = make_client(sampler=SequenceSampler([10.4, -3.2, 0.49, 2.5, 2.5, 0.5]))
r1 = pub(c, "N1", D1, "0.4").json()
r2 = pub(c, "N2", D1, "0.4").json()
check("不同键独立取噪", r1["counts"] != r2["counts"], (r1, r2))
check("取整并截断为非负整数", r1["counts"] == {"东": 10, "西": 0, "南": 0, "全市": 10}, r1)
check("第二组噪声取值独立", r2["counts"] == {"东": 2, "西": 2, "南": 0, "全市": 4}, r2)
check("两次发布均扣额度", Decimal(c.get(f"/budget/{D1}").json()["used"]) == Decimal("0.8"))

# ---------- 10. 并发发布不超额（真实 uvicorn 服务 + 20 线程） ----------
import httpx  # noqa: E402
import uvicorn  # noqa: E402

tmp = tempfile.TemporaryDirectory()
app_conc = create_app(os.path.join(tmp.name, "c.db"))
app_conc.state.sampler = ZeroSampler()
sock = socket.socket()
sock.bind(("127.0.0.1", 0))
port = sock.getsockname()[1]
sock.close()
server = uvicorn.Server(uvicorn.Config(app_conc, host="127.0.0.1", port=port, log_level="error"))
srv_thread = threading.Thread(target=server.run, daemon=True)
srv_thread.start()
for _ in range(200):
    if server.started:
        break
    time.sleep(0.05)
check("并发测试服务已启动", server.started)
with httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=10) as hc:
    def one(i):
        return hc.post("/publish", json={"release_key": f"T{i}", "date": D1, "epsilon": "0.1"}).status_code
    with ThreadPoolExecutor(max_workers=20) as ex:
        codes = list(ex.map(one, range(20)))
    ok, rej = codes.count(200), codes.count(429)
    check("并发下恰好 10 个成功(10×0.1=1)", ok == 10, codes)
    check("超额请求全部被拒(429)", rej == 10, codes)
    b = hc.get(f"/budget/{D1}").json()
    check("并发后总额度恰为 1、未超额", Decimal(b["used"]) == 1, b)
    check("发布结果数与成功数一致", len(hc.get("/releases").json()["releases"]) == 10)
server.should_exit = True
srv_thread.join(timeout=10)

# ---------- 11. 重启不改旧结果；后续修订不改旧发布、新发布取新快照 ----------
tmpdir = tempfile.TemporaryDirectory()
db = os.path.join(tmpdir.name, "r.db")
app1 = create_app(db)
app1.state.sampler = ZeroSampler()
with TestClient(app1) as c1:
    post_csv(c1, f"{D1},P1,东,1\n{D1},P2,西,1\n")
    j1 = pub(c1, "RK", D1, "0.5").json()
app2 = create_app(db)  # 模拟重启：同一数据库文件、全新应用实例
app2.state.sampler = ZeroSampler()
with TestClient(app2) as c2:
    r = c2.get("/releases/RK")
    check("重启后旧发布结果不变", r.status_code == 200 and r.json()["counts"] == j1["counts"], r.text)
    check("重启后预算记录仍在", c2.get(f"/budget/{D1}").json()["used"] == "0.5")
    post_csv(c2, f"{D1},P1,南,2\n{D1},P3,东,1\n")  # 后续修订与新增
    r = c2.get("/releases/RK")
    check("后续修订不改旧结果", r.json()["counts"] == j1["counts"], r.text)
    j2 = pub(c2, "RK2", D1, "0.5").json()
    check("新发布固定受理时最新数据", j2["counts"] == {"东": 1, "西": 1, "南": 1, "全市": 3}, j2)

# ---------- 12. 发布中途失败：回滚、不耗额度、键可重用 ----------
tmp, app, c = make_client(sampler=RaisingSampler(), raise_server_exceptions=False)
post_csv(c, f"{D1},P1,东,1\n")
r = pub(c, "F1", D1, "0.5")
check("噪声故障返回 500", r.status_code == 500, r.status_code)
check("中途失败不耗额度", Decimal(c.get(f"/budget/{D1}").json()["used"]) == 0)
check("中途失败不留发布记录", c.get("/releases/F1").status_code == 404)
app.state.sampler = ZeroSampler()
check("同一键可重试成功", pub(c, "F1", D1, "0.5").status_code == 200)

# ---------- 13. ε 校验与浮点精确入账 ----------
tmp, app, c = make_client(sampler=ZeroSampler())
for bad_eps in (0, -0.1, 1.5, "abc"):
    r = pub(c, f"eps-{bad_eps}", D1, bad_eps)
    check(f"非法 ε={bad_eps} 被拒(422)", r.status_code == 422, r.text)
check("ε=1 合法", pub(c, "eps-ok", D1, 1).status_code == 200)
check("ε=1 恰好用尽当日额度", Decimal(c.get(f"/budget/{D1}").json()["used"]) == 1)
r = pub(c, "eps-float", D2, 0.1)
check("JSON 浮点 ε 精确入账(0.1)", r.status_code == 200
      and c.get(f"/budget/{D2}").json()["used"] == "0.1", r.text)

# ---------- 14. 拉普拉斯采样统计性质 + 真实噪声发布性质 ----------
s = LaplaceSampler()
n = 50000
xs = [s.sample(2.0) for _ in range(n)]
mean = sum(xs) / n
var = sum((x - mean) ** 2 for x in xs) / n
check("拉普拉斯采样均值≈0", abs(mean) < 0.05, mean)
check("拉普拉斯采样方差≈2b²", abs(var - 8.0) < 0.8, var)

tmp, app, c = make_client()  # 真实采样器
post_csv(c, f"{D1},P1,东,1\n{D1},P2,西,1\n")
all_ok = True
for i in range(30):
    r = pub(c, f"R{i}", D1, "0.03")
    j = r.json()
    cnt = j.get("counts", {})
    if not (r.status_code == 200
            and all(isinstance(v, int) and v >= 0 for v in cnt.values())
            and cnt["全市"] == cnt["东"] + cnt["西"] + cnt["南"]):
        all_ok = False
        print("  异常样本:", r.status_code, j)
        break
check("30 次真实噪声发布：均为非负整数且全市=三区之和", all_ok)
check("30×0.03 额度累计精确", Decimal(c.get(f"/budget/{D1}").json()["used"]) == Decimal("0.9"))

# ---------- 15. 空日期发布、查询过滤、非法日期 ----------
tmp, app, c = make_client(sampler=ZeroSampler())
r = pub(c, "EMPTY", "2026-10-30", "0.2")
check("无数据日期发布全零", r.status_code == 200
      and r.json()["counts"] == {"东": 0, "西": 0, "南": 0, "全市": 0}, r.text)
check("按日期过滤查询", len(c.get("/releases", params={"date": "2026-10-30"}).json()["releases"]) == 1)
check("非法日期查询被拒(400)", c.get("/budget/2026-13-40").status_code == 400)
check("不存在的发布键 404", c.get("/releases/NOPE").status_code == 404)

print(f"\n全部 {len(DONE)} 项检查通过。")
