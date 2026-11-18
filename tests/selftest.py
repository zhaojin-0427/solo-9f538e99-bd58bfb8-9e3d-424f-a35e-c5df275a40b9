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


def post_csv(client, text, batch_id=None):
    data = {"file": ("data.csv", text.encode("utf-8"), "text/csv")}
    if batch_id is not None:
        data["batch_id"] = (None, batch_id)
    return client.post("/upload", files=data)


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

# ---------- 16. 批次上传：同批同内容沿用、同批异文冲突、多批来源并存 ----------
tmp, app, c = make_client(sampler=ZeroSampler())
csv_a = f"{D1},P1,东,1\n{D1},P2,西,1\n"
csv_b = f"{D1},P1,西,2\n{D1},P3,南,1\n"
r = post_csv(c, csv_a, batch_id="B1")
check("带批次号上传成功", r.status_code == 200 and r.json()["batch_id"] == "B1"
      and r.json()["batch_reused"] is False and r.json()["inserted"] == 2, r.text)
created = r.json()["batch_created_at"]
r = post_csv(c, csv_a, batch_id="B1")
check("同批同内容重试沿用原批次", r.status_code == 200
      and r.json()["batch_reused"] is True and r.json()["inserted"] == 0
      and r.json()["batch_created_at"] == created, r.text)
r = post_csv(c, f"{D1},P1,东,1\n", batch_id="B1")
check("同批异文冲突(409)", r.status_code == 409, r.text)
check("同批异文整批未入库", len(c.get("/batches").json()["batches"]) == 1)
r = post_csv(c, csv_b, batch_id="B2")
check("第二批上传成功", r.status_code == 200 and r.json()["inserted"] == 2, r.text)
r = post_csv(c, csv_b, batch_id="B3")
check("相同记录来自不同批次仍保留各自来源", r.status_code == 200
      and r.json()["batch_id"] == "B3" and r.json()["skipped_duplicates"] == 2, r.text)
prov = app.state.store.dump_provenance()
p1r2 = [p["batch_id"] for p in prov if p["patient_id"] == "P1" and p["revision"] == 2]
check("同一记录关联多个批次来源", sorted(p1r2) == ["B2", "B3"], p1r2)
r = c.post("/upload", files={"file": ("d.csv", f"{D1},P1,东,1\n".encode(), "text/csv")},
           data={"batch_id": "B1"})
check("批次号也可用表单字段提供", r.status_code == 409, r.text)
r = c.post("/upload", files={"file": ("d.csv", f"{D1},P1,东,1\n".encode(), "text/csv")},
           headers={"X-Batch-Id": "B1"})
check("批次号也可用请求头提供", r.status_code == 409, r.text)
r = post_csv(c, f"{D1},P4,东,1\n")
check("不传批次号时服务端生成", r.status_code == 200
      and r.json()["batch_id"].startswith("batch-"), r.text)
check("上传结果含批次隔离状态", r.json()["batch_isolated"] is False)
r = post_csv(c, f"{D1},P2,东,1\n{D1},P5,东,1\n", batch_id="B9")
check("冲突批次整批拒绝", r.status_code == 409, r.text)
check("被拒批次不留下批次元数据",
      [b["batch_id"] for b in c.get("/batches").json()["batches"]].count("B9") == 0)
check("被拒批次中的合法行也未入库",
      all(rec["patient_id"] != "P5" for rec in app.state.store.dump_records()))
c.post("/batches/B2/isolate")
r = post_csv(c, csv_b, batch_id="B2")
check("已隔离批次同内容重试仍沿用原批且保持隔离", r.status_code == 200
      and r.json()["batch_reused"] is True and r.json()["batch_isolated"] is True, r.text)
check("隔离 B2 后相同记录仍由 B3 供数",
      app.state.store.snapshot_counts(D1) == {"东": 1, "西": 2, "南": 1},
      app.state.store.snapshot_counts(D1))
c.post("/batches/B3/isolate")
check("全部来源隔离后这些记录退出统计",
      app.state.store.snapshot_counts(D1) == {"东": 2, "西": 1, "南": 0},
      app.state.store.snapshot_counts(D1))
check("已隔离批次不因重试恢复供数",
      app.state.store.snapshot_counts(D1) == {"东": 2, "西": 1, "南": 0},
      app.state.store.snapshot_counts(D1))

# ---------- 17. 隔离：既有发布不变、新发布重选来源、受影响清单 ----------
tmp, app, c = make_client(sampler=ZeroSampler())
post_csv(c, f"{D1},P1,东,1\n{D1},P2,西,1\n", batch_id="S1")
post_csv(c, f"{D1},P1,西,2\n{D1},P3,南,1\n", batch_id="S2")
check("隔离前快照取各患者最高修订",
      app.state.store.snapshot_counts(D1) == {"东": 0, "西": 2, "南": 1},
      app.state.store.snapshot_counts(D1))
j1 = pub(c, "REL1", D1, "0.4").json()
check("发布记录来源批次", j1["sources"] == ["S1", "S2"] and j1["source_known"] is True, j1)
check("发布键可按来源批次反查",
      [x["release_key"] for x in c.get("/releases", params={"batch_id": "S2"}).json()["releases"]] == ["REL1"])
r = c.post("/batches/S2/isolate")
j = r.json()
check("隔离成功并列出受影响发布键", r.status_code == 200
      and j["isolated"] is True and j["already_isolated"] is False
      and j["affected_release_keys"] == ["REL1"], r.text)
check("隔离幂等", c.post("/batches/S2/isolate").json()["already_isolated"] is True)
check("隔离不存在的批次 404", c.post("/batches/NOPE/isolate").status_code == 404)
check("隔离后旧发布数值不变", c.get("/releases/REL1").json()["counts"] == j1["counts"])
check("隔离不改已用预算", c.get(f"/budget/{D1}").json()["used"] == "0.4")
check("隔离后快照按未隔离来源重选最高修订",
      app.state.store.snapshot_counts(D1) == {"东": 1, "西": 1, "南": 0},
      app.state.store.snapshot_counts(D1))
j2 = pub(c, "REL2", D1, "0.4").json()
check("隔离后新发布按未隔离来源计数",
      j2["counts"] == {"东": 1, "西": 1, "南": 0, "全市": 2}, j2)
check("新发布不再采用已隔离批次", j2["sources"] == ["S1"], j2)
check("已隔离批次不进入新发布受影响清单",
      "REL2" not in c.get("/batches/S2").json()["affected_release_keys"])
check("批次详情列出受影响发布键与记录数",
      c.get("/batches/S2").json()["affected_release_keys"] == ["REL1"]
      and c.get("/batches/S2").json()["record_count"] == 2)
br = c.get("/batches/S2/releases").json()
check("受影响发布明细含原数值", len(br["releases"]) == 1
      and br["releases"][0]["counts"] == j1["counts"], br)
check("批次列表反映隔离状态", [b["batch_id"] for b in c.get("/batches").json()["batches"]
                                if b["isolated"]] == ["S2"])
check("隔离后预算照常扣减", c.get(f"/budget/{D1}").json()["used"] == "0.8")

# ---------- 18. 部分来源隔离：仍有未隔离来源时记录继续供数 ----------
tmp, app, c = make_client(sampler=ZeroSampler())
post_csv(c, f"{D1},P1,东,1\n", batch_id="M1")
post_csv(c, f"{D1},P1,东,1\n", batch_id="M2")
j = pub(c, "MK", D1, "0.2").json()
check("多来源记录的发布快照含全部未隔离来源", j["sources"] == ["M1", "M2"], j)
c.post("/batches/M1/isolate")
check("仅隔离其一时记录仍供数",
      app.state.store.snapshot_counts(D1) == {"东": 1, "西": 0, "南": 0},
      app.state.store.snapshot_counts(D1))
j2 = pub(c, "MK2", D1, "0.2").json()
check("仍有未隔离来源时新发布继续采用", j2["counts"]["东"] == 1 and j2["sources"] == ["M2"], j2)
c.post("/batches/M2/isolate")
check("全部来源隔离后记录退出统计",
      app.state.store.snapshot_counts(D1) == {"东": 0, "西": 0, "南": 0},
      app.state.store.snapshot_counts(D1))
check("两次隔离的受影响清单各自独立",
      c.get("/batches/M1").json()["affected_release_keys"] == ["MK"]
      and c.get("/batches/M2").json()["affected_release_keys"] == ["MK", "MK2"])

# ---------- 19. 隔离失败回滚：不留部分隔离 ----------
tmp, app, c = make_client(sampler=ZeroSampler(), raise_server_exceptions=False)
post_csv(c, f"{D1},P1,东,1\n", batch_id="R1")
pub(c, "RR", D1, "0.3")
orig = app.state.store._affected_release_keys


def boom(*a, **kw):
    raise RuntimeError("模拟隔离中途故障")


app.state.store._affected_release_keys = staticmethod(boom)
r = c.post("/batches/R1/isolate", )
check("隔离中途故障返回 500", r.status_code == 500, r.status_code)
app.state.store._affected_release_keys = orig
check("隔离失败不留部分隔离标记",
      c.get("/batches/R1").json()["isolated"] is False, c.get("/batches/R1").json())
check("隔离失败后原记录继续供数",
      app.state.store.snapshot_counts(D1) == {"东": 1, "西": 0, "南": 0})
r = c.post("/batches/R1/isolate")
check("故障恢复后可正常隔离", r.status_code == 200
      and r.json()["affected_release_keys"] == ["RR"], r.text)

# ---------- 20. 既有无来源记录继续供数、旧发布来源标记未知 ----------
import sqlite3  # noqa: E402

tmpdir = tempfile.TemporaryDirectory()
legacy_db = os.path.join(tmpdir.name, "legacy.db")
conn = sqlite3.connect(legacy_db)
conn.executescript(
    """
    CREATE TABLE records(date TEXT NOT NULL, patient_id TEXT NOT NULL, district TEXT NOT NULL,
                         revision INTEGER NOT NULL, PRIMARY KEY(date, patient_id, revision));
    CREATE TABLE budgets(date TEXT PRIMARY KEY, used TEXT NOT NULL DEFAULT '0');
    CREATE TABLE releases(release_key TEXT PRIMARY KEY, date TEXT NOT NULL, epsilon TEXT NOT NULL,
                          east INTEGER NOT NULL, west INTEGER NOT NULL, south INTEGER NOT NULL,
                          citywide INTEGER NOT NULL, created_at TEXT NOT NULL);
    """
)
conn.execute("INSERT INTO records VALUES(?,?,?,?)", (D1, "P1", "东", 1))
conn.execute("INSERT INTO budgets VALUES(?,?)", (D1, "0.5"))
conn.execute("INSERT INTO releases VALUES(?,?,?,?,?,?,?,?)",
             ("LEGACY", D1, "0.5", 9, 0, 0, 9, "2026-01-01T00:00:00+00:00"))
conn.commit()
conn.close()
app = create_app(legacy_db)
app.state.sampler = ZeroSampler()
with TestClient(app) as c:
    old = c.get("/releases/LEGACY").json()
    check("旧发布来源标记为未知", old["source_known"] is False and old["sources"] is None, old)
    check("旧发布数值保持不变", old["counts"] == {"东": 9, "西": 0, "南": 0, "全市": 9}, old)
    n = pub(c, "NEW", D1, "0.1").json()
    check("无来源旧记录继续供数", n["counts"]["东"] == 1, n)
    check("新发布来源已知但为空清单", n["source_known"] is True and n["sources"] == [], n)
    post_csv(c, f"{D1},P2,南,1\n", batch_id="LX")
    c.post("/batches/LX/isolate")
    n2 = pub(c, "NEW2", D1, "0.1").json()
    check("隔离新批次不影响无来源旧记录", n2["counts"]["东"] == 1 and n2["counts"]["南"] == 0, n2)
    check("隔离响应提示来源未知的发布数",
          c.post("/batches/LX/isolate").json()["unknown_provenance_release_count"] == 1)

# ---------- 21. 跨连接交错：发布先提交 / 隔离先提交，均以完整已提交状态为准 ----------
from app.store import Store  # noqa: E402


class RawRow:
    def __init__(self, date, patient_id, district, revision):
        self.date, self.patient_id = date, patient_id
        self.district, self.revision = district, revision


tmpdir = tempfile.TemporaryDirectory()
xdb = os.path.join(tmpdir.name, "x.db")
wa, wb = Store(xdb), Store(xdb)
wa.insert_batch([RawRow(D1, "P1", "东", 1)], batch_id="XB")
started, release = threading.Event(), threading.Event()


def blocking_noise(scale):
    started.set()          # 发布已进入写事务、尚未提交
    release.wait(5)
    return 0.0


cross: dict = {}


def do_publish():
    cross["pub"] = wa.publish("XK", D1, Decimal("0.2"), blocking_noise)


def do_isolate():
    started.wait(5)
    cross["iso"] = wb.isolate_batch("XB")


t1 = threading.Thread(target=do_publish)
t2 = threading.Thread(target=do_isolate)
t1.start(); t2.start()
time.sleep(0.2); release.set()   # 让隔离在发布提交之后才拿到写锁
t1.join(10); t2.join(10)
check("交错：发布先提交时来源归属完整", cross["pub"][0]["sources"] == ["XB"], cross)
check("交错：随后隔离把该发布列入受影响清单",
      cross["iso"]["affected_release_keys"] == ["XK"], cross)
check("交错：隔离后同批记录不再供数",
      wa.snapshot_counts(D1) == {"东": 0, "西": 0, "南": 0}, wa.snapshot_counts(D1))
check("交错：已发布数值与预算不变",
      wb.get_release("XK")["counts"] == {"东": 1, "西": 0, "南": 0, "全市": 1}
      and wb.get_budget(D1)["used"] == "0.2")

# 反向：隔离先拿到写锁（持锁期间），发布随后提交 → 新发布不得采用该批
y_a, y_b = wa, wb
y_a.insert_batch([RawRow(D2, "P1", "东", 1)], batch_id="YB")
iso_started, iso_go = threading.Event(), threading.Event()
orig_affected = Store._affected_release_keys


def slow_affected(cur, batch_id):
    iso_started.set()
    iso_go.wait(5)
    return orig_affected(cur, batch_id)


y_b._affected_release_keys = slow_affected


def iso_first():
    cross["iso2"] = y_b.isolate_batch("YB")


def pub_after():
    iso_started.wait(5)
    cross["pub2"] = y_a.publish("YK", D2, Decimal("0.2"), lambda s: 0.0)


t3 = threading.Thread(target=iso_first)
t4 = threading.Thread(target=pub_after)
t3.start(); t4.start()
time.sleep(0.2); iso_go.set()    # 隔离先提交，发布事务在其后读快照
t3.join(10); t4.join(10)
check("反向交错：隔离先提交则新发布不采用该批", cross["pub2"][0]["sources"] == [], cross)
check("反向交错：新发布按未隔离来源计数", cross["pub2"][0]["counts"]["东"] == 0, cross)
check("反向交错：受影响清单不含后提交的发布",
      cross["iso2"]["affected_release_keys"] == [], cross)
y_b._affected_release_keys = orig_affected
wa.close(); wb.close()

print(f"\n全部 {len(DONE)} 项检查通过。")
