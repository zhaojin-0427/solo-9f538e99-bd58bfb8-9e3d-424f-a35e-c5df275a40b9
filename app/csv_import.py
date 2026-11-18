"""CSV 解析与校验。

列（可有表头，顺序不限；无表头时按 date, patient_id, district, revision 顺序）：
    date       / 日期      YYYY-MM-DD
    patient_id / 患者代号  非空字符串（≤128 字符）
    district   / 区号      东 / 西 / 南（兼容 E/W/S、east/west/south，大小写不敏感）
    revision   / 修订号    正整数

任何一行非法、或批内出现“同一患者同日同修订号但区号不同”，均整批拒绝。
"""
from __future__ import annotations

import csv
import io
import re
from dataclasses import dataclass
from datetime import datetime

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
REV_RE = re.compile(r"^[0-9]+$")
MAX_REVISION = 2**63 - 1  # SQLite INTEGER 上限
MAX_ROWS = 200_000
MAX_ERRORS = 20
MAX_PATIENT_LEN = 128

DISTRICT_ALIASES = {
    "东": "东", "西": "西", "南": "南",
    "e": "东", "w": "西", "s": "南",
    "east": "东", "west": "西", "south": "南",
}

HEADER_ALIASES = {
    "date": "date", "日期": "date",
    "patient_id": "patient_id", "patient": "patient_id", "patientid": "patient_id",
    "患者代号": "patient_id", "患者": "patient_id",
    "district": "district", "区号": "district", "区": "district",
    "revision": "revision", "修订号": "revision",
}

REQUIRED = ("date", "patient_id", "district", "revision")


@dataclass(frozen=True)
class Row:
    date: str
    patient_id: str
    district: str
    revision: int


class CsvError(Exception):
    """CSV 校验失败，errors 为逐条错误说明。"""

    def __init__(self, errors: list[str]):
        self.errors = errors
        super().__init__("; ".join(errors[:3]))


def _valid_date(s: str) -> bool:
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def parse_csv(data: bytes) -> list[Row]:
    """解析并校验整个 CSV；任何错误都导致整批拒绝（抛 CsvError）。"""
    text = data.decode("utf-8-sig")  # UnicodeDecodeError 由调用方处理
    reader = csv.reader(io.StringIO(text))
    raw = [r for r in reader if any(cell.strip() for cell in r)]
    if not raw:
        raise CsvError(["CSV 文件为空"])

    first = [c.strip().lower() for c in raw[0]]
    if any(h in HEADER_ALIASES for h in first):
        col: dict[str, int] = {}
        for i, h in enumerate(first):
            k = HEADER_ALIASES.get(h)
            if k is not None and k not in col:
                col[k] = i
        missing = [k for k in REQUIRED if k not in col]
        if missing:
            raise CsvError([
                "表头缺少必需列: " + ", ".join(missing)
                + "（需要 date/日期, patient_id/患者代号, district/区号, revision/修订号）"
            ])
        body = raw[1:]
        offset = 2  # 行号（1 起，含表头）
    else:
        col = {k: i for i, k in enumerate(REQUIRED)}
        body = raw
        offset = 1

    if not body:
        raise CsvError(["CSV 没有数据行"])
    if len(body) > MAX_ROWS:
        raise CsvError([f"数据行数 {len(body)} 超过上限 {MAX_ROWS}"])

    errors: list[str] = []
    rows: list[Row] = []
    seen: dict[tuple, tuple] = {}  # (date, patient_id, revision) -> (district, 行号)

    for idx, r in enumerate(body):
        lineno = idx + offset
        if len(errors) >= MAX_ERRORS:
            errors.append("……错误过多，仅列出前若干条")
            break
        if len(r) < 4:
            errors.append(f"第{lineno}行：列数不足（需要 4 列）")
            continue
        d = r[col["date"]].strip()
        pid = r[col["patient_id"]].strip()
        dist_raw = r[col["district"]].strip()
        rev_raw = r[col["revision"]].strip()

        bad = False
        if not DATE_RE.match(d) or not _valid_date(d):
            errors.append(f"第{lineno}行：日期非法（须为 YYYY-MM-DD）：{d!r}")
            bad = True
        if not pid:
            errors.append(f"第{lineno}行：患者代号为空")
            bad = True
        elif len(pid) > MAX_PATIENT_LEN:
            errors.append(f"第{lineno}行：患者代号超过 {MAX_PATIENT_LEN} 字符")
            bad = True
        dist = DISTRICT_ALIASES.get(dist_raw.lower())
        if dist is None:
            errors.append(f"第{lineno}行：区号非法（限 东/西/南）：{dist_raw!r}")
            bad = True
        rev = None
        if not REV_RE.match(rev_raw):
            errors.append(f"第{lineno}行：修订号须为正整数：{rev_raw!r}")
            bad = True
        else:
            rev = int(rev_raw)
            if rev < 1:
                errors.append(f"第{lineno}行：修订号须为正整数（≥1）：{rev_raw!r}")
                bad = True
            elif rev > MAX_REVISION:
                errors.append(f"第{lineno}行：修订号过大：{rev_raw!r}")
                bad = True
        if bad:
            continue

        key = (d, pid, rev)
        prev = seen.get(key)
        if prev is not None:
            if prev[0] != dist:
                errors.append(
                    f"第{lineno}行与第{prev[1]}行：同一患者同日同修订号但区号不同"
                    f"（{prev[0]} / {dist}），整批拒绝"
                )
            continue  # 完全相同的重复行直接忽略（幂等）
        seen[key] = (dist, lineno)
        rows.append(Row(d, pid, dist, rev))

    if errors:
        raise CsvError(errors)
    return rows
