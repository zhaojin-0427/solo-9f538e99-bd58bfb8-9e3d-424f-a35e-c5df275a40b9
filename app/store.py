"""SQLite 存储层：原始记录、每日 ε 预算、发布结果。

并发模型：单连接 + 可重入锁；写事务一律 BEGIN IMMEDIATE。
“读预算 → 校验 → 扣减 → 写入结果”在同一事务内完成，
因此并发发布不会超额；任何失败整体回滚，不消耗预算。
发布结果落库后即固定：后续修订、服务重启都不会改变已发布结果。
"""
from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone
from decimal import Decimal
from typing import Callable, Iterable, Optional

DISTRICTS = ("东", "西", "南")
BUDGET_PER_DATE = Decimal("1")

SCHEMA = """
CREATE TABLE IF NOT EXISTS records (
    date       TEXT NOT NULL,
    patient_id TEXT NOT NULL,
    district   TEXT NOT NULL,
    revision   INTEGER NOT NULL,
    PRIMARY KEY (date, patient_id, revision)
);
CREATE INDEX IF NOT EXISTS idx_records_date ON records(date);

CREATE TABLE IF NOT EXISTS budgets (
    date TEXT PRIMARY KEY,
    used TEXT NOT NULL DEFAULT '0'   -- 十进制字符串，Python 端用 Decimal 精确计算
);

CREATE TABLE IF NOT EXISTS releases (
    release_key TEXT PRIMARY KEY,
    date        TEXT NOT NULL,
    epsilon     TEXT NOT NULL,       -- 十进制字符串
    east        INTEGER NOT NULL,
    west        INTEGER NOT NULL,
    south       INTEGER NOT NULL,
    citywide    INTEGER NOT NULL,
    created_at  TEXT NOT NULL
);
"""


class BatchConflictError(Exception):
    """上传批次与库内已有记录存在“同修订号异文”冲突。"""

    def __init__(self, conflicts: list[str]):
        self.conflicts = conflicts
        super().__init__("；".join(conflicts[:5]))


class ReleaseParamConflictError(Exception):
    """发布键已存在，但日期或 ε 与首次发布不一致。"""


class BudgetExceededError(Exception):
    """该数据日期的 ε 预算不足。"""

    def __init__(self, date: str, requested: Decimal, remaining: Decimal):
        self.date = date
        self.requested = requested
        self.remaining = remaining
        super().__init__(f"日期 {date} 预算不足：请求 ε={requested}，剩余额度 {remaining}")


def _release_dict(row: sqlite3.Row) -> dict:
    return {
        "release_key": row["release_key"],
        "date": row["date"],
        "epsilon": row["epsilon"],
        "counts": {
            "东": row["east"],
            "西": row["west"],
            "南": row["south"],
            "全市": row["citywide"],
        },
        "created_at": row["created_at"],
    }


class Store:
    def __init__(self, path: str):
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        with self._lock:
            self._conn.executescript(SCHEMA)
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA busy_timeout=5000")

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ---------------- 上传 ----------------
    def insert_batch(self, rows: Iterable) -> dict:
        """整批写入；遇“同修订号异文”冲突则整体回滚（抛 BatchConflictError）。"""
        with self._lock:
            cur = self._conn.cursor()
            inserted = skipped = 0
            conflicts: list[str] = []
            try:
                cur.execute("BEGIN IMMEDIATE")
                for r in rows:
                    cur.execute(
                        "SELECT district FROM records WHERE date=? AND patient_id=? AND revision=?",
                        (r.date, r.patient_id, r.revision),
                    )
                    old = cur.fetchone()
                    if old is not None:
                        if old["district"] != r.district:
                            conflicts.append(
                                f"{r.date} 患者 {r.patient_id} 修订号 {r.revision}："
                                f"库内为「{old['district']}」，新数据为「{r.district}」"
                            )
                            if len(conflicts) >= 10:
                                break
                            continue
                        skipped += 1  # 内容完全相同的重复上传：幂等跳过
                        continue
                    cur.execute(
                        "INSERT INTO records(date, patient_id, district, revision) VALUES (?,?,?,?)",
                        (r.date, r.patient_id, r.district, r.revision),
                    )
                    inserted += 1
                if conflicts:
                    raise BatchConflictError(conflicts)
                cur.execute("COMMIT")
                return {"inserted": inserted, "skipped_duplicates": skipped}
            except Exception:
                if self._conn.in_transaction:
                    self._conn.execute("ROLLBACK")
                raise

    # ---------------- 发布 ----------------
    def publish(
        self,
        release_key: str,
        date: str,
        epsilon: Decimal,
        noise_fn: Callable[[float], float],
    ) -> tuple[dict, bool]:
        """发布一次差分隐私计数，返回 (发布结果, 是否重放)。

        - 同键同参（日期与 ε 数值相等）：返回原结果，不重复扣额度；
        - 同键异参：抛 ReleaseParamConflictError；
        - 预算不足：抛 BudgetExceededError；
        - 任何失败整体回滚，不消耗预算，发布键不被占用。
        """
        eps_str = format(epsilon, "f")
        with self._lock:
            cur = self._conn.cursor()
            try:
                cur.execute("BEGIN IMMEDIATE")

                cur.execute("SELECT * FROM releases WHERE release_key=?", (release_key,))
                old = cur.fetchone()
                if old is not None:
                    if old["date"] == date and Decimal(old["epsilon"]) == epsilon:
                        cur.execute("COMMIT")
                        return _release_dict(old), True
                    raise ReleaseParamConflictError(
                        f"发布键 {release_key!r} 已用于 date={old['date']}, ε={old['epsilon']}，"
                        f"与本次 date={date}, ε={eps_str} 不一致"
                    )

                # 受理时刻的已提交数据快照（同事务内读取，随事务提交而固定）
                true_counts = self._snapshot_counts(date)

                cur.execute("SELECT used FROM budgets WHERE date=?", (date,))
                row = cur.fetchone()
                used = Decimal(row["used"]) if row else Decimal("0")
                remaining = BUDGET_PER_DATE - used
                if epsilon > remaining:
                    raise BudgetExceededError(date, epsilon, remaining)

                scale = 1.0 / float(epsilon)
                noisy = {d: max(0, round(true_counts[d] + noise_fn(scale))) for d in DISTRICTS}
                citywide = sum(noisy.values())  # 全市 = 三区公布值之和（后处理）
                created_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

                cur.execute(
                    "INSERT INTO releases(release_key, date, epsilon, east, west, south, citywide, created_at)"
                    " VALUES (?,?,?,?,?,?,?,?)",
                    (release_key, date, eps_str,
                     noisy["东"], noisy["西"], noisy["南"], citywide, created_at),
                )
                new_used = used + epsilon
                cur.execute(
                    "INSERT INTO budgets(date, used) VALUES (?, ?)"
                    " ON CONFLICT(date) DO UPDATE SET used=excluded.used",
                    (date, format(new_used, "f")),
                )
                cur.execute("COMMIT")

                result = {
                    "release_key": release_key,
                    "date": date,
                    "epsilon": eps_str,
                    "counts": {"东": noisy["东"], "西": noisy["西"], "南": noisy["南"], "全市": citywide},
                    "created_at": created_at,
                }
                return result, False
            except Exception:
                if self._conn.in_transaction:
                    self._conn.execute("ROLLBACK")
                raise

    # ---------------- 查询 ----------------
    def _snapshot_counts(self, date: str) -> dict:
        """某日期三区真实计数：同一患者只采用最高修订号。调用方须持有锁。"""
        cur = self._conn.execute(
            """
            SELECT district, COUNT(*) AS n FROM (
                SELECT r.patient_id AS pid, r.district AS district
                FROM records r
                WHERE r.date = ?
                  AND r.revision = (
                      SELECT MAX(revision) FROM records
                      WHERE date = r.date AND patient_id = r.patient_id
                  )
            ) GROUP BY district
            """,
            (date,),
        )
        counts = {d: 0 for d in DISTRICTS}
        for row in cur:
            if row["district"] in counts:
                counts[row["district"]] = row["n"]
        return counts

    def snapshot_counts(self, date: str) -> dict:
        with self._lock:
            return self._snapshot_counts(date)

    def get_release(self, release_key: str) -> Optional[dict]:
        with self._lock:
            cur = self._conn.execute("SELECT * FROM releases WHERE release_key=?", (release_key,))
            row = cur.fetchone()
            return _release_dict(row) if row else None

    def list_releases(self, date: Optional[str] = None) -> list[dict]:
        with self._lock:
            if date is None:
                cur = self._conn.execute("SELECT * FROM releases ORDER BY created_at, release_key")
            else:
                cur = self._conn.execute(
                    "SELECT * FROM releases WHERE date=? ORDER BY created_at, release_key", (date,))
            return [_release_dict(r) for r in cur]

    def get_budget(self, date: str) -> dict:
        with self._lock:
            cur = self._conn.execute("SELECT used FROM budgets WHERE date=?", (date,))
            row = cur.fetchone()
            used = Decimal(row["used"]) if row else Decimal("0")
            remaining = BUDGET_PER_DATE - used
            return {
                "date": date,
                "budget": format(BUDGET_PER_DATE, "f"),
                "used": format(used, "f"),
                "remaining": format(remaining, "f"),
            }

    def dump_records(self) -> list[dict]:
        """诊断/自测用：返回全部原始记录。"""
        with self._lock:
            cur = self._conn.execute("SELECT * FROM records ORDER BY date, patient_id, revision")
            return [dict(r) for r in cur]
