"""SQLite 存储层：原始记录、批次来源、每日 ε 预算、发布结果。

并发模型：单连接 + 可重入锁；写事务一律 BEGIN IMMEDIATE。
“读预算 → 校验 → 扣减 → 写入结果（含来源快照）”在同一事务内完成，
因此并发发布不会超额；任何失败整体回滚，不消耗预算、不占用发布键。
批次上传（记录 + 来源 + 批次元数据）与批次隔离（更新隔离标记 + 读取受影响
发布键）也各自在同一事务内完成，失败不留下部分状态。

来源语义：
- 一条记录可同时来自多个批次（record_batches 多对多）；
- 记录存在来源批次时，只要有任一来源批次未隔离即可供新发布使用；
- 没有来源批次的既有记录（旧库迁移而来）视为始终可用；
- 发布时把“当时实际采用的记录所对应的未隔离批次”快照到 release_sources，
  之后隔离批次不会改变已发布数值和预算，但可据此列出可能受影响的发布键。
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import uuid
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
    legacy     INTEGER NOT NULL DEFAULT 0,  -- 1=来源未知的旧库记录，始终供数
    PRIMARY KEY (date, patient_id, revision)
);
CREATE INDEX IF NOT EXISTS idx_records_date ON records(date);

CREATE TABLE IF NOT EXISTS budgets (
    date TEXT PRIMARY KEY,
    used TEXT NOT NULL DEFAULT '0'   -- 十进制字符串，Python 端用 Decimal 精确计算
);

CREATE TABLE IF NOT EXISTS releases (
    release_key   TEXT PRIMARY KEY,
    date          TEXT NOT NULL,
    epsilon       TEXT NOT NULL,     -- 十进制字符串
    east          INTEGER NOT NULL,
    west          INTEGER NOT NULL,
    south         INTEGER NOT NULL,
    citywide      INTEGER NOT NULL,
    created_at    TEXT NOT NULL,
    source_known  INTEGER NOT NULL DEFAULT 0  -- 0=旧版本发布，来源未知
);

CREATE TABLE IF NOT EXISTS batches (
    batch_id     TEXT PRIMARY KEY,
    content_hash TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    isolated_at  TEXT                -- NULL 表示未隔离
);

CREATE TABLE IF NOT EXISTS record_batches (
    date       TEXT NOT NULL,
    patient_id TEXT NOT NULL,
    revision   INTEGER NOT NULL,
    batch_id   TEXT NOT NULL,
    PRIMARY KEY (date, patient_id, revision, batch_id)
);
CREATE INDEX IF NOT EXISTS idx_record_batches_batch ON record_batches(batch_id);

CREATE TABLE IF NOT EXISTS release_sources (
    release_key TEXT NOT NULL,
    batch_id    TEXT NOT NULL,
    PRIMARY KEY (release_key, batch_id)
);
CREATE INDEX IF NOT EXISTS idx_release_sources_batch ON release_sources(batch_id);
"""

# 参与统计的有效记录：旧库无来源记录、无任何批次关联的记录始终有效；
# 有来源批次的记录，只要有至少一个来源批次未隔离即可用。
_ACTIVE = """(
    {a}.legacy = 1
    OR NOT EXISTS (
        SELECT 1 FROM record_batches xb
        WHERE xb.date = {a}.date
          AND xb.patient_id = {a}.patient_id
          AND xb.revision = {a}.revision
    )
    OR EXISTS (
        SELECT 1 FROM record_batches xb
        JOIN batches x ON x.batch_id = xb.batch_id
        WHERE xb.date = {a}.date
          AND xb.patient_id = {a}.patient_id
          AND xb.revision = {a}.revision
          AND x.isolated_at IS NULL
    )
)"""


def _active(alias: str) -> str:
    return _ACTIVE.format(a=alias)


class BatchConflictError(Exception):
    """上传批次与库内已有记录存在“同修订号异文”冲突。"""

    def __init__(self, conflicts: list[str]):
        self.conflicts = conflicts
        super().__init__("；".join(conflicts[:5]))


class BatchContentConflictError(Exception):
    """同一批次号被用于内容不同的上传。"""

    def __init__(self, batch_id: str):
        self.batch_id = batch_id
        super().__init__(f"批次号 {batch_id!r} 已存在且内容不同")


class BatchNotFoundError(Exception):
    """批次号不存在。"""

    def __init__(self, batch_id: str):
        self.batch_id = batch_id
        super().__init__(f"批次号 {batch_id!r} 不存在")


class ReleaseParamConflictError(Exception):
    """发布键已存在，但日期或 ε 与首次发布不一致。"""


class BudgetExceededError(Exception):
    """该数据日期的 ε 预算不足。"""

    def __init__(self, date: str, requested: Decimal, remaining: Decimal):
        self.date = date
        self.requested = requested
        self.remaining = remaining
        super().__init__(f"日期 {date} 预算不足：请求 ε={requested}，剩余额度 {remaining}")


def new_batch_id() -> str:
    """为未显式提供批次号的上传生成一个服务端批次号。"""
    return "batch-" + uuid.uuid4().hex


def content_hash(rows: Iterable) -> str:
    """批次内容指纹：按规范化后的行集合计算，行顺序不影响结果。"""
    payload = sorted(
        (r.date, r.patient_id, r.district, int(r.revision)) for r in rows
    )
    blob = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def _release_dict(row: sqlite3.Row) -> dict:
    known = bool(row["source_known"])
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
        "source_known": known,
        # 旧版本发布的来源不可追溯，明确标记为 None；新发布即使未用任何批次也是 []
        "sources": [] if known else None,
    }


def _batch_dict(row: sqlite3.Row, record_count: int = 0, affected_count: int = 0) -> dict:
    isolated_at = row["isolated_at"]
    return {
        "batch_id": row["batch_id"],
        "content_hash": row["content_hash"],
        "created_at": row["created_at"],
        "isolated": isolated_at is not None,
        "isolated_at": isolated_at,
        "record_count": record_count,
        "affected_release_count": affected_count,
    }


class Store:
    def __init__(self, path: str):
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        with self._lock:
            self._conn.executescript(SCHEMA)
            self._migrate()
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA busy_timeout=5000")

    def _migrate(self) -> None:
        """兼容旧库：补上来源未知的旧记录标记与既有发布的来源未知标记。"""
        cols = {r[1] for r in self._conn.execute("PRAGMA table_info(releases)")}
        if "source_known" not in cols:
            self._conn.execute(
                "ALTER TABLE releases ADD COLUMN source_known INTEGER NOT NULL DEFAULT 0"
            )
        cols = {r[1] for r in self._conn.execute("PRAGMA table_info(records)")}
        if "legacy" not in cols:
            self._conn.execute(
                "ALTER TABLE records ADD COLUMN legacy INTEGER NOT NULL DEFAULT 0"
            )
            # 迁移前已存在的记录没有任何批次来源，其来源已不可追溯：标记为旧记录。
            has_batches = self._conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='batches'"
            ).fetchone()
            if has_batches is not None:
                self._conn.execute(
                    "UPDATE records SET legacy=1 WHERE NOT EXISTS ("
                    " SELECT 1 FROM record_batches rb WHERE rb.date=records.date"
                    " AND rb.patient_id=records.patient_id AND rb.revision=records.revision)"
                )
            else:
                self._conn.execute("UPDATE records SET legacy=1")

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ---------------- 上传 ----------------
    def insert_batch(self, rows: Iterable, batch_id: Optional[str] = None) -> dict:
        """整批写入记录及其批次来源；失败整体回滚。

        - 批次号不存在：新建批次，写入记录并建立来源关联；
        - 同批次号 + 同内容（行集合规范化后相同）：沿用原批次，不重复写记录；
        - 同批次号 + 异内容：抛 BatchContentConflictError，整批不入库；
        - 同修订号异文：抛 BatchConflictError，整批（含批次元数据）回滚；
        - **相同记录来自不同批次**：记录不重复写入，但保留各批次的来源关联。
        """
        rows = list(rows)
        digest = content_hash(rows)
        batch_id = batch_id or new_batch_id()
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        with self._lock:
            cur = self._conn.cursor()
            try:
                cur.execute("BEGIN IMMEDIATE")

                cur.execute(
                    "SELECT batch_id, content_hash, created_at, isolated_at"
                    " FROM batches WHERE batch_id=?",
                    (batch_id,),
                )
                batch = cur.fetchone()
                if batch is not None:
                    if batch["content_hash"] != digest:
                        raise BatchContentConflictError(batch_id)
                    cur.execute("COMMIT")
                    return {
                        "batch_id": batch_id,
                        "batch_reused": True,
                        "batch_isolated": batch["isolated_at"] is not None,
                        "batch_created_at": batch["created_at"],
                        "inserted": 0,
                        "skipped_duplicates": len(rows),
                    }

                cur.execute(
                    "INSERT INTO batches(batch_id, content_hash, created_at, isolated_at)"
                    " VALUES (?,?,?,NULL)",
                    (batch_id, digest, now),
                )
                inserted = skipped = 0
                conflicts: list[str] = []
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
                        skipped += 1  # 记录内容相同：不重复写，但仍保留本批来源
                    else:
                        cur.execute(
                            "INSERT INTO records(date, patient_id, district, revision) VALUES (?,?,?,?)",
                            (r.date, r.patient_id, r.district, r.revision),
                        )
                        inserted += 1
                    cur.execute(
                        "INSERT OR IGNORE INTO record_batches(date, patient_id, revision, batch_id)"
                        " VALUES (?,?,?,?)",
                        (r.date, r.patient_id, r.revision, batch_id),
                    )
                if conflicts:
                    raise BatchConflictError(conflicts)
                cur.execute("COMMIT")
                return {
                    "batch_id": batch_id,
                    "batch_reused": False,
                    "batch_isolated": False,
                    "batch_created_at": now,
                    "inserted": inserted,
                    "skipped_duplicates": skipped,
                }
            except Exception:
                if self._conn.in_transaction:
                    self._conn.execute("ROLLBACK")
                raise

    # ---------------- 批次隔离 ----------------
    def isolate_batch(self, batch_id: str) -> dict:
        """隔离批次；幂等。受影响发布键与隔离标记在同一事务中读取/提交。"""
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        with self._lock:
            cur = self._conn.cursor()
            try:
                cur.execute("BEGIN IMMEDIATE")
                cur.execute(
                    "SELECT batch_id, created_at, isolated_at FROM batches WHERE batch_id=?",
                    (batch_id,),
                )
                batch = cur.fetchone()
                if batch is None:
                    raise BatchNotFoundError(batch_id)
                already = batch["isolated_at"] is not None
                if already:
                    isolated_at = batch["isolated_at"]
                else:
                    isolated_at = now
                    cur.execute(
                        "UPDATE batches SET isolated_at=? WHERE batch_id=?",
                        (isolated_at, batch_id),
                    )
                affected = self._affected_release_keys(cur, batch_id)
                unknown = cur.execute(
                    "SELECT COUNT(*) FROM releases WHERE source_known=0"
                ).fetchone()[0]
                cur.execute("COMMIT")
                return {
                    "batch_id": batch_id,
                    "isolated": True,
                    "already_isolated": already,
                    "isolated_at": isolated_at,
                    "affected_release_keys": affected,
                    "affected_release_count": len(affected),
                    "unknown_provenance_release_count": unknown,
                }
            except Exception:
                if self._conn.in_transaction:
                    self._conn.execute("ROLLBACK")
                raise

    @staticmethod
    def _affected_release_keys(cur: sqlite3.Cursor, batch_id: str) -> list[str]:
        cur.execute(
            """
            SELECT r.release_key
            FROM releases r
            JOIN release_sources rs ON rs.release_key = r.release_key
            WHERE rs.batch_id = ?
            ORDER BY r.created_at, r.release_key
            """,
            (batch_id,),
        )
        return [row[0] for row in cur.fetchall()]

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
        - 任何失败整体回滚，不消耗预算，发布键与来源快照均不落库。
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
                        result = _release_dict(old)
                        if result["source_known"]:
                            result["sources"] = self._release_sources(cur, release_key)
                        cur.execute("COMMIT")
                        return result, True
                    raise ReleaseParamConflictError(
                        f"发布键 {release_key!r} 已用于 date={old['date']}, ε={old['epsilon']}，"
                        f"与本次 date={date}, ε={eps_str} 不一致"
                    )

                # 受理时刻的已提交数据快照（同事务内读取，随事务提交而固定）
                true_counts = self._snapshot_counts(date)
                source_batches = self._snapshot_source_batches(date)

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
                    "INSERT INTO releases(release_key, date, epsilon, east, west, south,"
                    " citywide, created_at, source_known) VALUES (?,?,?,?,?,?,?,?,1)",
                    (release_key, date, eps_str,
                     noisy["东"], noisy["西"], noisy["南"], citywide, created_at),
                )
                for batch_id in source_batches:
                    cur.execute(
                        "INSERT OR IGNORE INTO release_sources(release_key, batch_id)"
                        " VALUES (?,?)",
                        (release_key, batch_id),
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
                    "source_known": True,
                    "sources": source_batches,
                }
                return result, False
            except Exception:
                if self._conn.in_transaction:
                    self._conn.execute("ROLLBACK")
                raise

    @staticmethod
    def _release_sources(cur: sqlite3.Cursor, release_key: str) -> list[str]:
        cur.execute(
            "SELECT batch_id FROM release_sources WHERE release_key=? ORDER BY batch_id",
            (release_key,),
        )
        return [row[0] for row in cur.fetchall()]

    # ---------------- 统计快照 ----------------
    def _snapshot_counts(self, date: str) -> dict:
        """某日期三区真实计数：在未隔离来源中，每患者只采用最高修订号。

        没有来源批次的旧记录始终参与；某记录的全部来源都被隔离时该记录退出计数。
        调用方须持有锁（发布路径中同时处于 BEGIN IMMEDIATE 事务内）。
        """
        cur = self._conn.execute(
            f"""
            SELECT district, COUNT(*) AS n FROM (
                SELECT r.patient_id AS pid, r.district AS district
                FROM records r
                WHERE r.date = ?
                  AND {_active("r")}
                  AND r.revision = (
                      SELECT MAX(r2.revision) FROM records r2
                      WHERE r2.date = r.date
                        AND r2.patient_id = r.patient_id
                        AND {_active("r2")}
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

    def _snapshot_source_batches(self, date: str) -> list[str]:
        """发布时实际采用的记录所关联的批次号。调用方须持有锁。

        只列出当时未隔离的批次：某记录若还有多个未隔离来源，这些批次都算
        “当时被采用”，隔离其中任一批都可能在事后被列为受影响。
        发布本身在 BEGIN IMMEDIATE 事务内调用，读到的未隔离状态与随后的写入
        一同提交；若并发隔离先提交，则这里已看不到该批；若本事务先提交，
        隔离方的受影响清单会包含本次发布。两种情况都不会出现“归属撕裂”。
        """
        cur = self._conn.execute(
            f"""
            SELECT DISTINCT rb.batch_id
            FROM record_batches rb
            JOIN batches b ON b.batch_id = rb.batch_id AND b.isolated_at IS NULL
            JOIN records r ON r.date = rb.date
                          AND r.patient_id = rb.patient_id
                          AND r.revision = rb.revision
                          AND r.legacy = 0
            WHERE rb.date = ?
              AND rb.revision = (
                  SELECT MAX(r2.revision) FROM records r2
                  WHERE r2.date = rb.date
                    AND r2.patient_id = rb.patient_id
                    AND {_active("r2")}
              )
            ORDER BY rb.batch_id
            """,
            (date,),
        )
        return [row[0] for row in cur.fetchall()]

    def snapshot_counts(self, date: str) -> dict:
        with self._lock:
            return self._snapshot_counts(date)

    def snapshot_source_batches(self, date: str) -> list[str]:
        with self._lock:
            return self._snapshot_source_batches(date)

    # ---------------- 查询 ----------------
    def _attach_sources(self, cur: sqlite3.Cursor, items: list[dict]) -> list[dict]:
        keys = sorted({item["release_key"] for item in items if item["source_known"]})
        if not keys:
            return items
        placeholders = ",".join("?" * len(keys))
        cur.execute(
            f"SELECT release_key, batch_id FROM release_sources"
            f" WHERE release_key IN ({placeholders}) ORDER BY release_key, batch_id",
            keys,
        )
        sources: dict[str, list[str]] = {}
        for row in cur.fetchall():
            sources.setdefault(row["release_key"], []).append(row["batch_id"])
        for item in items:
            if item["source_known"]:
                item["sources"] = sources.get(item["release_key"], [])
        return items

    def get_release(self, release_key: str) -> Optional[dict]:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("SELECT * FROM releases WHERE release_key=?", (release_key,))
            row = cur.fetchone()
            if row is None:
                return None
            item = _release_dict(row)
            return self._attach_sources(cur, [item])[0]

    def list_releases(
        self, date: Optional[str] = None, batch_id: Optional[str] = None
    ) -> list[dict]:
        with self._lock:
            cur = self._conn.cursor()
            sql = "SELECT r.* FROM releases r"
            where: list[str] = []
            params: list = []
            if batch_id is not None:
                sql += " JOIN release_sources rs ON rs.release_key = r.release_key"
                where.append("rs.batch_id = ?")
                params.append(batch_id)
            if date is not None:
                where.append("r.date = ?")
                params.append(date)
            if where:
                sql += " WHERE " + " AND ".join(where)
            sql += " ORDER BY r.created_at, r.release_key"
            cur.execute(sql, params)
            items = [_release_dict(row) for row in cur.fetchall()]
            return self._attach_sources(cur, items)

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

    # ---------------- 批次查询 ----------------
    def _batch_row(self, cur: sqlite3.Cursor, batch_id: str) -> Optional[sqlite3.Row]:
        cur.execute(
            "SELECT batch_id, content_hash, created_at, isolated_at"
            " FROM batches WHERE batch_id=?",
            (batch_id,),
        )
        return cur.fetchone()

    def _record_count(self, cur: sqlite3.Cursor, batch_id: str) -> int:
        cur.execute(
            "SELECT COUNT(*) FROM record_batches WHERE batch_id=?", (batch_id,)
        )
        return cur.fetchone()[0]

    def _affected_count(self, cur: sqlite3.Cursor, batch_id: str) -> int:
        cur.execute(
            "SELECT COUNT(*) FROM release_sources WHERE batch_id=?", (batch_id,)
        )
        return cur.fetchone()[0]

    def get_batch(self, batch_id: str) -> Optional[dict]:
        """批次详情，含受影响发布键（发布时曾采用该批记录）。"""
        with self._lock:
            cur = self._conn.cursor()
            row = self._batch_row(cur, batch_id)
            if row is None:
                return None
            info = _batch_dict(
                row,
                record_count=self._record_count(cur, batch_id),
                affected_count=self._affected_count(cur, batch_id),
            )
            info["affected_release_keys"] = self._affected_release_keys(cur, batch_id)
            return info

    def get_batch_releases(self, batch_id: str) -> Optional[dict]:
        """批次详情 + 受影响发布结果明细（同一把锁内读取）。"""
        with self._lock:
            cur = self._conn.cursor()
            row = self._batch_row(cur, batch_id)
            if row is None:
                return None
            releases = self.list_releases(batch_id=batch_id)  # RLock 可重入
            info = _batch_dict(row, record_count=self._record_count(cur, batch_id),
                               affected_count=len(releases))
            info["release_keys"] = [r["release_key"] for r in releases]
            info["releases"] = releases
            info["unknown_provenance_release_count"] = cur.execute(
                "SELECT COUNT(*) FROM releases WHERE source_known=0"
            ).fetchone()[0]
            return info

    def list_batches(self) -> list[dict]:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                """
                SELECT b.batch_id, b.content_hash, b.created_at, b.isolated_at,
                       (SELECT COUNT(*) FROM record_batches rb WHERE rb.batch_id=b.batch_id)
                           AS record_count,
                       (SELECT COUNT(*) FROM release_sources rs WHERE rs.batch_id=b.batch_id)
                           AS affected_count
                FROM batches b
                ORDER BY b.created_at, b.batch_id
                """
            )
            return [
                _batch_dict(r, record_count=r["record_count"], affected_count=r["affected_count"])
                for r in cur.fetchall()
            ]

    # ---------------- 诊断/自测 ----------------
    def dump_records(self) -> list[dict]:
        """诊断/自测用：返回全部原始记录。"""
        with self._lock:
            cur = self._conn.execute("SELECT * FROM records ORDER BY date, patient_id, revision")
            return [dict(r) for r in cur]

    def dump_provenance(self) -> list[dict]:
        """诊断/自测用：返回记录到批次的来源关联。"""
        with self._lock:
            cur = self._conn.execute(
                "SELECT * FROM record_batches ORDER BY date, patient_id, revision, batch_id"
            )
            return [dict(r) for r in cur]

    def dump_release_sources(self) -> list[dict]:
        """诊断/自测用：返回发布结果到来源批次的快照。"""
        with self._lock:
            cur = self._conn.execute(
                "SELECT * FROM release_sources ORDER BY release_key, batch_id"
            )
            return [dict(r) for r in cur]
