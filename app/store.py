"""SQLite 存储层：统一时间线 + 各实体表。

所有事实同时记录业务时间 occurred_at 与入库时间 recorded_at，
窗口、告警、处置单只追加新版本/新事件，从不覆盖历史。
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS events(
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT NOT NULL UNIQUE,
  event_type TEXT NOT NULL,
  aggregate_id TEXT NOT NULL,
  occurred_at TEXT NOT NULL,
  recorded_at TEXT NOT NULL,
  actor TEXT,
  payload TEXT NOT NULL DEFAULT '{}',
  idempotency_key TEXT UNIQUE
);
CREATE TABLE IF NOT EXISTS snapshots(
  snapshot_id TEXT PRIMARY KEY,
  business_line TEXT NOT NULL,
  region TEXT NOT NULL,
  category TEXT NOT NULL,
  version INTEGER NOT NULL,
  stock_qty REAL NOT NULL,
  safety_stock REAL NOT NULL,
  daily_throughput REAL NOT NULL,
  capacity REAL NOT NULL,
  occurred_at TEXT NOT NULL,
  recorded_at TEXT NOT NULL,
  late INTEGER NOT NULL DEFAULT 0,
  UNIQUE(business_line, region, category, version)
);
CREATE TABLE IF NOT EXISTS business_events(
  biz_event_id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  business_line TEXT NOT NULL,
  region TEXT NOT NULL,
  category TEXT NOT NULL,
  occurred_at TEXT NOT NULL,
  recorded_at TEXT NOT NULL,
  late INTEGER NOT NULL DEFAULT 0,
  payload TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS rules(
  rule_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  content TEXT NOT NULL,
  published_at TEXT NOT NULL,
  PRIMARY KEY(rule_id, version)
);
CREATE TABLE IF NOT EXISTS windows(
  window_pk INTEGER PRIMARY KEY AUTOINCREMENT,
  window_id TEXT NOT NULL,
  business_line TEXT NOT NULL,
  region TEXT NOT NULL,
  category TEXT NOT NULL,
  window_start TEXT NOT NULL,
  window_end TEXT NOT NULL,
  version INTEGER NOT NULL,
  coverage_days REAL,
  throughput_pressure REAL,
  data_status TEXT NOT NULL,
  alert_level TEXT NOT NULL,
  rule_version INTEGER NOT NULL,
  inputs TEXT NOT NULL,
  computed_at TEXT NOT NULL,
  UNIQUE(window_id, version)
);
CREATE TABLE IF NOT EXISTS alerts(
  alert_id TEXT PRIMARY KEY,
  fingerprint TEXT NOT NULL,
  business_line TEXT NOT NULL,
  region TEXT NOT NULL,
  category TEXT NOT NULL,
  window_id TEXT,
  kind TEXT NOT NULL,
  level TEXT NOT NULL,
  state TEXT NOT NULL,
  opened_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  merged_into TEXT
);
CREATE TABLE IF NOT EXISTS alert_events(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  alert_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  actor TEXT,
  reviewer TEXT,
  reason TEXT,
  at TEXT NOT NULL,
  payload TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS incidents(
  incident_id TEXT PRIMARY KEY,
  alert_id TEXT NOT NULL,
  owner TEXT NOT NULL,
  state TEXT NOT NULL,
  opened_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS incident_events(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  incident_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  actor TEXT,
  reviewer TEXT,
  from_owner TEXT,
  to_owner TEXT,
  judgment TEXT,
  reason TEXT,
  at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS dispatch_orders(
  dispatch_id TEXT PRIMARY KEY,
  incident_id TEXT NOT NULL,
  dedup_key TEXT NOT NULL UNIQUE,
  content TEXT NOT NULL,
  status TEXT NOT NULL,
  issued_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS dispatch_amendments(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  dispatch_id TEXT NOT NULL,
  content TEXT NOT NULL,
  reason TEXT,
  at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS operators(
  operator_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  role TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS shifts(
  shift_id TEXT PRIMARY KEY,
  opened_at TEXT NOT NULL,
  closed_at TEXT
);
CREATE TABLE IF NOT EXISTS handovers(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  shift_id TEXT NOT NULL,
  from_operator TEXT NOT NULL,
  to_operator TEXT NOT NULL,
  at TEXT NOT NULL,
  note TEXT
);
CREATE TABLE IF NOT EXISTS reports(
  report_id TEXT PRIMARY KEY,
  shift_id TEXT,
  as_of TEXT NOT NULL,
  published_at TEXT NOT NULL,
  items TEXT NOT NULL
);
"""


class Store:
    """SQLite 存储。`:memory:` 或文件路径；what-if 推演用 clone() 隔离。"""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self.path = str(path)
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        try:
            yield self.conn
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        return self.conn.execute(sql, params)

    def query(self, sql: str, params: tuple = ()) -> list[dict[str, Any]]:
        return [dict(row) for row in self.conn.execute(sql, params).fetchall()]

    def query_one(self, sql: str, params: tuple = ()) -> dict[str, Any] | None:
        row = self.conn.execute(sql, params).fetchone()
        return dict(row) if row is not None else None

    def clone(self) -> "Store":
        """把当前库完整复制到独立内存库，用于隔离推演。"""
        sandbox = Store(":memory:")
        self.conn.backup(sandbox.conn)
        return sandbox

    def close(self) -> None:
        self.conn.close()


def dumps(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def loads(payload: str | None) -> Any:
    return json.loads(payload) if payload else None
