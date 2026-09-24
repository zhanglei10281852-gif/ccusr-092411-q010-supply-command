"""只追加（append-only）事件存储。

SQLite 实现：单文件持久化，也支持 :memory:。存储只负责：
1. 追加事件并保证 event_id / 幂等键唯一；
2. 按聚合或全量、按 occurred_at / recorded_at 两种顺序重放。

业务语义（迟到、缺失、告警）不在此层。
"""
from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path

from .events import Event
from .errors import DuplicateError

SCHEMA = """
create table if not exists event_log (
    seq              integer primary key autoincrement,
    event_id         text not null unique,
    event_type       text not null,
    aggregate_id     text not null,
    occurred_at      text not null,
    recorded_at      text not null,
    idempotency_key  text unique,
    causation_id     text,
    correlation_id   text,
    actor            text,
    version          integer not null,
    payload          text not null
);
create index if not exists idx_event_agg on event_log(aggregate_id, occurred_at);
create index if not exists idx_event_occurred on event_log(occurred_at);
create index if not exists idx_event_recorded on event_log(recorded_at);
create index if not exists idx_event_corr on event_log(correlation_id);
"""


class EventStore:
    def __init__(self, path: str = ":memory:") -> None:
        self.path = path
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(SCHEMA)
        self._conn.commit()
        self._lock = threading.RLock()

    def close(self) -> None:
        self._conn.close()

    # ---- 写入 ----
    def append(self, event: Event) -> None:
        with self._lock:
            try:
                self._conn.execute(
                    "insert into event_log (event_id, event_type, aggregate_id, occurred_at, "
                    "recorded_at, idempotency_key, causation_id, correlation_id, actor, version, payload) "
                    "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        event.event_id,
                        event.event_type,
                        event.aggregate_id,
                        event.occurred_at.isoformat(),
                        event.recorded_at.isoformat(),
                        event.idempotency_key,
                        event.causation_id,
                        event.correlation_id,
                        event.actor,
                        event.version,
                        json.dumps(event.payload, ensure_ascii=False, sort_keys=True),
                    ),
                )
                self._conn.commit()
            except sqlite3.IntegrityError as exc:
                raise DuplicateError(f"事件重复：{event.event_id}（{exc}）") from exc

    def append_many(self, events: list[Event]) -> None:
        for event in events:
            self.append(event)

    # ---- 读取 ----
    @staticmethod
    def _row_to_event(row: sqlite3.Row) -> Event:
        import json

        return Event(
            event_id=row["event_id"],
            event_type=row["event_type"],
            aggregate_id=row["aggregate_id"],
            occurred_at=row["occurred_at"],
            payload=json.loads(row["payload"]),
            recorded_at=row["recorded_at"],
            idempotency_key=row["idempotency_key"],
            causation_id=row["causation_id"],
            correlation_id=row["correlation_id"],
            actor=row["actor"],
            version=row["version"],
        )

    def get(self, event_id: str) -> Event | None:
        row = self._conn.execute(
            "select * from event_log where event_id = ?", (event_id,)
        ).fetchone()
        return self._row_to_event(row) if row else None

    def has_idempotency_key(self, key: str) -> bool:
        row = self._conn.execute(
            "select 1 from event_log where idempotency_key = ?", (key,)
        ).fetchone()
        return row is not None

    def events_for(self, aggregate_id: str) -> list[Event]:
        rows = self._conn.execute(
            "select * from event_log where aggregate_id = ? order by occurred_at, seq",
            (aggregate_id,),
        ).fetchall()
        return [self._row_to_event(r) for r in rows]

    def all_events(self, order: str = "occurred_at") -> list[Event]:
        column = "recorded_at, seq" if order == "recorded_at" else "occurred_at, seq"
        rows = self._conn.execute(
            f"select * from event_log order by {column}"
        ).fetchall()
        return [self._row_to_event(r) for r in rows]

    def events_between(
        self, start, end, order: str = "occurred_at", event_types: tuple[str, ...] | None = None
    ) -> list[Event]:
        column = "recorded_at" if order == "recorded_at" else "occurred_at"
        sql = f"select * from event_log where {column} >= ? and {column} < ?"
        params: list = [start.isoformat(), end.isoformat()]
        if event_types:
            sql += f" and event_type in ({','.join('?' for _ in event_types)})"
            params.extend(event_types)
        sql += f" order by {column}, seq"
        rows = self._conn.execute(sql, params).fetchall()
        return [self._row_to_event(r) for r in rows]

    def correlation_chain(self, correlation_id: str) -> list[Event]:
        rows = self._conn.execute(
            "select * from event_log where correlation_id = ? order by occurred_at, seq",
            (correlation_id,),
        ).fetchall()
        return [self._row_to_event(r) for r in rows]

    def count(self) -> int:
        return self._conn.execute("select count(*) from event_log").fetchone()[0]
