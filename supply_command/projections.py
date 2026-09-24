"""统一时间线投影：双时态查询、迟到校正与补算。

时间线同时按两个维度组织事件：
- 业务时间 occurred_at：事件描述的事实时刻；
- 系统时间 recorded_at：事件实际到达的时刻。

提供两种历史视图：
- ``view_as_of(t, late=False)``：站在系统时刻 t 能看到的样子
  （只含 recorded_at <= t）——复盘"夜班当时为什么解除告警"用它；
- ``view_as_of(t, late=True)``：以现在已知的全部数据还原业务时刻 t
  ——迟到事件校正历史视图用它。

补算（backfill）：系统恢复后，对没有任何 window.calculated 的窗口重新结算；
已有窗口则在出现迟到快照时生成 revision=2.. 的 window.recalculated，
派单侧用幂等键保证不重复派单。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Iterable

from . import clock
from .events import (
    Event,
    SNAPSHOT_RECEIVED,
    WINDOW_CALCULATED,
    WINDOW_RECALCULATED,
    WINDOW_MISSING,
)
from .snapshots import Snapshot
from .windows import (
    MetricWindow,
    WindowInput,
    compute_window,
    window_aggregate_id,
    DEFAULT_WINDOW_MINUTES,
    QUALITY_MISSING,
)
from .catalog import Catalog


@dataclass
class TimelineEntry:
    occurred_at: datetime
    recorded_at: datetime
    event_type: str
    aggregate_id: str
    event_id: str
    late: bool
    payload: dict[str, Any] = field(default_factory=dict)


class TimelineProjection:
    """从事件存储重放出的只读统一时间线。"""

    def __init__(self, events: Iterable[Event], catalog: Catalog | None = None) -> None:
        self.events = sorted(events, key=lambda e: (e.occurred_at, e.recorded_at, e.event_id))
        self.catalog = catalog

    # ---- 统一时间线 ----
    def timeline(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
        business_line: str | None = None,
        category: str | None = None,
        region: str | None = None,
    ) -> list[TimelineEntry]:
        """统一时间线。

        迟到标记只对 ``snapshot.received`` 有业务含义：到达时间晚于
        其所属窗口关闭 + 规则 max_late_seconds；派生事件一律不标迟到。
        """
        out: list[TimelineEntry] = []
        for e in self.events:
            if start and e.occurred_at < start:
                continue
            if end and e.occurred_at >= end:
                continue
            p = e.payload
            if business_line and p.get("business_line") != business_line:
                continue
            if category and p.get("category") != category:
                continue
            if region and p.get("region") != region:
                continue
            late = False
            if e.event_type == SNAPSHOT_RECEIVED and self.catalog is not None:
                try:
                    rule = self.catalog.rule_at(
                        p["business_line"], p["category"], p["region"], e.occurred_at
                    )
                    window_end = clock.floor_window(
                        e.occurred_at, DEFAULT_WINDOW_MINUTES
                    ) + timedelta(minutes=DEFAULT_WINDOW_MINUTES)
                    late = e.recorded_at > window_end + timedelta(
                        seconds=rule.max_late_seconds
                    )
                except Exception:
                    late = False
            out.append(
                TimelineEntry(
                    occurred_at=e.occurred_at,
                    recorded_at=e.recorded_at,
                    event_type=e.event_type,
                    aggregate_id=e.aggregate_id,
                    event_id=e.event_id,
                    late=late,
                    payload=e.payload,
                )
            )
        return out

    # ---- 双时态快照视图 ----
    def snapshots_for(
        self,
        business_line: str,
        category: str,
        region: str,
        as_of: datetime,
        late: bool,
    ) -> list[tuple[Snapshot, datetime]]:
        """返回构造窗口输入所需的 (快照, 到达时间)。

        late=False：仅 recorded_at <= as_of（当时实时可见）；
        late=True ：使用全部已知快照（含迟到），按业务时间还原。
        """
        as_of = clock.parse(as_of)
        result: list[tuple[Snapshot, datetime]] = []
        for e in self.events:
            if e.event_type != SNAPSHOT_RECEIVED:
                continue
            p = e.payload
            if (
                p.get("business_line") != business_line
                or p.get("category") != category
                or p.get("region") != region
            ):
                continue
            if not late and e.recorded_at > as_of:
                continue
            result.append((Snapshot.from_event(e), e.recorded_at))
        return result

    def latest_known_snapshot(
        self,
        business_line: str,
        category: str,
        region: str,
        business_time: datetime,
        late: bool,
    ) -> tuple[Snapshot | None, datetime | None]:
        rows = self.snapshots_for(business_line, category, region, business_time, late)
        rows = [(s, r) for s, r in rows if s.observed_at <= business_time]
        if not rows:
            return None, None
        rows.sort(key=lambda r: (r[0].observed_at, r[0].version))
        return rows[-1]

    # ---- 窗口指标视图 ----
    def metric_windows(
        self,
        business_line: str,
        category: str,
        region: str,
        include_revisions: bool = True,
    ) -> list[MetricWindow]:
        """该品类区域的全部已发布窗口（含/不含修正版）。"""
        out: list[MetricWindow] = []
        for e in self.events:
            if e.event_type not in (WINDOW_CALCULATED, WINDOW_RECALCULATED, WINDOW_MISSING):
                continue
            p = e.payload
            if (
                p.get("business_line") != business_line
                or p.get("category") != category
                or p.get("region") != region
            ):
                continue
            out.append(MetricWindow.from_payload(p))
        if not include_revisions:
            latest: dict[str, MetricWindow] = {}
            for mw in out:
                key = mw.aggregate_id
                if key not in latest or mw.revision > latest[key].revision:
                    latest[key] = mw
            out = list(latest.values())
        out.sort(key=lambda mw: (mw.start, mw.revision))
        return out

    def published_window_ids(self) -> set[str]:
        return {
            e.aggregate_id
            for e in self.events
            if e.event_type in (WINDOW_CALCULATED, WINDOW_RECALCULATED, WINDOW_MISSING)
        }

    # ---- 补算：找出缺失窗口并重新结算 ----
    def backfill(
        self,
        catalog: Catalog,
        until: datetime,
        minutes: int = DEFAULT_WINDOW_MINUTES,
        grace_seconds: int = 0,
    ) -> list[tuple[str, MetricWindow]]:
        """对到期但从未发布过窗口指标的时间段补算。

        返回 (事件类型, 指标) 列表，由应用层决定如何发布。
        已有窗口若检测到迟到快照，由 :meth:`recalculate` 处理。
        """
        until = clock.parse(until)
        results: list[tuple[str, MetricWindow]] = []
        published = self.published_window_ids()

        # 收集出现过的 (业态, 品类, 区域) 与最早快照时间
        seen: dict[tuple[str, str, str], datetime] = {}
        for e in self.events:
            if e.event_type != SNAPSHOT_RECEIVED:
                continue
            p = e.payload
            key = (p["business_line"], p["category"], p["region"])
            ts = clock.parse(p["observed_at"])
            if key not in seen or ts < seen[key]:
                seen[key] = ts

        for (bl, cat, region), first_seen in seen.items():
            rule = catalog.rule_at(bl, cat, region, until)
            step = timedelta(minutes=minutes)
            wstart = clock.floor_window(first_seen, minutes)
            while wstart + step <= until - timedelta(seconds=grace_seconds):
                agg_id = window_aggregate_id(bl, cat, region, wstart)
                if agg_id not in published:
                    rows = self.snapshots_for(bl, cat, region, wstart + step, late=True)
                    rows = [
                        (s, r) for s, r in rows
                        if wstart <= s.observed_at < wstart + step and r <= until
                    ]
                    win = WindowInput(bl, cat, region, wstart, rows, rule, minutes)
                    mw = compute_window(win, evaluation_at=until)
                    etype = WINDOW_MISSING if mw.quality == QUALITY_MISSING else WINDOW_CALCULATED
                    results.append((etype, mw))
                wstart += step
        results.sort(key=lambda x: x[1].start)
        return results

    def recalculate(
        self,
        catalog: Catalog,
        minutes: int = DEFAULT_WINDOW_MINUTES,
    ) -> list[MetricWindow]:
        """找出已有窗口中、其首算之后又到达迟到快照的窗口，产出修正版。"""
        out: list[MetricWindow] = []
        window_events: dict[str, list[Event]] = {}
        for e in self.events:
            if e.event_type in (WINDOW_CALCULATED, WINDOW_RECALCULATED, WINDOW_MISSING):
                window_events.setdefault(e.aggregate_id, []).append(e)

        for agg_id, wevents in window_events.items():
            wevents.sort(key=lambda e: e.recorded_at)
            last_published = wevents[-1]
            payload = last_published.payload
            bl, cat, region = (
                payload["business_line"],
                payload["category"],
                payload["region"],
            )
            start = clock.parse(payload["window_start"])
            end = clock.parse(payload["window_end"])
            revision = payload.get("revision", 1)

            late_rows = self.snapshots_for(bl, cat, region, end, late=True)
            late_rows = [(s, r) for s, r in late_rows if start <= s.observed_at < end]
            arrived_after = [
                (s, r) for s, r in late_rows if r > last_published.recorded_at
            ]
            if not arrived_after:
                continue
            rule = catalog.rule_at(bl, cat, region, start)
            win = WindowInput(bl, cat, region, start, late_rows, rule, minutes)
            mw = compute_window(
                win, evaluation_at=clock.now(), revision=revision + 1
            )
            out.append(mw)
        out.sort(key=lambda mw: mw.start)
        return out
