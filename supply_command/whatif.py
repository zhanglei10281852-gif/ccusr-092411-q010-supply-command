"""沙盘推演：隔离地模拟仓容变化或到货变化，不污染真实时间线。

沙盘从事件存储复制快照事件到独立内存存储，应用假设条件后重算窗口，
输出"基准 vs 推演"对比。推演产生的事件带 ``whatif`` 标记，
永不写回主存储，也不会触发真实派单。
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from . import clock
from .catalog import Catalog
from .events import (
    Event,
    SNAPSHOT_RECEIVED,
    WINDOW_CALCULATED,
    WINDOW_RECALCULATED,
    WINDOW_MISSING,
    WHATIF_SCENARIO_APPLIED,
)
from .snapshots import Snapshot
from .windows import WindowInput, compute_window, DEFAULT_WINDOW_MINUTES, MetricWindow


@dataclass
class Scenario:
    name: str
    capacity_delta: float = 0.0          # 仓容调整（吨），叠加到期末库存
    inbound_multiplier: float = 1.0      # 到货量倍数
    inbound_extra: float = 0.0           # 每窗口额外到货（吨）
    outbound_multiplier: float = 1.0     # 出货量倍数
    affected_categories: list[str] | None = None  # None 表示全部
    start: datetime | None = None
    end: datetime | None = None


@dataclass
class ScenarioResult:
    scenario: str
    baseline: list[MetricWindow] = field(default_factory=list)
    projected: list[MetricWindow] = field(default_factory=list)

    def diff(self) -> list[dict[str, Any]]:
        base = {w.aggregate_id: w for w in self.baseline}
        out: list[dict[str, Any]] = []
        for w in self.projected:
            b = base.get(w.aggregate_id)
            if b is None:
                continue
            out.append(
                {
                    "window": w.aggregate_id,
                    "coverage_baseline": b.coverage_days,
                    "coverage_projected": w.coverage_days,
                    "level_baseline": b.level,
                    "level_projected": w.level,
                    "alert_would_change": b.level != w.level,
                }
            )
        return out


class WhatIfSandbox:
    def __init__(self, real_events: list[Event], catalog: Catalog) -> None:
        self.events = list(real_events)
        self.catalog = catalog

    def run(
        self,
        scenario: Scenario,
        minutes: int = DEFAULT_WINDOW_MINUTES,
    ) -> ScenarioResult:
        start = clock.parse(scenario.start) if scenario.start else None
        end = clock.parse(scenario.end) if scenario.end else None

        # 按窗口聚合真实快照
        buckets: dict[tuple[str, str, str, datetime], list[tuple[Snapshot, datetime]]] = {}
        for e in self.events:
            if e.event_type != SNAPSHOT_RECEIVED:
                continue
            snap = Snapshot.from_event(e)
            if start and snap.observed_at < start:
                continue
            if end and snap.observed_at >= end:
                continue
            if (
                scenario.affected_categories is not None
                and snap.category not in scenario.affected_categories
            ):
                continue
            wstart = clock.floor_window(snap.observed_at, minutes)
            key = (snap.business_line, snap.category, snap.region, wstart)
            buckets.setdefault(key, []).append((snap, e.recorded_at))

        result = ScenarioResult(scenario=scenario.name)
        step = timedelta(minutes=minutes)
        for (bl, cat, region, wstart), rows in sorted(buckets.items(), key=lambda x: x[0][3]):
            rule = self.catalog.rule_at(bl, cat, region, wstart)
            rows.sort(key=lambda r: (r[0].observed_at, r[0].version))

            base_win = WindowInput(bl, cat, region, wstart, rows, rule, minutes)
            base_mw = compute_window(base_win, evaluation_at=wstart + step)
            result.baseline.append(base_mw)

            # 应用假设：复制快照并改写数值（推演数据，不回写）
            sim_rows: list[tuple[Snapshot, datetime]] = []
            for snap, recorded in rows:
                import dataclasses as dc

                sim_inbound = snap.inbound * scenario.inbound_multiplier + scenario.inbound_extra
                sim_outbound = snap.outbound * scenario.outbound_multiplier
                sim_stock = snap.stock + scenario.capacity_delta + (sim_inbound - snap.inbound)
                sim = dc.replace(
                    snap,
                    inbound=max(sim_inbound, 0.0),
                    outbound=max(sim_outbound, 0.0),
                    stock=max(sim_stock, 0.0),
                    event_id=f"whatif-{uuid.uuid4().hex[:12]}",
                    source=f"{snap.source}|whatif:{scenario.name}",
                )
                sim_rows.append((sim, recorded))
            sim_win = WindowInput(bl, cat, region, wstart, sim_rows, rule, minutes)
            sim_mw = compute_window(sim_win, evaluation_at=wstart + step)
            sim_mw.note = f"沙盘推演：{scenario.name}"
            result.projected.append(sim_mw)
        return result
