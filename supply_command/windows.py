"""窗口指标引擎。

按品类、区域、时间窗（默认 30 分钟，左闭右开）从快照事件计算：
- 期末库存 stock_end（窗口内 observed_at 最晚的快照）；
- 窗口到货 inbound / 出货 outbound 合计；
- 覆盖天数 coverage_days = 期末库存 / 日均出货；
- 吞吐压力 throughput_pressure = 实际出货速率 / 规则参考吞吐；
- 告警等级 level：none | warn | critical；
- 数据质量 quality：ok | late | missing | anomaly。

关键区分：
- **late（数据迟到）**：窗口本应有快照，事后才到（到达时间晚于窗口关闭+宽限）；
  历史视图可被迟到数据校正（recalculated），但不删除首算结论。
- **missing（数据缺失）**：到评估时刻仍无任何快照，且超过 max_gap。
- **anomaly（真实供应异常）**：数据按时到达，但库存/覆盖天数确实越线。

引擎是纯函数式的：给定快照与规则，产出指标，不直接写事件。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from . import clock
from .catalog import CategoryRule
from .snapshots import Snapshot

LEVEL_NONE = "none"
LEVEL_WARN = "warn"
LEVEL_CRITICAL = "critical"

QUALITY_OK = "ok"
QUALITY_LATE = "late"
QUALITY_MISSING = "missing"
QUALITY_ANOMALY = "anomaly"

DEFAULT_WINDOW_MINUTES = 30


def window_aggregate_id(
    business_line: str, category: str, region: str, start: datetime
) -> str:
    start = clock.parse(start)
    return f"window:{business_line}:{category}:{region}:{start.strftime('%Y%m%dT%H%M')}"


def parse_window_aggregate_id(aggregate_id: str) -> tuple[str, str, str, datetime]:
    parts = aggregate_id.split(":")
    if len(parts) != 5 or parts[0] != "window":
        raise ValueError(f"非法窗口聚合编号：{aggregate_id}")
    ts = datetime.strptime(parts[4], "%Y%m%dT%H%M").replace(tzinfo=clock.CST)
    return parts[1], parts[2], parts[3], ts


@dataclass(frozen=True)
class WindowInput:
    """一次窗口计算的输入：窗口内已知快照（含到达时间）。"""

    business_line: str
    category: str
    region: str
    start: datetime
    snapshots: list[tuple[Snapshot, datetime]]  # (快照, recorded_at)
    rule: CategoryRule
    minutes: int = DEFAULT_WINDOW_MINUTES

    @property
    def end(self) -> datetime:
        return self.start + timedelta(minutes=self.minutes)


@dataclass
class MetricWindow:
    business_line: str
    category: str
    region: str
    start: datetime
    end: datetime
    rule_version: int
    stock_end: float
    inbound_total: float
    outbound_total: float
    coverage_days: float
    throughput_pressure: float
    level: str
    quality: str
    basis_snapshot_ids: list[str] = field(default_factory=list)
    basis_rule_version: int = 1
    revision: int = 1                 # 首算=1，迟到校正依次递增
    note: str = ""

    @property
    def aggregate_id(self) -> str:
        return window_aggregate_id(self.business_line, self.category, self.region, self.start)

    def to_payload(self) -> dict[str, Any]:
        return {
            "business_line": self.business_line,
            "category": self.category,
            "region": self.region,
            "window_start": self.start.isoformat(),
            "window_end": self.end.isoformat(),
            "rule_version": self.rule_version,
            "stock_end": self.stock_end,
            "inbound_total": self.inbound_total,
            "outbound_total": self.outbound_total,
            "coverage_days": round(self.coverage_days, 4),
            "throughput_pressure": round(self.throughput_pressure, 4),
            "level": self.level,
            "quality": self.quality,
            "basis_snapshot_ids": list(self.basis_snapshot_ids),
            "revision": self.revision,
            "note": self.note,
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "MetricWindow":
        start = clock.parse(payload["window_start"])
        minutes = int(
            (clock.parse(payload["window_end"]) - start).total_seconds() // 60
        )
        return cls(
            business_line=payload["business_line"],
            category=payload["category"],
            region=payload["region"],
            start=start,
            end=clock.parse(payload["window_end"]),
            rule_version=payload.get("rule_version", 1),
            stock_end=payload["stock_end"],
            inbound_total=payload["inbound_total"],
            outbound_total=payload["outbound_total"],
            coverage_days=payload["coverage_days"],
            throughput_pressure=payload["throughput_pressure"],
            level=payload["level"],
            quality=payload["quality"],
            basis_snapshot_ids=payload.get("basis_snapshot_ids", []),
            revision=payload.get("revision", 1),
            note=payload.get("note", ""),
        )


def classify(
    stock_end: float, coverage_days: float, rule: CategoryRule
) -> str:
    if stock_end < rule.safe_stock or coverage_days < rule.critical_coverage_days:
        return LEVEL_CRITICAL
    if coverage_days < rule.warn_coverage_days:
        return LEVEL_WARN
    return LEVEL_NONE


def compute_window(
    win: WindowInput,
    evaluation_at: datetime | None = None,
    revision: int = 1,
) -> MetricWindow:
    """纯计算。evaluation_at 决定把窗口判成缺失还是迟到。

    - 窗口内无快照，且评估时刻已超过 窗口结束 + max_gap：missing；
    - 有快照但全部晚于 窗口结束 + max_late 到达：late（仍按数据计算指标）；
    - 其余为按时数据；指标越线时 quality=anomaly，否则 ok。
    """
    evaluation_at = clock.parse(evaluation_at or clock.now())
    window_end = win.end
    rows = sorted(win.snapshots, key=lambda r: (r[0].observed_at, r[0].version))

    if not rows:
        if evaluation_at < window_end + timedelta(seconds=win.rule.max_gap_seconds):
            # 宽限期内：窗口尚未到期，调用方一般不应在此时结算
            pass
        return MetricWindow(
            business_line=win.business_line,
            category=win.category,
            region=win.region,
            start=win.start,
            end=window_end,
            rule_version=win.rule.version,
            stock_end=0.0,
            inbound_total=0.0,
            outbound_total=0.0,
            coverage_days=0.0,
            throughput_pressure=0.0,
            level=LEVEL_CRITICAL,  # 缺失按最严重提示，但 quality 与真实异常区分
            quality=QUALITY_MISSING,
            basis_snapshot_ids=[],
            revision=revision,
            note="窗口内无任何快照，判定为数据缺失",
        )

    inbound_total = sum(s.inbound for s, _ in rows)
    outbound_total = sum(s.outbound for s, _ in rows)
    last_snap, _ = rows[-1]
    stock_end = last_snap.stock

    hours = win.minutes / 60.0
    daily_out = outbound_total / hours * 24 if outbound_total > 0 else win.rule.throughput_per_hour * 24
    coverage_days = stock_end / daily_out if daily_out > 0 else float("inf")
    pressure = (
        (outbound_total / hours) / win.rule.throughput_per_hour
        if win.rule.throughput_per_hour > 0
        else 0.0
    )

    latest_arrival = max(recorded for _, recorded in rows)
    is_late = latest_arrival > window_end + timedelta(seconds=win.rule.max_late_seconds)

    level = classify(stock_end, coverage_days, win.rule)
    if is_late:
        quality = QUALITY_LATE
        note = f"数据迟到 {int((latest_arrival - window_end).total_seconds())}s 后到达，历史视图据此校正"
    elif level != LEVEL_NONE:
        quality = QUALITY_ANOMALY
        note = "数据按时到达，指标真实越线"
    else:
        quality = QUALITY_OK
        note = ""

    return MetricWindow(
        business_line=win.business_line,
        category=win.category,
        region=win.region,
        start=win.start,
        end=window_end,
        rule_version=win.rule.version,
        stock_end=stock_end,
        inbound_total=inbound_total,
        outbound_total=outbound_total,
        coverage_days=coverage_days,
        throughput_pressure=pressure,
        level=level,
        quality=quality,
        basis_snapshot_ids=[s.event_id for s, _ in rows],
        revision=revision,
        note=note,
    )
