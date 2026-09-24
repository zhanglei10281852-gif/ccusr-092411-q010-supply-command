"""数据快照：各业态带版本的数据快照与来源血缘。

快照聚合 ``snapshot:<business_line>:<category>:<region>`` 上只追加
``snapshot.received`` 事件；每条快照携带：
- 业务时间戳 observed_at（数据所描述的时刻）与系统到达时间 recorded_at；
- 单调递增的版本号，迟到数据以新版本追加、不覆盖旧版本；
- source（来源系统/点位）、batch（上报批次）、quality 标记。

投影层负责把事件流还原成"某业务时刻的最新已知版本"，从而支持迟到校正。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from . import clock
from .catalog import BUSINESS_LINES
from .errors import ValidationError
from .events import Event, SNAPSHOT_RECEIVED


def snapshot_aggregate_id(business_line: str, category: str, region: str) -> str:
    return f"snapshot:{business_line}:{category}:{region}"


def parse_aggregate_id(aggregate_id: str) -> tuple[str, str, str]:
    parts = aggregate_id.split(":")
    if len(parts) != 4 or parts[0] != "snapshot":
        raise ValidationError(f"非法快照聚合编号：{aggregate_id}")
    return parts[1], parts[2], parts[3]


@dataclass(frozen=True)
class Snapshot:
    business_line: str
    category: str
    region: str
    observed_at: datetime          # 数据所描述的业务时刻
    stock: float                    # 库存（吨）
    inbound: float = 0.0            # 窗口内到货（吨）
    outbound: float = 0.0           # 窗口内出货（吨）
    version: int = 1
    source: str = "unknown"         # 来源系统/计量点
    batch: str | None = None
    quality: str = "ok"             # ok | suspect | corrected
    note: str = ""
    event_id: str = ""
    recorded_at: datetime | None = None  # 网关到达时间；缺省由事件层取当前时刻

    @property
    def aggregate_id(self) -> str:
        return snapshot_aggregate_id(self.business_line, self.category, self.region)

    def validate(self) -> None:
        if self.business_line not in BUSINESS_LINES:
            raise ValidationError(f"未知业态：{self.business_line}")
        if self.stock < 0 or self.inbound < 0 or self.outbound < 0:
            raise ValidationError("库存/进出货量不能为负")
        if self.quality not in {"ok", "suspect", "corrected"}:
            raise ValidationError(f"非法质量标记：{self.quality}")

    def to_event(self, recorded_at: datetime | None = None) -> Event:
        self.validate()
        observed = clock.parse(self.observed_at)
        arrival = clock.parse(recorded_at or self.recorded_at) if (recorded_at or self.recorded_at) else None
        payload: dict[str, Any] = {
            "business_line": self.business_line,
            "category": self.category,
            "region": self.region,
            "observed_at": observed.isoformat(),
            "stock": self.stock,
            "inbound": self.inbound,
            "outbound": self.outbound,
            "source": self.source,
            "batch": self.batch,
            "quality": self.quality,
            "note": self.note,
            "version": self.version,
        }
        return Event(
            event_id=(
                self.event_id
                or f"snap-{self.business_line}-{self.category}-{self.region}"
                   f"-{observed.strftime('%Y%m%dT%H%M')}-v{self.version}"
            ),
            event_type=SNAPSHOT_RECEIVED,
            aggregate_id=self.aggregate_id,
            occurred_at=observed,
            payload=payload,
            recorded_at=arrival,
            idempotency_key=f"snap:{self.aggregate_id}:{observed.isoformat()}:{self.source}:{self.version}",
        )

    @classmethod
    def from_event(cls, event: Event) -> "Snapshot":
        p = event.payload
        return cls(
            business_line=p["business_line"],
            category=p["category"],
            region=p["region"],
            observed_at=clock.parse(p["observed_at"]),
            stock=p["stock"],
            inbound=p.get("inbound", 0.0),
            outbound=p.get("outbound", 0.0),
            version=p.get("version", 1),
            source=p.get("source", "unknown"),
            batch=p.get("batch"),
            quality=p.get("quality", "ok"),
            note=p.get("note", ""),
            event_id=event.event_id,
            recorded_at=event.recorded_at,
        )


@dataclass
class SnapshotSeries:
    """一个 (品类, 区域) 的全部快照版本，按业务时间+版本组织。"""

    aggregate_id: str
    snapshots: list[Snapshot] = field(default_factory=list)

    @classmethod
    def from_events(cls, events: list[Event]) -> "SnapshotSeries":
        if not events:
            raise ValidationError("空事件流无法构造快照序列")
        agg = events[0].aggregate_id
        snaps = [Snapshot.from_event(e) for e in events if e.event_type == SNAPSHOT_RECEIVED]
        snaps.sort(key=lambda s: (s.observed_at, s.version))
        return cls(aggregate_id=agg, snapshots=snaps)

    def latest_at(self, as_of: datetime, include_late: bool = True) -> Snapshot | None:
        """业务时刻 as-of 视图：observed_at <= as_of 的最新版本。

        include_late=False 时只使用 recorded_at <= as_of 的数据，
        即"当时实时看到的样子"（用于复盘夜班为何误判）。
        """
        as_of = clock.parse(as_of)
        chosen: Snapshot | None = None
        for snap in self.snapshots:
            if snap.observed_at > as_of:
                continue
            if not include_late and (
                snap.recorded_at is None or clock.parse(snap.recorded_at) > as_of
            ):
                continue
            if chosen is None or (snap.observed_at, snap.version) >= (
                chosen.observed_at,
                chosen.version,
            ):
                chosen = snap
        return chosen

    def latest_version_for_observed(self, observed_at: datetime) -> Snapshot | None:
        observed_at = clock.parse(observed_at)
        candidates = [s for s in self.snapshots if s.observed_at == observed_at]
        return max(candidates, key=lambda s: s.version, default=None)
