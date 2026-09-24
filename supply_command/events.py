"""领域事件：事件溯源的核心记录。

每条事件同时带两个时间：
- ``occurred_at``：业务发生时间（事件实际生效的时刻）；
- ``recorded_at``：系统接收时间（进入时间线的时刻）。

两者之差即数据延迟。迟到事件（occurred_at 早于已发布视图边界）仍正常追加，
由投影层决定它是"校正历史"还是"无法解释"。
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any

from . import clock

# ---- 事件类型（与 domain/contract.json 对齐并扩展） ----
SNAPSHOT_RECEIVED = "snapshot.received"
SNAPSHOT_VERSION_PUBLISHED = "snapshot.version_published"
RULE_PUBLISHED = "rule.published"
WINDOW_CALCULATED = "window.calculated"
WINDOW_RECALCULATED = "window.recalculated"
WINDOW_MISSING = "window.missing"
ALERT_OPENED = "alert.opened"
ALERT_MERGED = "alert.merged"
ALERT_ESCALATED = "alert.escalated"
ALERT_TRANSFERRED = "alert.transferred"
ALERT_SUPPRESSED = "alert.suppressed"
ALERT_RESOLVED = "alert.resolved"
ALERT_CORRECTED = "alert.corrected"
INCIDENT_ASSIGNED = "incident.assigned"
INCIDENT_RESOLVED = "incident.resolved"
INCIDENT_AMENDED = "incident.amended"
DAILY_REPORT_PUBLISHED = "daily_report.published"
DAILY_REPORT_RECONCILED = "daily_report.reconciled"
WHATIF_SCENARIO_APPLIED = "whatif.scenario_applied"

EVENT_TYPES: frozenset[str] = frozenset(
    v for k, v in globals().items() if k.isupper() and isinstance(v, str) and "." in v
)


@dataclass(frozen=True)
class Event:
    event_id: str
    event_type: str
    aggregate_id: str
    occurred_at: datetime
    payload: dict[str, Any] = field(default_factory=dict)
    recorded_at: datetime | None = None
    idempotency_key: str | None = None
    causation_id: str | None = None  # 触发本事件的命令/上游事件
    correlation_id: str | None = None  # 同一告警链/处置链
    actor: str | None = None
    version: int = 1  # 该聚合的事件序号

    def __post_init__(self) -> None:
        if self.event_type not in EVENT_TYPES:
            raise ValueError(f"未知事件类型：{self.event_type}")
        object.__setattr__(self, "occurred_at", clock.parse(self.occurred_at))
        if self.recorded_at is None:
            object.__setattr__(self, "recorded_at", clock.now())
        else:
            object.__setattr__(self, "recorded_at", clock.parse(self.recorded_at))
        if self.recorded_at < self.occurred_at:
            raise ValueError("recorded_at 不能早于 occurred_at（不允许未来事件）")

    @property
    def is_late(self) -> bool:
        """迟到：到达时业务时间已经过去（由存储结合水位判断，这里是原始属性）。"""
        delay = (self.recorded_at - self.occurred_at).total_seconds()
        return delay > self.payload.get("late_threshold_seconds", 0)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["occurred_at"] = self.occurred_at.isoformat()
        data["recorded_at"] = self.recorded_at.isoformat()
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Event":
        return cls(
            event_id=data["event_id"],
            event_type=data["event_type"],
            aggregate_id=data["aggregate_id"],
            occurred_at=clock.parse(data["occurred_at"]),
            payload=data.get("payload", {}),
            recorded_at=clock.parse(data["recorded_at"]) if data.get("recorded_at") else None,
            idempotency_key=data.get("idempotency_key"),
            causation_id=data.get("causation_id"),
            correlation_id=data.get("correlation_id"),
            actor=data.get("actor"),
            version=data.get("version", 1),
        )

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True)
