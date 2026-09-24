"""处置单（incident）：告警驱动的派单与处置记录。

核心规则：
- 处置单一经发布即不可变；迟到数据校正历史视图后，原处置单**保留原判断**，
  只追加 ``incident.amended`` 修正记录，二者同时可查、可对账。
- 派单有稳定幂等键（窗口+告警+动作），系统恢复后补算遗漏窗口不会重复派单。
- 处置单跟随唯一责任链（correlation_id），班次交接只追加交接记录，不另起新单。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from .catalog import ACTION_ROLES
from .errors import AuthorizationError, IllegalTransition
from .events import (
    Event,
    INCIDENT_ASSIGNED,
    INCIDENT_RESOLVED,
    INCIDENT_AMENDED,
)

STATE_OPEN = "open"
STATE_HANDLED = "handled"
STATE_AMENDED = "amended"


@dataclass(frozen=True)
class Disposition:
    """处置单发布时的原始判断快照（不可变）。"""

    decision: str                  # 例如：补货、限流、现场核查
    rationale: str                 # 当时依据的文字说明
    level: str
    stock_end: float
    coverage_days: float
    quality: str                   # 当时认定的质量（可能事后被迟到数据推翻）
    basis_window_ids: list[str]
    decided_by: str
    decided_at: datetime


@dataclass
class Amendment:
    """事后修正：只追加，不改写原判断。"""

    amended_at: datetime
    amended_by: str
    reviewer: str
    original_quality: str
    corrected_quality: str
    note: str
    causation_event_id: str        # 触发修正的窗口重算/告警校正事件


@dataclass
class Incident:
    incident_id: str
    correlation_id: str
    alert_id: str
    business_line: str
    category: str
    region: str
    state: str = STATE_OPEN
    owner: str = ""
    original: Disposition | None = None
    handled_at: datetime | None = None
    handling_note: str = ""
    amendments: list[Amendment] = field(default_factory=list)
    history: list[Event] = field(default_factory=list)

    def apply(self, event: Event) -> None:
        p = event.payload
        etype = event.event_type
        if etype == INCIDENT_ASSIGNED:
            self.owner = p.get("owner", event.actor or "")
            self.original = Disposition(
                decision=p["decision"],
                rationale=p.get("rationale", ""),
                level=p.get("level", "warn"),
                stock_end=p.get("stock_end", 0.0),
                coverage_days=p.get("coverage_days", 0.0),
                quality=p.get("quality", "ok"),
                basis_window_ids=list(p.get("basis_window_ids", [])),
                decided_by=event.actor or "",
                decided_at=event.occurred_at,
            )
        elif etype == INCIDENT_RESOLVED:
            self.state = STATE_HANDLED
            self.handled_at = event.occurred_at
            self.handling_note = p.get("note", "")
        elif etype == INCIDENT_AMENDED:
            self.state = STATE_AMENDED if self.state == STATE_HANDLED else self.state
            self.amendments.append(
                Amendment(
                    amended_at=event.occurred_at,
                    amended_by=event.actor or "",
                    reviewer=p.get("reviewer", ""),
                    original_quality=p["original_quality"],
                    corrected_quality=p["corrected_quality"],
                    note=p.get("note", ""),
                    causation_event_id=p.get("causation_event_id", ""),
                )
            )
        self.history.append(event)

    @staticmethod
    def dispatch_idempotency_key(
        alert_id: str, window_id: str, decision: str
    ) -> str:
        return f"dispatch:{alert_id}:{window_id}:{decision}"

    @classmethod
    def assign_event(
        cls,
        incident_id: str,
        correlation_id: str,
        alert_id: str,
        business_line: str,
        category: str,
        region: str,
        owner: str,
        disposition: Disposition,
        actor_role: str,
        occurred_at: datetime,
        recorded_at: datetime | None = None,
    ) -> Event:
        if actor_role not in ACTION_ROLES["assign"]:
            raise AuthorizationError(f"角色 {actor_role} 无权派单")
        return Event(
            event_id=f"evt-incident-assign-{incident_id}",
            event_type=INCIDENT_ASSIGNED,
            aggregate_id=incident_id,
            occurred_at=occurred_at,
            payload={
                "alert_id": alert_id,
                "business_line": business_line,
                "category": category,
                "region": region,
                "owner": owner,
                "decision": disposition.decision,
                "rationale": disposition.rationale,
                "level": disposition.level,
                "stock_end": disposition.stock_end,
                "coverage_days": disposition.coverage_days,
                "quality": disposition.quality,
                "basis_window_ids": list(disposition.basis_window_ids),
            },
            correlation_id=correlation_id,
            actor=disposition.decided_by,
            recorded_at=recorded_at,
            idempotency_key=cls.dispatch_idempotency_key(
                alert_id,
                disposition.basis_window_ids[0] if disposition.basis_window_ids else "",
                disposition.decision,
            ),
        )

    def resolve_event(self, actor_id: str, actor_role: str, note: str, at: datetime) -> Event:
        if actor_role not in ACTION_ROLES["assign"]:
            raise AuthorizationError(f"角色 {actor_role} 无权办结处置单")
        if self.state not in {STATE_OPEN, STATE_AMENDED}:
            raise IllegalTransition("处置单仅在未办结状态可办结")
        return Event(
            event_id=f"evt-incident-resolve-{self.incident_id}-{at.strftime('%Y%m%d%H%M%S')}",
            event_type=INCIDENT_RESOLVED,
            aggregate_id=self.incident_id,
            occurred_at=at,
            payload={"note": note,
                     "business_line": self.business_line,
                     "category": self.category, "region": self.region},
            correlation_id=self.correlation_id,
            actor=actor_id,
        )

    def amend_event(
        self,
        amended_by: str,
        reviewer: str,
        corrected_quality: str,
        note: str,
        at: datetime,
        causation_event_id: str,
    ) -> Event:
        if self.original is None:
            raise IllegalTransition("无原始判断的处置单不能修正")
        if amended_by == reviewer:
            raise AuthorizationError("修正处置单需要另一人复核")
        return Event(
            event_id=f"evt-incident-amend-{self.incident_id}-{at.strftime('%Y%m%d%H%M%S')}",
            event_type=INCIDENT_AMENDED,
            aggregate_id=self.incident_id,
            occurred_at=at,
            payload={
                "original_quality": self.original.quality,
                "corrected_quality": corrected_quality,
                "note": note,
                "reviewer": reviewer,
                "causation_event_id": causation_event_id,
                "business_line": self.business_line,
                "category": self.category, "region": self.region,
            },
            correlation_id=self.correlation_id,
            actor=amended_by,
        )

    @classmethod
    def from_events(cls, incident_id: str, events: list[Event]) -> "Incident":
        if not events:
            raise IllegalTransition(f"处置单 {incident_id} 无事件")
        first = events[0]
        p = first.payload
        incident = cls(
            incident_id=incident_id,
            correlation_id=first.correlation_id or incident_id,
            alert_id=p["alert_id"],
            business_line=p["business_line"],
            category=p["category"],
            region=p["region"],
        )
        for event in events:
            incident.apply(event)
        return incident
