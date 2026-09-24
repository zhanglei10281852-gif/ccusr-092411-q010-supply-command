"""告警聚合：打开、合并、升级、转交、抑制、解除、事后校正。

状态机：

    alerted ──assign──▶ assigned ──┐
       │                            ├── escalate ──▶ escalated
       └────────────(escalate)─────┤
       ├── suppress ──▶ suppressed ──(reopen)──▶ alerted
       ├── transfer ──▶ （状态不变，责任链追加交接记录）
       └── resolve ───▶ resolved ──(迟到数据推翻前提)──▶ corrected

约束：
- 合并/去重：同一 (品类, 区域) 存在活跃告警时，重复上报只追加一条
  ``alert.merged``（指向主告警），不新增独立事件链；
- 升级、抑制、解除、事后修正需要双人复核（发起人 + 复核人，角色合规且非同一人）；
- 转交不改变唯一责任链：任何时刻只有一个当前责任方，交接记录按序追加；
- 解除必须给出可还原的数据依据（窗口指标编号列表），禁止仅凭汇总日报解除；
- resolved 之后若迟到数据推翻当时判断，状态进入 corrected，但处置单保留原判断。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from . import clock
from .catalog import (
    ACTION_ROLES,
    DUAL_CONTROL_ACTIONS,
    ROLE_AUDITOR,
)
from .errors import AuthorizationError, IllegalTransition, BasisError
from .events import (
    Event,
    ALERT_OPENED,
    ALERT_MERGED,
    ALERT_ESCALATED,
    ALERT_TRANSFERRED,
    ALERT_SUPPRESSED,
    ALERT_RESOLVED,
    ALERT_CORRECTED,
)

STATE_ALERTED = "alerted"
STATE_ASSIGNED = "assigned"
STATE_ESCALATED = "escalated"
STATE_SUPPRESSED = "suppressed"
STATE_RESOLVED = "resolved"
STATE_CORRECTED = "corrected"

ACTIVE_STATES = frozenset({STATE_ALERTED, STATE_ASSIGNED, STATE_ESCALATED, STATE_SUPPRESSED})
TERMINAL_STATES = frozenset({STATE_RESOLVED, STATE_CORRECTED})


@dataclass(frozen=True)
class Actor:
    user_id: str
    role: str
    shift: str = ""           # 班次标记，仅用于交接记录展示
    display_name: str = ""


@dataclass(frozen=True)
class Approval:
    """双人复核凭据：发起人与复核人。"""

    actor: Actor
    reviewer: Actor

    def validate(self, action: str) -> None:
        allowed = ACTION_ROLES.get(action, frozenset())
        if self.actor.role not in allowed:
            raise AuthorizationError(f"角色 {self.actor.role} 无权执行 {action}")
        if action in DUAL_CONTROL_ACTIONS:
            if self.actor.user_id == self.reviewer.user_id:
                raise AuthorizationError("双人复核要求发起人与复核人不是同一人")
            # 复核人可以是审计员，或具备同动作权限的另一人
            if self.reviewer.role != ROLE_AUDITOR and self.reviewer.role not in allowed:
                raise AuthorizationError(
                    f"复核人角色 {self.reviewer.role} 不能复核 {action}"
                )


@dataclass
class Handoff:
    """责任链上的一环。整条链顺序唯一，当前责任人始终只有一个。"""

    from_owner: str
    to_owner: str
    at: datetime
    actor: str
    reason: str = ""


@dataclass
class Alert:
    alert_id: str
    correlation_id: str
    business_line: str
    category: str
    region: str
    state: str = STATE_ALERTED
    level: str = "warn"
    opened_at: datetime | None = None
    opened_by: str = ""
    current_owner: str = ""
    window_ids: list[str] = field(default_factory=list)
    basis_window_ids: list[str] = field(default_factory=list)
    merged_into: str | None = None                 # 被合并时指向主告警
    merged_children: list[str] = field(default_factory=list)
    handoffs: list[Handoff] = field(default_factory=list)
    suppress_until: datetime | None = None
    suppress_reason: str = ""
    resolved_at: datetime | None = None
    resolution_basis: list[str] = field(default_factory=list)
    corrected_at: datetime | None = None
    correction_note: str = ""
    history: list[Event] = field(default_factory=list)

    # ---- 构造事件的命令（校验在此完成） ----
    def can_act(self, state_ok: bool, message: str) -> None:
        if not state_ok:
            raise IllegalTransition(f"告警 {self.alert_id} 状态 {self.state} 下不能{message}")

    def apply(self, event: Event) -> None:
        p = event.payload
        etype = event.event_type
        if etype == ALERT_OPENED:
            self.state = STATE_ALERTED
            self.level = p.get("level", "warn")
            self.opened_at = event.occurred_at
            self.opened_by = event.actor or ""
            self.current_owner = p.get("initial_owner", event.actor or "")
            self.window_ids = list(p.get("window_ids", []))
        elif etype == ALERT_MERGED:
            # 子告警上的归并事件指向主告警；主告警上的链接事件记录子告警
            if "parent_alert_id" in p:
                self.merged_into = p["parent_alert_id"]
            child = p.get("child_alert_id")
            if child and child not in self.merged_children:
                self.merged_children.append(child)
        elif etype == ALERT_ESCALATED:
            self.state = STATE_ESCALATED
            self.level = "critical"
            self.current_owner = p.get("new_owner", self.current_owner)
        elif etype == ALERT_TRANSFERRED:
            self.handoffs.append(
                Handoff(
                    from_owner=p["from_owner"],
                    to_owner=p["to_owner"],
                    at=event.occurred_at,
                    actor=event.actor or "",
                    reason=p.get("reason", ""),
                )
            )
            self.current_owner = p["to_owner"]
        elif etype == ALERT_SUPPRESSED:
            self.state = STATE_SUPPRESSED
            self.suppress_until = (
                clock.parse(p["until"]) if p.get("until") else None
            )
            self.suppress_reason = p.get("reason", "")
        elif etype == ALERT_RESOLVED:
            self.state = STATE_RESOLVED
            self.resolved_at = event.occurred_at
            self.resolution_basis = list(p.get("basis_window_ids", []))
        elif etype == "alert.corrected":
            self.state = STATE_CORRECTED
            self.corrected_at = event.occurred_at
            self.correction_note = p.get("note", "")
        self.history.append(event)

    @property
    def is_active(self) -> bool:
        """活跃 = 处于工作状态且未被合并进其他告警。"""
        return self.state in ACTIVE_STATES and self.merged_into is None

    @classmethod
    def open_event(
        cls,
        alert_id: str,
        correlation_id: str,
        business_line: str,
        category: str,
        region: str,
        level: str,
        window_ids: list[str],
        approval: Approval,
        occurred_at: datetime,
        initial_owner: str = "",
        recorded_at: datetime | None = None,
    ) -> Event:
        approval.validate("merge")  # 打开/上报与合并使用同一档权限
        return Event(
            event_id=f"evt-alert-open-{alert_id}",
            event_type=ALERT_OPENED,
            aggregate_id=alert_id,
            occurred_at=occurred_at,
            payload={
                "business_line": business_line,
                "category": category,
                "region": region,
                "level": level,
                "window_ids": list(window_ids),
                "initial_owner": initial_owner or approval.actor.user_id,
            },
            correlation_id=correlation_id,
            actor=approval.actor.user_id,
            recorded_at=recorded_at,
        )

    @classmethod
    def merge_event(
        cls,
        child_alert_id: str,
        parent_alert_id: str,
        correlation_id: str,
        approval: Approval,
        occurred_at: datetime,
        duplicate_window_id: str,
        business_line: str = "",
        category: str = "",
        region: str = "",
        recorded_at: datetime | None = None,
    ) -> Event:
        approval.validate("merge")
        return Event(
            event_id=f"evt-alert-merge-{child_alert_id}-{parent_alert_id.split(':')[-1]}",
            event_type=ALERT_MERGED,
            aggregate_id=child_alert_id,
            occurred_at=occurred_at,
            payload={
                "parent_alert_id": parent_alert_id,
                "duplicate_window_id": duplicate_window_id,
                "dedup": True,
                "business_line": business_line,
                "category": category, "region": region,
            },
            correlation_id=correlation_id,
            actor=approval.actor.user_id,
            recorded_at=recorded_at,
            idempotency_key=f"merge:{child_alert_id}:{parent_alert_id}",
        )

    def escalate_event(
        self, approval: Approval, reason: str, new_owner: str, at: datetime
    ) -> Event:
        approval.validate("escalate")
        self.can_act(self.state in ACTIVE_STATES - {STATE_SUPPRESSED}, "升级")
        return Event(
            event_id=f"evt-alert-escalate-{self.alert_id}-{at.strftime('%Y%m%d%H%M%S')}",
            event_type=ALERT_ESCALATED,
            aggregate_id=self.alert_id,
            occurred_at=at,
            payload={"reason": reason, "new_owner": new_owner,
                     "reviewer": approval.reviewer.user_id,
                     "business_line": self.business_line,
                     "category": self.category, "region": self.region},
            correlation_id=self.correlation_id,
            actor=approval.actor.user_id,
        )

    def transfer_event(
        self, approval: Approval, to_owner: str, at: datetime, reason: str = ""
    ) -> Event:
        approval.validate("transfer")
        self.can_act(self.state in ACTIVE_STATES, "转交")
        if to_owner == self.current_owner:
            raise IllegalTransition("转交目标与当前责任人相同")
        return Event(
            event_id=f"evt-alert-transfer-{self.alert_id}-{at.strftime('%Y%m%d%H%M%S')}",
            event_type=ALERT_TRANSFERRED,
            aggregate_id=self.alert_id,
            occurred_at=at,
            payload={
                "from_owner": self.current_owner,
                "to_owner": to_owner,
                "reason": reason,
                "shift_from": approval.actor.shift,
                "business_line": self.business_line,
                "category": self.category, "region": self.region,
            },
            correlation_id=self.correlation_id,
            actor=approval.actor.user_id,
        )

    def suppress_event(
        self, approval: Approval, until: datetime, reason: str, at: datetime
    ) -> Event:
        approval.validate("suppress")
        self.can_act(self.state in ACTIVE_STATES - {STATE_SUPPRESSED}, "抑制")
        if not reason:
            raise IllegalTransition("抑制必须给出原因")
        return Event(
            event_id=f"evt-alert-suppress-{self.alert_id}-{at.strftime('%Y%m%d%H%M%S')}",
            event_type=ALERT_SUPPRESSED,
            aggregate_id=self.alert_id,
            occurred_at=at,
            payload={
                "until": clock.parse(until).isoformat(),
                "reason": reason,
                "reviewer": approval.reviewer.user_id,
                "business_line": self.business_line,
                "category": self.category, "region": self.region,
            },
            correlation_id=self.correlation_id,
            actor=approval.actor.user_id,
        )

    def resolve_event(
        self,
        approval: Approval,
        basis_window_ids: list[str],
        at: datetime,
        recorded_at: datetime | None = None,
    ) -> Event:
        approval.validate("resolve")
        self.can_act(self.state in ACTIVE_STATES, "解除")
        if not basis_window_ids:
            raise BasisError(
                "解除告警必须引用可还原的窗口指标作为依据，不能仅凭汇总日报"
            )
        return Event(
            event_id=f"evt-alert-resolve-{self.alert_id}-{at.strftime('%Y%m%d%H%M%S')}",
            event_type=ALERT_RESOLVED,
            aggregate_id=self.alert_id,
            occurred_at=at,
            payload={
                "basis_window_ids": list(basis_window_ids),
                "reviewer": approval.reviewer.user_id,
                "business_line": self.business_line,
                "category": self.category, "region": self.region,
            },
            correlation_id=self.correlation_id,
            actor=approval.actor.user_id,
            recorded_at=recorded_at,
        )

    def correct_event(self, note: str, at: datetime, reviewer: Actor) -> Event:
        """迟到数据推翻已解除告警的前提：进入 corrected（不抹除解除记录）。"""
        if self.state != STATE_RESOLVED:
            raise IllegalTransition("只有已解除告警可被事后校正")
        return Event(
            event_id=f"evt-alert-correct-{self.alert_id}-{at.strftime('%Y%m%d%H%M%S')}",
            event_type=ALERT_CORRECTED,
            aggregate_id=self.alert_id,
            occurred_at=at,
            payload={"note": note, "reviewer": reviewer.user_id,
                     "business_line": self.business_line,
                     "category": self.category, "region": self.region},
            correlation_id=self.correlation_id,
            actor=reviewer.user_id,
        )

    @classmethod
    def from_events(cls, alert_id: str, events: list[Event]) -> "Alert":
        if not events:
            raise IllegalTransition(f"告警 {alert_id} 无事件")
        first = events[0]
        p = first.payload
        alert = cls(
            alert_id=alert_id,
            correlation_id=first.correlation_id or alert_id,
            business_line=p["business_line"],
            category=p["category"],
            region=p["region"],
        )
        for event in events:
            alert.apply(event)
        return alert
