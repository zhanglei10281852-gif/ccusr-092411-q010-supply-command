"""应用服务：保供指挥后端的命令侧门面。

把事件存储、目录、窗口引擎、告警/处置单工作流、补算、日报核对串成
一套可直接调用的服务（也被 HTTP API 层包装）。

所有写操作都走"构造领域事件 → 追加到只增日志"，重复命令通过
幂等键安全返回既有结果（重复上报不新增事件、恢复补算不重复派单）。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from . import clock
from .catalog import Catalog, CategoryRule
from .alerts import (
    Actor,
    Approval,
    Alert,
    STATE_RESOLVED,
)
from .events import (
    Event,
    RULE_PUBLISHED,
    SNAPSHOT_RECEIVED,
    WINDOW_CALCULATED,
    WINDOW_RECALCULATED,
    WINDOW_MISSING,
    ALERT_OPENED,
    ALERT_MERGED,
    ALERT_RESOLVED,
    INCIDENT_ASSIGNED,
    INCIDENT_AMENDED,
)
from .errors import DuplicateError, IllegalTransition, NotFound
from .incidents import Incident, Disposition
from .projections import TimelineProjection
from .snapshots import Snapshot, snapshot_aggregate_id
from .store import EventStore
from .windows import (
    MetricWindow,
    WindowInput,
    compute_window,
    window_aggregate_id,
    DEFAULT_WINDOW_MINUTES,
    LEVEL_WARN,
    LEVEL_CRITICAL,
    QUALITY_MISSING,
)


def actor_from(data: dict[str, Any]) -> Actor:
    return Actor(
        user_id=data["user_id"],
        role=data["role"],
        shift=data.get("shift", ""),
        display_name=data.get("display_name", ""),
    )


@dataclass
class IngestResult:
    event_id: str
    duplicate: bool
    late: bool
    recalculated_windows: list[str]


class SupplyCommandService:
    def __init__(self, store: EventStore, catalog: Catalog | None = None) -> None:
        self.store = store
        self.catalog = catalog or Catalog()
        self._replay_rules()

    def _replay_rules(self) -> None:
        """从事件存储重建带版本规则目录（持久化重启后恢复）。"""
        for e in self.store.all_events(order="recorded_at"):
            if e.event_type != RULE_PUBLISHED:
                continue
            p = e.payload
            valid_from = clock.parse(p["valid_from"]) if p.get("valid_from") else None
            rule = CategoryRule(
                business_line=p["business_line"],
                category=p["category"],
                region=p["region"],
                safe_stock=p["safe_stock"],
                warn_coverage_days=p["warn_coverage_days"],
                critical_coverage_days=p["critical_coverage_days"],
                throughput_per_hour=p.get("throughput_per_hour", 1.0),
                max_late_seconds=p.get("max_late_seconds", 3600),
                max_gap_seconds=p.get("max_gap_seconds", 1800),
                valid_from=valid_from,
                version=p.get("version", 1),
            )
            try:
                self.catalog.publish_rule(rule)
            except Exception:
                pass  # 重放幂等：版本已存在

    # ================= 规则 =================
    def publish_rule(self, rule: CategoryRule) -> Event:
        key = (
            f"rule:{rule.business_line}:{rule.category}:{rule.region}:v{rule.version}"
        )
        if self.store.has_idempotency_key(key):
            return self._find_event_by_idempotency(key)
        valid_from = rule.valid_from or clock.now()
        event = Event(
            event_id=f"evt-rule-{rule.business_line}-{rule.category}-"
                      f"{rule.region}-v{rule.version}",
            event_type=RULE_PUBLISHED,
            aggregate_id=f"rule:{rule.business_line}:{rule.category}:{rule.region}",
            occurred_at=valid_from,
            payload={
                "business_line": rule.business_line,
                "category": rule.category,
                "region": rule.region,
                "safe_stock": rule.safe_stock,
                "warn_coverage_days": rule.warn_coverage_days,
                "critical_coverage_days": rule.critical_coverage_days,
                "throughput_per_hour": rule.throughput_per_hour,
                "max_late_seconds": rule.max_late_seconds,
                "max_gap_seconds": rule.max_gap_seconds,
                "valid_from": valid_from.isoformat(),
                "version": rule.version,
            },
            idempotency_key=key,
        )
        self.store.append(event)
        self.catalog.publish_rule(rule)
        return event

    # ================= 快照摄入 =================
    def ingest(self, snapshot: Snapshot) -> IngestResult:
        event = snapshot.to_event()
        if self.store.has_idempotency_key(event.idempotency_key):
            return IngestResult(event_id=event.event_id, duplicate=True, late=False,
                                recalculated_windows=[])
        try:
            self.store.append(event)
        except DuplicateError:
            return IngestResult(event_id=event.event_id, duplicate=True, late=False,
                                recalculated_windows=[])

        # 迟到判定：到达时间晚于 窗口关闭 + 规则宽限
        late = False
        recalc_ids: list[str] = []
        try:
            rule = self.catalog.rule_at(
                snapshot.business_line, snapshot.category, snapshot.region,
                snapshot.observed_at,
            )
            window_end = clock.floor_window(snapshot.observed_at, DEFAULT_WINDOW_MINUTES) \
                + timedelta(minutes=DEFAULT_WINDOW_MINUTES)
            deadline = window_end + timedelta(seconds=rule.max_late_seconds)
            late = event.recorded_at > deadline
            if late:
                recalc_ids = self._recalc_due_to_snapshot(event)
        except NotFound:
            pass
        return IngestResult(event.event_id, False, late, recalc_ids)

    def _recalc_due_to_snapshot(self, snapshot_event: Event) -> list[str]:
        """迟到快照落在已结算窗口：追加 revision+1 的重算事件。"""
        p = snapshot_event.payload
        bl, cat, region = p["business_line"], p["category"], p["region"]
        observed = clock.parse(p["observed_at"])
        wstart = clock.floor_window(observed, DEFAULT_WINDOW_MINUTES)
        agg_id = window_aggregate_id(bl, cat, region, wstart)
        window_events = [
            e for e in self.store.events_for(agg_id)
            if e.event_type in (WINDOW_CALCULATED, WINDOW_RECALCULATED, WINDOW_MISSING)
        ]
        if not window_events:
            return []
        window_events.sort(key=lambda e: (e.recorded_at, e.event_id))
        last = window_events[-1]
        if snapshot_event.recorded_at <= last.recorded_at:
            return []

        revision = last.payload.get("revision", 1) + 1
        end = clock.parse(last.payload["window_end"])
        rule = self.catalog.rule_at(bl, cat, region, wstart)
        rows: list[tuple[Snapshot, datetime]] = []
        snap_agg = snapshot_aggregate_id(bl, cat, region)
        for e in self.store.events_for(snap_agg):
            if e.event_type != SNAPSHOT_RECEIVED:
                continue
            s = Snapshot.from_event(e)
            if wstart <= s.observed_at < end:
                rows.append((s, e.recorded_at))
        rows.sort(key=lambda r: (r[0].observed_at, r[0].version))
        win = WindowInput(bl, cat, region, wstart, rows, rule, DEFAULT_WINDOW_MINUTES)
        mw = compute_window(win, evaluation_at=snapshot_event.recorded_at, revision=revision)
        recalc_event = self._publish_window_event(
            WINDOW_RECALCULATED, mw, causation=snapshot_event.event_id,
            recorded_at=snapshot_event.recorded_at,
        )
        return [recalc_event.event_id]

    # ================= 窗口结算 =================
    def _publish_window_event(
        self, event_type: str, mw: MetricWindow, causation: str | None = None,
        recorded_at: datetime | None = None,
    ) -> Event:
        revision = mw.revision
        event = Event(
            event_id=f"evt-window-{mw.aggregate_id.split(':', 1)[1]}-r{revision}",
            event_type=event_type,
            aggregate_id=mw.aggregate_id,
            occurred_at=mw.end,
            payload=mw.to_payload(),
            idempotency_key=f"window:{mw.aggregate_id}:r{revision}",
            causation_id=causation,
            recorded_at=recorded_at,
        )
        if self.store.has_idempotency_key(event.idempotency_key):
            existing = self.store.get(event.event_id)
            return existing  # type: ignore[return-value]
        self.store.append(event)
        return event

    def close_window(
        self,
        business_line: str,
        category: str,
        region: str,
        start: datetime,
        minutes: int = DEFAULT_WINDOW_MINUTES,
        evaluation_at: datetime | None = None,
    ) -> Event:
        """结算单个窗口。到期无快照 → window.missing（数据缺失，区别于真实异常）。"""
        start = clock.floor_window(clock.parse(start), minutes)
        end = start + timedelta(minutes=minutes)
        evaluation_at = clock.parse(evaluation_at or clock.now())
        rule = self.catalog.rule_at(business_line, category, region, start)

        from .snapshots import snapshot_aggregate_id

        rows: list[tuple[Snapshot, datetime]] = []
        for e in self.store.events_for(snapshot_aggregate_id(business_line, category, region)):
            if e.event_type != SNAPSHOT_RECEIVED or e.recorded_at > evaluation_at:
                continue
            s = Snapshot.from_event(e)
            if start <= s.observed_at < end:
                rows.append((s, e.recorded_at))
        rows.sort(key=lambda r: (r[0].observed_at, r[0].version))

        win = WindowInput(business_line, category, region, start, rows, rule, minutes)
        mw = compute_window(win, evaluation_at=evaluation_at, revision=1)
        event_type = WINDOW_MISSING if mw.quality == QUALITY_MISSING else WINDOW_CALCULATED
        # 窗口指标的"系统发布时刻"即结算评估时刻，保证历史回放确定性
        return self._publish_window_event(event_type, mw, recorded_at=evaluation_at)

    # ================= 告警 =================
    def _load_alert(self, alert_id: str) -> Alert:
        events = self.store.events_for(alert_id)
        if not events:
            raise NotFound(f"告警不存在：{alert_id}")
        return Alert.from_events(alert_id, events)

    def find_active_alert(self, business_line: str, category: str, region: str) -> Alert | None:
        """该品类区域的活跃告警；若早先告警已被合并，则沿归并链找到主告警。"""
        for e in self.store.all_events():
            if e.event_type != ALERT_OPENED:
                continue
            p = e.payload
            if (p["business_line"], p["category"], p["region"]) != (
                business_line, category, region
            ):
                continue
            alert = self._load_alert(e.aggregate_id)
            while alert.merged_into is not None:
                alert = self._load_alert(alert.merged_into)
            if alert.is_active:
                return alert
        return None

    def report_alert(
        self,
        business_line: str,
        category: str,
        region: str,
        level: str,
        window_ids: list[str],
        approval: Approval,
        occurred_at: datetime | None = None,
        initial_owner: str = "",
        recorded_at: datetime | None = None,
    ) -> tuple[Event, bool]:
        """上报/打开告警；同品类区域已有活跃告警时去重（不新增事件）。

        返回 (事件, 是否新建)。
        """
        occurred_at = clock.parse(occurred_at or clock.now())
        existing = self.find_active_alert(business_line, category, region)
        if existing is not None:
            return existing.history[0], False

        alert_id = f"alert:{business_line}:{category}:{region}:{occurred_at.strftime('%Y%m%d%H%M%S')}"
        event = Alert.open_event(
            alert_id=alert_id,
            correlation_id=f"chain:{alert_id}",
            business_line=business_line,
            category=category,
            region=region,
            level=level,
            window_ids=window_ids,
            approval=approval,
            occurred_at=occurred_at,
            initial_owner=initial_owner,
            recorded_at=recorded_at,
        )
        self.store.append(event)
        return event, True

    def merge_alerts(
        self, child_id: str, parent_id: str, approval: Approval,
        at: datetime | None = None, duplicate_window_id: str = ""
    ) -> Event:
        child = self._load_alert(child_id)
        parent = self._load_alert(parent_id)
        at = clock.parse(at or clock.now())
        event = Alert.merge_event(
            child_alert_id=child_id,
            parent_alert_id=parent_id,
            correlation_id=parent.correlation_id,
            approval=approval,
            occurred_at=at,
            duplicate_window_id=duplicate_window_id,
            business_line=child.business_line,
            category=child.category,
            region=child.region,
        )
        if self.store.has_idempotency_key(
            f"merge:{child_id}:{parent_id}"
        ):
            return self.store.events_for(child_id)[-1]
        self.store.append(event)
        # 在主告警上记录归并关系（幂等保护）
        link_key = f"merge-link:{child_id}:{parent_id}"
        if not self.store.has_idempotency_key(link_key):
            self.store.append(Event(
                event_id=f"evt-alert-mergelink-{child_id}-{at.strftime('%Y%m%d%H%M%S')}",
                event_type=ALERT_MERGED,
                aggregate_id=parent_id,
                occurred_at=at,
                payload={"child_alert_id": child_id,
                         "duplicate_window_id": duplicate_window_id,
                         "business_line": parent.business_line,
                         "category": parent.category,
                         "region": parent.region},
                correlation_id=parent.correlation_id,
                actor=approval.actor.user_id,
                idempotency_key=link_key,
            ))
        return event

    def _alert_action(self, alert_id: str, builder) -> Event:
        alert = self._load_alert(alert_id)
        event = builder(alert)
        self.store.append(event)
        alert.apply(event)
        return event

    def escalate_alert(self, alert_id: str, approval: Approval, reason: str,
                       new_owner: str, at: datetime | None = None) -> Event:
        at = clock.parse(at or clock.now())
        return self._alert_action(
            alert_id, lambda a: a.escalate_event(approval, reason, new_owner, at)
        )

    def transfer_alert(self, alert_id: str, approval: Approval, to_owner: str,
                       at: datetime | None = None, reason: str = "") -> Event:
        at = clock.parse(at or clock.now())
        return self._alert_action(
            alert_id, lambda a: a.transfer_event(approval, to_owner, at, reason)
        )

    def suppress_alert(self, alert_id: str, approval: Approval, until: datetime,
                       reason: str, at: datetime | None = None) -> Event:
        until = clock.parse(until)
        at = clock.parse(at or clock.now())
        return self._alert_action(
            alert_id, lambda a: a.suppress_event(approval, until, reason, at)
        )

    def resolve_alert(self, alert_id: str, approval: Approval,
                      basis_window_ids: list[str], at: datetime | None = None) -> Event:
        at = clock.parse(at or clock.now())
        return self._alert_action(
            alert_id, lambda a: a.resolve_event(approval, basis_window_ids, at)
        )

    def correct_after_recalc(
        self, alert_id: str, recalc_event_id: str, reviewer: Actor,
        note: str, at: datetime | None = None
    ) -> list[Event]:
        """迟到数据推翻已解除告警：告警转 corrected，关联处置单追加修正。

        原解除事件与处置单原判断均保留，只追加。
        """
        at = clock.parse(at or clock.now())
        alert = self._load_alert(alert_id)
        recalc = self.store.get(recalc_event_id)
        if recalc is None:
            raise NotFound(f"重算事件不存在：{recalc_event_id}")
        if alert.state != STATE_RESOLVED:
            raise IllegalTransition("仅已解除告警可做事后校正")
        if recalc_event_id in {e.event_id for e in alert.history}:
            return []
        corrected = alert.correct_event(note, at, reviewer)
        self.store.append(corrected)

        out: list[Event] = [corrected]
        # 原解除人（告警判断的发布者）作为修正发起人
        resolver = next(
            (e.actor for e in reversed(alert.history) if e.event_type == ALERT_RESOLVED),
            reviewer.user_id,
        )
        # 同一责任链上的全部处置单都追加修正：告警前提被推翻，链上每张单都受影响。
        # 原判断仍完整保留在 incident.assigned 事件中。
        for e in self.store.correlation_chain(alert.correlation_id):
            if e.event_type != INCIDENT_ASSIGNED:
                continue
            inc = self._load_incident(e.aggregate_id)
            amend = inc.amend_event(
                amended_by=resolver,
                reviewer=reviewer.user_id,
                corrected_quality=recalc.payload.get("quality", "late"),
                note=f"迟到数据校正：{note}（依据窗口 {recalc.aggregate_id} r{recalc.payload.get('revision', 1)}）",
                at=at,
                causation_event_id=recalc_event_id,
            )
            self.store.append(amend)
            out.append(amend)
        return out

    # ================= 处置单 =================
    def _load_incident(self, incident_id: str) -> Incident:
        events = self.store.events_for(incident_id)
        if not events:
            raise NotFound(f"处置单不存在：{incident_id}")
        return Incident.from_events(incident_id, events)

    def dispatch_incident(
        self,
        alert_id: str,
        owner: str,
        decision: str,
        rationale: str,
        actor: Actor,
        at: datetime | None = None,
    ) -> tuple[Event, bool]:
        """派单。幂等键 = 告警+首依据窗口+决策，重复派单返回既有事件。"""
        at = clock.parse(at or clock.now())
        alert = self._load_alert(alert_id)

        # 取告警关联窗口中业务时间最近的一版指标作为处置依据
        window_ids = self._alert_window_ids(alert)
        latest_window = self._latest_window(window_ids)
        if latest_window is None:
            raise IllegalTransition("派单必须基于至少一个已结算窗口指标")

        disposition = Disposition(
            decision=decision,
            rationale=rationale,
            level=latest_window.level,
            stock_end=latest_window.stock_end,
            coverage_days=latest_window.coverage_days,
            quality=latest_window.quality,
            basis_window_ids=[latest_window.aggregate_id],
            decided_by=actor.user_id,
            decided_at=at,
        )
        incident_id = (
            f"incident:{alert.business_line}:{alert.category}:{alert.region}:"
            f"{at.strftime('%Y%m%d%H%M%S')}"
        )
        event = Incident.assign_event(
            incident_id=incident_id,
            correlation_id=alert.correlation_id,
            alert_id=alert_id,
            business_line=alert.business_line,
            category=alert.category,
            region=alert.region,
            owner=owner,
            disposition=disposition,
            actor_role=actor.role,
            occurred_at=at,
        )
        if self.store.has_idempotency_key(event.idempotency_key):
            prior = self._find_event_by_idempotency(event.idempotency_key)
            return prior, False
        self.store.append(event)
        return event, True

    def _find_event_by_idempotency(self, key: str) -> Event:
        import sqlite3
        row = self.store._conn.execute(
            "select * from event_log where idempotency_key = ?", (key,)
        ).fetchone()
        return self.store._row_to_event(row)

    def _alert_window_ids(self, alert: Alert) -> list[str]:
        ids = set(alert.window_ids)
        for e in alert.history:
            if e.event_type == ALERT_RESOLVED:
                ids.update(e.payload.get("basis_window_ids", []))
            if e.event_type == ALERT_OPENED:
                ids.update(e.payload.get("window_ids", []))
        return sorted(ids)

    def _latest_window(self, window_ids: list[str]) -> MetricWindow | None:
        best: MetricWindow | None = None
        for wid in window_ids:
            events = [
                e for e in self.store.events_for(wid)
                if e.event_type in (WINDOW_CALCULATED, WINDOW_RECALCULATED, WINDOW_MISSING)
            ]
            if not events:
                continue
            events.sort(key=lambda e: e.recorded_at)
            mw = MetricWindow.from_payload(events[-1].payload)
            if best is None or mw.start > best.start or (
                mw.start == best.start and mw.revision > best.revision
            ):
                best = mw
        return best

    def resolve_incident(self, incident_id: str, actor: Actor, note: str,
                         at: datetime | None = None) -> Event:
        at = clock.parse(at or clock.now())
        inc = self._load_incident(incident_id)
        event = inc.resolve_event(actor.user_id, actor.role, note, at)
        self.store.append(event)
        return event

    # ================= 恢复补算 =================
    def recover(self, until: datetime, minutes: int = DEFAULT_WINDOW_MINUTES) -> dict[str, Any]:
        """系统恢复后：补算遗漏窗口（缺失/迟到都如实标注），不重复派单。

        - 从未结算的窗口 → 补发 window.calculated / window.missing；
        - 已结算但有更新迟到快照 → 在摄入时已逐条 recalc；
        - 对补算发现的严重窗口补开告警（同品类去重），派单幂等由
          :meth:`dispatch_incident` 的稳定幂等键保证。
        """
        until = clock.parse(until)
        proj = TimelineProjection(self.store.all_events())
        planned = proj.backfill(self.catalog, until, minutes=minutes)
        published: list[str] = []
        alerts_opened: list[str] = []
        for etype, mw in planned:
            event = self._publish_window_event(etype, mw, recorded_at=until)
            published.append(event.event_id)
            if mw.level in {LEVEL_WARN, LEVEL_CRITICAL} and mw.quality != QUALITY_MISSING:
                # 补开告警需服务身份；这里使用系统值守账号
                sysop = Actor("system-recovery", "operator", shift="recovery")
                reviewer = Actor("system-auditor", "auditor", shift="recovery")
                approval = Approval(sysop, reviewer)
                alert_event, created = self.report_alert(
                    mw.business_line, mw.category, mw.region, mw.level,
                    [mw.aggregate_id], approval, occurred_at=mw.end,
                    initial_owner="system-recovery", recorded_at=until,
                )
                if created:
                    alerts_opened.append(alert_event.aggregate_id)
        return {
            "windows_backfilled": len(published),
            "window_event_ids": published,
            "alerts_opened": alerts_opened,
        }

    # ================= 查询 =================
    def projection(self) -> TimelineProjection:
        return TimelineProjection(self.store.all_events(), self.catalog)

    def list_alerts(self) -> list[Alert]:
        ids = {
            e.aggregate_id for e in self.store.all_events()
            if e.event_type == ALERT_OPENED
        }
        return [self._load_alert(i) for i in sorted(ids)]

    def list_incidents(self) -> list[Incident]:
        ids = {e.aggregate_id for e in self.store.all_events()
               if e.event_type in {INCIDENT_ASSIGNED, INCIDENT_AMENDED}}
        return [self._load_incident(i) for i in sorted(ids)]
