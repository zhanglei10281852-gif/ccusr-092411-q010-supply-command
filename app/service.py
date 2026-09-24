"""保供指挥核心服务。

统一时间线（events 表）承载全部事实；快照、窗口、告警、处置单只追加不覆盖。
覆盖需求：
- 多业态带版本快照与业务事件汇入统一时间线，重复上报不新增事件（幂等键）。
- 按品类/区域/时间窗计算覆盖天数、吞吐压力与告警等级。
- 区分数据迟到 / 数据缺失 / 真实供应异常；迟到数据校正历史视图。
- 已发布处置单保留原判断，修正以追加方式记录。
- 告警合并/升级/转交/抑制/解除受角色与双人复核约束。
- 班次交接不改变唯一责任链。
- 任一指标可追血缘；what-if 推演与主库隔离。
- 系统恢复后补算遗漏窗口而不重复派单；日报可逐项核对。
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from typing import Any

from .clock import Clock, format_value, parse
from .rules import RuleBook
from .store import Store, dumps, loads

LEVEL_RANK = {"none": 0, "info": 1, "warning": 2, "critical": 3}
ACTIVE_ALERT_STATES = {"observing", "alerted", "assigned", "escalated", "suppressed"}
TERMINAL_ALERT_STATES = {"resolved", "corrected"}
REVIEWED_ACTIONS = {"merge", "escalate", "transfer", "suppress", "resolve", "assign"}


class DomainError(Exception):
    """业务规则冲突。"""


class CommandService:
    def __init__(self, store: Store, clock: Clock) -> None:
        self.store = store
        self.clock = clock
        self.rules = RuleBook(store, clock)

    # ------------------------------------------------------------------ 工具
    def _emit(
        self,
        event_type: str,
        aggregate_id: str,
        payload: dict[str, Any] | None = None,
        actor: str | None = None,
        occurred_at: str | None = None,
        idempotency_key: str | None = None,
    ) -> tuple[str, bool]:
        """写入统一时间线。幂等键重复时不新增事件，返回 (event_id, created)。"""
        if idempotency_key is not None:
            row = self.store.query_one(
                "SELECT event_id FROM events WHERE idempotency_key = ?", (idempotency_key,)
            )
            if row is not None:
                return row["event_id"], False
        event_id = f"evt-{uuid.uuid4().hex[:12]}"
        occurred = occurred_at or format_value(self.clock.now())
        with self.store.transaction():
            self.store.execute(
                "INSERT INTO events(event_id, event_type, aggregate_id, occurred_at,"
                " recorded_at, actor, payload, idempotency_key) VALUES (?,?,?,?,?,?,?,?)",
                (
                    event_id,
                    event_type,
                    aggregate_id,
                    occurred,
                    format_value(self.clock.now()),
                    actor,
                    dumps(payload or {}),
                    idempotency_key,
                ),
            )
        return event_id, True

    def _operator(self, operator_id: str) -> dict[str, Any]:
        row = self.store.query_one("SELECT * FROM operators WHERE operator_id = ?", (operator_id,))
        if row is None:
            raise DomainError(f"未注册的操作员：{operator_id}")
        return row

    def _dual_review(self, action: str, actor: str, reviewer: str) -> None:
        """双人复核：动作受角色约束，复核人须为指挥长且不得是申请人本人。"""
        if action not in REVIEWED_ACTIONS:
            raise DomainError(f"未知受控动作：{action}")
        actor_row = self._operator(actor)
        reviewer_row = self._operator(reviewer)
        if actor_row["role"] not in ("operator", "commander"):
            raise DomainError(f"{actor} 无权发起 {action}")
        if reviewer_row["role"] != "commander":
            raise DomainError(f"复核人 {reviewer} 必须具备 commander 角色")
        if actor == reviewer:
            raise DomainError("申请人与复核人不得为同一人")

    # -------------------------------------------------------------- 基础资料
    def register_operator(self, operator_id: str, name: str, role: str) -> None:
        if role not in ("operator", "commander"):
            raise DomainError(f"非法角色：{role}")
        with self.store.transaction():
            self.store.execute(
                "INSERT OR REPLACE INTO operators(operator_id, name, role) VALUES (?,?,?)",
                (operator_id, name, role),
            )

    def open_shift(self, shift_id: str, operator_id: str) -> None:
        self._operator(operator_id)
        with self.store.transaction():
            self.store.execute(
                "INSERT INTO shifts(shift_id, opened_at) VALUES (?,?)",
                (shift_id, format_value(self.clock.now())),
            )
        self._emit("shift.opened", shift_id, {"operator": operator_id}, actor=operator_id)

    def handover_shift(self, shift_id: str, from_operator: str, to_operator: str, note: str = "") -> None:
        """班次交接：只记录交接事实，不改动任何处置单责任链。"""
        self._operator(from_operator)
        self._operator(to_operator)
        if from_operator == to_operator:
            raise DomainError("交接双方不得为同一人")
        shift = self.store.query_one("SELECT * FROM shifts WHERE shift_id = ?", (shift_id,))
        if shift is None or shift["closed_at"] is not None:
            raise DomainError(f"班次不存在或已关闭：{shift_id}")
        now = format_value(self.clock.now())
        with self.store.transaction():
            self.store.execute(
                "INSERT INTO handovers(shift_id, from_operator, to_operator, at, note)"
                " VALUES (?,?,?,?,?)",
                (shift_id, from_operator, to_operator, now, note),
            )
        self._emit(
            "shift.handed_over",
            shift_id,
            {"from": from_operator, "to": to_operator, "note": note},
            actor=from_operator,
        )

    # -------------------------------------------------------------- 数据摄入
    def ingest_snapshot(
        self,
        business_line: str,
        region: str,
        category: str,
        version: int,
        stock_qty: float,
        safety_stock: float,
        daily_throughput: float,
        capacity: float,
        occurred_at: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        """带版本快照入库。重复上报（同幂等键）不新增事件；迟到快照触发窗口校正。"""
        snapshot_id = f"{business_line}|{region}|{category}|v{version}"
        existing = self.store.query_one(
            "SELECT snapshot_id FROM snapshots WHERE business_line=? AND region=?"
            " AND category=? AND version=?",
            (business_line, region, category, version),
        )
        if existing is not None and self.store.query_one(
            "SELECT event_id FROM events WHERE idempotency_key=?", (idempotency_key,)
        ) is None:
            raise DomainError(f"版本冲突：{snapshot_id} 已存在且内容不同")
        event_id, created = self._emit(
            "snapshot.received", snapshot_id, {"snapshot_id": snapshot_id},
            occurred_at=occurred_at, idempotency_key=idempotency_key,
        )
        if not created:
            return {"snapshot_id": snapshot_id, "event_id": event_id, "duplicate": True, "late": False}

        occurred = parse(occurred_at)
        recorded = self.clock.now()
        rule = self.rules.for_line(business_line)
        late = (recorded - occurred).total_seconds() > rule["content"]["late_threshold_seconds"]
        with self.store.transaction():
            self.store.execute(
                "INSERT INTO snapshots VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    snapshot_id, business_line, region, category, version,
                    stock_qty, safety_stock, daily_throughput, capacity,
                    format_value(occurred), format_value(recorded), int(late),
                ),
            )
        result = {"snapshot_id": snapshot_id, "event_id": event_id, "duplicate": False, "late": late}
        if late:
            result["corrections"] = self._correct_windows_for(
                business_line, region, category, occurred
            )
        return result

    def ingest_business_event(
        self,
        biz_event_id: str,
        kind: str,
        business_line: str,
        region: str,
        category: str,
        occurred_at: str,
        payload: dict[str, Any],
        idempotency_key: str,
    ) -> dict[str, Any]:
        event_id, created = self._emit(
            "business_event.received", biz_event_id, {"kind": kind, **payload},
            occurred_at=occurred_at, idempotency_key=idempotency_key,
        )
        if not created:
            return {"biz_event_id": biz_event_id, "event_id": event_id, "duplicate": True, "late": False}
        occurred = parse(occurred_at)
        recorded = self.clock.now()
        rule = self.rules.for_line(business_line)
        late = (recorded - occurred).total_seconds() > rule["content"]["late_threshold_seconds"]
        with self.store.transaction():
            self.store.execute(
                "INSERT INTO business_events VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    biz_event_id, kind, business_line, region, category,
                    format_value(occurred), format_value(recorded), int(late), dumps(payload),
                ),
            )
        return {"biz_event_id": biz_event_id, "event_id": event_id, "duplicate": False, "late": late}

    # -------------------------------------------------------------- 窗口计算
    @staticmethod
    def window_id(business_line: str, region: str, category: str, window_start: str) -> str:
        return f"{business_line}|{region}|{category}|{window_start}"

    def _compute_metrics(
        self,
        business_line: str,
        region: str,
        category: str,
        window_start: datetime,
        window_end: datetime,
        as_of: datetime,
    ) -> dict[str, Any]:
        """纯函数：按 recorded_at <= as_of 回放可见事实，计算窗口指标。"""
        rule = self.rules.for_line(business_line)
        content = rule["content"]
        snapshots = self.store.query(
            "SELECT * FROM snapshots WHERE business_line=? AND region=? AND category=?"
            " AND occurred_at < ? AND recorded_at <= ?"
            " ORDER BY occurred_at DESC, version DESC",
            (business_line, region, category, format_value(window_end), format_value(as_of)),
        )
        events = self.store.query(
            "SELECT biz_event_id FROM business_events WHERE business_line=? AND region=?"
            " AND category=? AND occurred_at >= ? AND occurred_at < ? AND recorded_at <= ?",
            (
                business_line, region, category,
                format_value(window_start), format_value(window_end), format_value(as_of),
            ),
        )
        inputs = {
            "snapshots": [s["snapshot_id"] for s in snapshots],
            "business_events": [e["biz_event_id"] for e in events],
            "rule": f"{rule['rule_id']}@{rule['version']}",
        }
        max_age = timedelta(seconds=content["snapshot_max_age_seconds"])
        snapshot = snapshots[0] if snapshots else None
        if snapshot is None or parse(snapshot["occurred_at"]) < window_start - max_age:
            return {
                "coverage_days": None, "throughput_pressure": None,
                "data_status": "missing", "alert_level": "none", "inputs": inputs,
            }
        throughput = snapshot["daily_throughput"]
        capacity = snapshot["capacity"]
        coverage = round(snapshot["stock_qty"] / throughput, 4) if throughput > 0 else 999.0
        pressure = round(throughput / capacity, 4) if capacity > 0 else 0.0
        if coverage < content["safety_coverage_days"]:
            level = "critical"
        elif coverage < content["warning_coverage_days"]:
            level = "warning"
        else:
            level = "none"
        if pressure >= content["pressure_critical"]:
            level = "critical"
        elif pressure >= content["pressure_warning"] and LEVEL_RANK[level] < LEVEL_RANK["warning"]:
            level = "warning"
        data_status = "late" if snapshot["late"] else "ok"
        return {
            "coverage_days": coverage, "throughput_pressure": pressure,
            "data_status": data_status, "alert_level": level, "inputs": inputs,
        }

    def compute_window(
        self,
        business_line: str,
        region: str,
        category: str,
        window_start: str,
        window_end: str,
        as_of: str | None = None,
        reason: str = "scheduled",
    ) -> dict[str, Any]:
        """计算（或重算）一个窗口，追加新版本；按结果自动开合告警。"""
        start, end = parse(window_start), parse(window_end)
        as_of_dt = parse(as_of) if as_of else self.clock.now()
        metrics = self._compute_metrics(business_line, region, category, start, end, as_of_dt)
        rule = self.rules.for_line(business_line)
        wid = self.window_id(business_line, region, category, window_start)
        row = self.store.query_one("SELECT MAX(version) AS v FROM windows WHERE window_id=?", (wid,))
        version = (row["v"] or 0) + 1
        now = format_value(self.clock.now())
        with self.store.transaction():
            self.store.execute(
                "INSERT INTO windows(window_id, business_line, region, category, window_start,"
                " window_end, version, coverage_days, throughput_pressure, data_status,"
                " alert_level, rule_version, inputs, computed_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    wid, business_line, region, category, format_value(start), format_value(end),
                    version, metrics["coverage_days"], metrics["throughput_pressure"],
                    metrics["data_status"], metrics["alert_level"], rule["version"],
                    dumps(metrics["inputs"]), now,
                ),
            )
        self._emit(
            "window.calculated", f"{wid}#v{version}",
            {
                "window_id": wid, "version": version, "reason": reason,
                "coverage_days": metrics["coverage_days"],
                "throughput_pressure": metrics["throughput_pressure"],
                "data_status": metrics["data_status"],
                "alert_level": metrics["alert_level"],
            },
        )
        result = {"window_id": wid, "version": version, **metrics}
        if reason != "late_correction":
            self._sync_alerts(business_line, region, category, wid, metrics)
        return result

    def _sync_alerts(
        self, business_line: str, region: str, category: str, window_id: str, metrics: dict[str, Any]
    ) -> None:
        """根据窗口结果开合告警；活动告警指纹去重，重复上报不新增事件。"""
        if metrics["data_status"] == "missing":
            self.open_alert(business_line, region, category, "data_missing", "warning",
                            window_id=window_id, actor="system")
            return
        level = metrics["alert_level"]
        substantive = LEVEL_RANK.get(level, 0) >= LEVEL_RANK["warning"]
        if metrics["data_status"] == "late" and not substantive:
            self.open_alert(business_line, region, category, "data_late", "info",
                            window_id=window_id, actor="system")
        if substantive:
            kind = "low_coverage" if metrics["coverage_days"] is not None else "high_pressure"
            self.open_alert(business_line, region, category, kind, level,
                            window_id=window_id, actor="system")

    def _correct_windows_for(
        self, business_line: str, region: str, category: str, occurred: datetime
    ) -> list[dict[str, Any]]:
        """迟到数据到达：重算覆盖其业务时间的窗口，并校正已终结的告警与处置单。"""
        windows = self.store.query(
            "SELECT DISTINCT window_id, window_start, window_end FROM windows"
            " WHERE business_line=? AND region=? AND category=?"
            " AND window_start <= ? AND window_end > ?",
            (business_line, region, category, format_value(occurred), format_value(occurred)),
        )
        corrections = []
        for window in windows:
            new = self.compute_window(
                business_line, region, category, window["window_start"], window["window_end"],
                reason="late_correction",
            )
            result = self._apply_corrections(window["window_id"], new)
            # 修正后再同步：已改判的活动告警按指纹去重；全新异常或误判终结后补开活动告警
            self._sync_alerts(business_line, region, category, window["window_id"], new)
            corrections.append(result)
        return corrections

    def _apply_corrections(self, window_id: str, new_metrics: dict[str, Any]) -> dict[str, Any]:
        """迟到数据校正窗口关联告警：原判断保留，修正另记。

        - 已 resolved/suppressed：置 corrected（终结），处置单同步追加修正。
        - 仍活动（如 data_missing 待查）：升级为真实告警并保持可处置，不重复开告警。
        """
        corrected = []
        alerts = self.store.query("SELECT * FROM alerts WHERE window_id=?", (window_id,))
        new_level = new_metrics["alert_level"]
        for alert in alerts:
            if alert["state"] == "corrected":
                continue
            now = format_value(self.clock.now())
            correction = {
                "previous_level": alert["level"], "corrected_level": new_level,
                "window_id": window_id, "window_version": new_metrics["version"],
                "coverage_days": new_metrics["coverage_days"],
                "cause": "late_data",
            }
            if alert["state"] in ("resolved", "suppressed"):
                last = self.store.query_one(
                    "SELECT payload FROM alert_events WHERE alert_id=? AND event_type IN"
                    " ('alert.resolved','alert.suppressed') ORDER BY id DESC LIMIT 1",
                    (alert["alert_id"],),
                )
                old_level = (loads(last["payload"]) or {}).get("alert_level") if last else None
                if old_level == new_level:
                    continue
                correction["previous_level"] = old_level
                with self.store.transaction():
                    self.store.execute(
                        "INSERT INTO alert_events(alert_id, event_type, actor, reason, at, payload)"
                        " VALUES (?,?,?,?,?,?)",
                        (alert["alert_id"], "alert.corrected", "system", "迟到数据校正", now,
                         dumps(correction)),
                    )
                    self.store.execute(
                        "UPDATE alerts SET state='corrected', level=?, updated_at=? WHERE alert_id=?",
                        (new_level, now, alert["alert_id"]),
                    )
                self._emit("alert.corrected", alert["alert_id"], correction, actor="system")
                incident = self.store.query_one(
                    "SELECT * FROM incidents WHERE alert_id=?", (alert["alert_id"],)
                )
                if incident is not None:
                    self._correct_incident(incident, correction)
                corrected.append({"alert_id": alert["alert_id"], **correction})
            elif alert["kind"] in ("data_missing", "data_late") and LEVEL_RANK.get(
                new_level, 0
            ) > LEVEL_RANK.get(alert["level"], 0):
                # 待查的数据告警被迟到数据证实为真实异常：升级但保持活动、可派单
                new_state = "escalated" if new_level == "critical" else "alerted"
                new_kind = "low_coverage" if new_metrics["coverage_days"] is not None else "high_pressure"
                new_fingerprint = f"{alert['business_line']}|{alert['region']}|{alert['category']}|{new_kind}"
                with self.store.transaction():
                    self.store.execute(
                        "INSERT INTO alert_events(alert_id, event_type, actor, reason, at, payload)"
                        " VALUES (?,?,?,?,?,?)",
                        (alert["alert_id"], "alert.corrected", "system",
                         "迟到数据证实为真实供应异常", now, dumps(correction)),
                    )
                    self.store.execute(
                        "UPDATE alerts SET level=?, kind=?, fingerprint=?, state=?, updated_at=?"
                        " WHERE alert_id=?",
                        (new_level, new_kind, new_fingerprint, new_state, now, alert["alert_id"]),
                    )
                self._emit("alert.corrected", alert["alert_id"], correction, actor="system")
                corrected.append({"alert_id": alert["alert_id"], **correction})
        return {"window_id": window_id, "corrected": corrected}

    def _correct_incident(self, incident: dict[str, Any], correction: dict[str, Any]) -> None:
        """处置单追加修正：原判断不动，修正作为新链节与处置单修订。"""
        now = format_value(self.clock.now())
        original = self.store.query_one(
            "SELECT judgment FROM incident_events WHERE incident_id=? AND judgment IS NOT NULL"
            " ORDER BY id DESC LIMIT 1",
            (incident["incident_id"],),
        )
        with self.store.transaction():
            self.store.execute(
                "INSERT INTO incident_events(incident_id, event_type, actor, judgment, reason, at)"
                " VALUES (?,?,?,?,?,?)",
                (incident["incident_id"], "incident.corrected", "system",
                 original["judgment"] if original else None, "迟到数据校正，原判断保留", now),
            )
            self.store.execute(
                "UPDATE incidents SET state='corrected', updated_at=? WHERE incident_id=?",
                (now, incident["incident_id"]),
            )
        dispatch = self.store.query_one(
            "SELECT * FROM dispatch_orders WHERE incident_id=? ORDER BY issued_at DESC LIMIT 1",
            (incident["incident_id"],),
        )
        if dispatch is not None:
            with self.store.transaction():
                self.store.execute(
                    "INSERT INTO dispatch_amendments(dispatch_id, content, reason, at)"
                    " VALUES (?,?,?,?)",
                    (dispatch["dispatch_id"], dumps(correction), "迟到数据校正", now),
                )
        self._emit("incident.corrected", incident["incident_id"], correction, actor="system")

    # ------------------------------------------------------------------ 告警
    def open_alert(
        self,
        business_line: str,
        region: str,
        category: str,
        kind: str,
        level: str,
        window_id: str | None = None,
        actor: str = "system",
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        """开告警。同指纹活动告警去重：重复上报不新增事件。"""
        fingerprint = f"{business_line}|{region}|{category}|{kind}"
        existing = self.store.query_one(
            "SELECT * FROM alerts WHERE fingerprint=? AND state IN"
            " ('observing','alerted','assigned','escalated','suppressed')",
            (fingerprint,),
        )
        if existing is not None:
            return {"alert_id": existing["alert_id"], "created": False, "state": existing["state"]}
        if idempotency_key is not None:
            row = self.store.query_one(
                "SELECT event_id FROM events WHERE idempotency_key=?", (idempotency_key,)
            )
            if row is not None:
                dup = self.store.query_one(
                    "SELECT * FROM alerts WHERE fingerprint=? ORDER BY opened_at DESC LIMIT 1",
                    (fingerprint,),
                )
                return {"alert_id": dup["alert_id"], "created": False, "state": dup["state"]}
        alert_id = f"alert-{uuid.uuid4().hex[:10]}"
        state = "alerted" if LEVEL_RANK.get(level, 0) >= LEVEL_RANK["warning"] else "observing"
        now = format_value(self.clock.now())
        with self.store.transaction():
            self.store.execute(
                "INSERT INTO alerts VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (alert_id, fingerprint, business_line, region, category, window_id,
                 kind, level, state, now, now, None),
            )
            self.store.execute(
                "INSERT INTO alert_events(alert_id, event_type, actor, at, payload)"
                " VALUES (?,?,?,?,?)",
                (alert_id, "alert.opened", actor, now,
                 dumps({"level": level, "kind": kind, "window_id": window_id})),
            )
        self._emit("alert.opened", alert_id, {"level": level, "kind": kind},
                   actor=actor, idempotency_key=idempotency_key)
        return {"alert_id": alert_id, "created": True, "state": state}

    def _alert(self, alert_id: str) -> dict[str, Any]:
        row = self.store.query_one("SELECT * FROM alerts WHERE alert_id=?", (alert_id,))
        if row is None:
            raise DomainError(f"告警不存在：{alert_id}")
        return row

    def _transition_alert(
        self, alert_id: str, event_type: str, new_state: str,
        actor: str, reviewer: str, reason: str, payload: dict[str, Any] | None = None,
    ) -> None:
        now = format_value(self.clock.now())
        with self.store.transaction():
            self.store.execute(
                "INSERT INTO alert_events(alert_id, event_type, actor, reviewer, reason, at, payload)"
                " VALUES (?,?,?,?,?,?,?)",
                (alert_id, event_type, actor, reviewer, reason, now, dumps(payload or {})),
            )
            self.store.execute(
                "UPDATE alerts SET state=?, updated_at=? WHERE alert_id=?",
                (new_state, now, alert_id),
            )
        self._emit(event_type, alert_id, {"reason": reason, **(payload or {})}, actor=actor)

    def merge_alerts(self, source_id: str, target_id: str, actor: str, reviewer: str, reason: str) -> None:
        self._dual_review("merge", actor, reviewer)
        source, target = self._alert(source_id), self._alert(target_id)
        if source["state"] not in ACTIVE_ALERT_STATES or target["state"] not in ACTIVE_ALERT_STATES:
            raise DomainError("只能合并活动状态的告警")
        now = format_value(self.clock.now())
        with self.store.transaction():
            self.store.execute(
                "UPDATE alerts SET state='resolved', merged_into=?, updated_at=? WHERE alert_id=?",
                (target_id, now, source_id),
            )
            self.store.execute(
                "INSERT INTO alert_events(alert_id, event_type, actor, reviewer, reason, at, payload)"
                " VALUES (?,?,?,?,?,?,?)",
                (source_id, "alert.merged", actor, reviewer, reason, now,
                 dumps({"merged_into": target_id})),
            )
            self.store.execute(
                "INSERT INTO alert_events(alert_id, event_type, actor, reviewer, reason, at, payload)"
                " VALUES (?,?,?,?,?,?,?)",
                (target_id, "alert.merged", actor, reviewer, reason, now,
                 dumps({"absorbed": source_id})),
            )
        self._emit("alert.merged", source_id, {"merged_into": target_id, "reason": reason}, actor=actor)

    def escalate_alert(self, alert_id: str, actor: str, reviewer: str, reason: str) -> None:
        self._dual_review("escalate", actor, reviewer)
        alert = self._alert(alert_id)
        if alert["state"] not in ACTIVE_ALERT_STATES:
            raise DomainError("只能升级活动状态的告警")
        new_level = "critical" if alert["level"] in ("warning", "info") else alert["level"]
        with self.store.transaction():
            self.store.execute("UPDATE alerts SET level=? WHERE alert_id=?", (new_level, alert_id))
        self._transition_alert(alert_id, "alert.escalated", "escalated", actor, reviewer, reason,
                               {"new_level": new_level})
        incident = self.store.query_one("SELECT * FROM incidents WHERE alert_id=?", (alert_id,))
        if incident is not None and incident["state"] in ("assigned", "escalated"):
            with self.store.transaction():
                self.store.execute(
                    "UPDATE incidents SET state='escalated', updated_at=? WHERE incident_id=?",
                    (format_value(self.clock.now()), incident["incident_id"]),
                )

    def assign_alert(self, alert_id: str, owner: str, actor: str, reviewer: str, reason: str) -> dict[str, Any]:
        """派单：告警转处置单，责任人唯一；自动开具处置单（幂等）。"""
        self._dual_review("assign", actor, reviewer)
        self._operator(owner)
        alert = self._alert(alert_id)
        if alert["state"] not in ("observing", "alerted", "escalated"):
            raise DomainError(f"当前状态不可派单：{alert['state']}")
        if self.store.query_one(
            "SELECT 1 AS x FROM incidents WHERE alert_id=?", (alert_id,)
        ):
            raise DomainError("该告警已开过处置单，不得重复派单")
        incident_id = f"incident-{alert_id}"
        now = format_value(self.clock.now())
        judgment = self._current_judgment(alert)
        with self.store.transaction():
            self.store.execute(
                "INSERT INTO incidents VALUES (?,?,?,?,?,?)",
                (incident_id, alert_id, owner, "assigned", now, now),
            )
            self.store.execute(
                "INSERT INTO incident_events(incident_id, event_type, actor, reviewer, to_owner,"
                " judgment, reason, at) VALUES (?,?,?,?,?,?,?,?)",
                (incident_id, "incident.assigned", actor, reviewer, owner, dumps(judgment), reason, now),
            )
        self._transition_alert(alert_id, "alert.assigned", "assigned", actor, reviewer, reason,
                               {"owner": owner, "incident_id": incident_id})
        self._emit("incident.assigned", incident_id, {"owner": owner, "alert_id": alert_id}, actor=actor)
        dispatch = self.dispatch_for_incident(incident_id, reason="assign")
        return {"incident_id": incident_id, "owner": owner, "dispatch": dispatch}

    def transfer_incident(self, incident_id: str, to_owner: str, actor: str, reviewer: str, reason: str) -> None:
        """转交：责任链追加新链节，任一时刻责任人唯一。"""
        self._dual_review("transfer", actor, reviewer)
        self._operator(to_owner)
        incident = self.store.query_one("SELECT * FROM incidents WHERE incident_id=?", (incident_id,))
        if incident is None:
            raise DomainError(f"处置单不存在：{incident_id}")
        if incident["state"] not in ("assigned", "escalated", "corrected"):
            raise DomainError(f"当前状态不可转交：{incident['state']}")
        if incident["owner"] == to_owner:
            raise DomainError("转交对象与现任责任人相同")
        now = format_value(self.clock.now())
        with self.store.transaction():
            self.store.execute(
                "INSERT INTO incident_events(incident_id, event_type, actor, reviewer, from_owner,"
                " to_owner, reason, at) VALUES (?,?,?,?,?,?,?,?)",
                (incident_id, "incident.transferred", actor, reviewer, incident["owner"],
                 to_owner, reason, now),
            )
            self.store.execute(
                "UPDATE incidents SET owner=?, updated_at=? WHERE incident_id=?",
                (to_owner, now, incident_id),
            )
        self._emit("incident.transferred", incident_id,
                   {"from": incident["owner"], "to": to_owner, "reason": reason}, actor=actor)
        self._transition_alert(incident["alert_id"], "alert.transferred", "assigned",
                               actor, reviewer, reason, {"owner": to_owner})

    def suppress_alert(self, alert_id: str, actor: str, reviewer: str, reason: str) -> None:
        self._dual_review("suppress", actor, reviewer)
        if not reason:
            raise DomainError("抑制必须填写理由")
        alert = self._alert(alert_id)
        if alert["state"] not in ACTIVE_ALERT_STATES:
            raise DomainError("只能抑制活动状态的告警")
        self._transition_alert(alert_id, "alert.suppressed", "suppressed", actor, reviewer, reason,
                               {"alert_level": alert["level"]})

    def resolve_alert(self, alert_id: str, actor: str, reviewer: str, reason: str) -> None:
        """解除告警：冻结当时判断快照，供事后按入库时间回放还原依据。"""
        self._dual_review("resolve", actor, reviewer)
        alert = self._alert(alert_id)
        if alert["state"] not in ACTIVE_ALERT_STATES:
            raise DomainError("只能解除活动状态的告警")
        judgment = self._current_judgment(alert)
        self._transition_alert(alert_id, "alert.resolved", "resolved", actor, reviewer, reason,
                               {"judgment": judgment, "alert_level": alert["level"]})
        incident = self.store.query_one("SELECT * FROM incidents WHERE alert_id=?", (alert_id,))
        if incident is not None and incident["state"] in ("assigned", "escalated"):
            now = format_value(self.clock.now())
            with self.store.transaction():
                self.store.execute(
                    "INSERT INTO incident_events(incident_id, event_type, actor, reviewer,"
                    " judgment, reason, at) VALUES (?,?,?,?,?,?,?)",
                    (incident["incident_id"], "incident.resolved", actor, reviewer,
                     dumps(judgment), reason, now),
                )
                self.store.execute(
                    "UPDATE incidents SET state='resolved', updated_at=? WHERE incident_id=?",
                    (now, incident["incident_id"]),
                )
            self._emit("incident.resolved", incident["incident_id"],
                       {"judgment": judgment, "reason": reason}, actor=actor)

    def _current_judgment(self, alert: dict[str, Any]) -> dict[str, Any]:
        """冻结告警当前依据的窗口指标快照。"""
        judgment: dict[str, Any] = {"alert_level": alert["level"], "kind": alert["kind"]}
        if alert["window_id"]:
            window = self.store.query_one(
                "SELECT * FROM windows WHERE window_id=? ORDER BY version DESC LIMIT 1",
                (alert["window_id"],),
            )
            if window is not None:
                judgment.update({
                    "window_id": window["window_id"], "window_version": window["version"],
                    "coverage_days": window["coverage_days"],
                    "throughput_pressure": window["throughput_pressure"],
                    "data_status": window["data_status"],
                })
        return judgment

    # ---------------------------------------------------------------- 处置单
    def dispatch_for_incident(self, incident_id: str, reason: str) -> dict[str, Any]:
        """开处置单。同一处置单仅一张有效单，重复触发不重开（修正走 amendments）。"""
        incident = self.store.query_one("SELECT * FROM incidents WHERE incident_id=?", (incident_id,))
        if incident is None:
            raise DomainError(f"处置单不存在：{incident_id}")
        dedup_key = incident_id
        existing = self.store.query_one(
            "SELECT * FROM dispatch_orders WHERE dedup_key=?", (dedup_key,)
        )
        if existing is not None:
            return {"dispatch_id": existing["dispatch_id"], "created": False}
        alert = self._alert(incident["alert_id"])
        dispatch_id = f"dispatch-{uuid.uuid4().hex[:10]}"
        content = {
            "incident_id": incident_id, "alert_id": alert["alert_id"],
            "owner": incident["owner"], "level": alert["level"], "kind": alert["kind"],
            "judgment": self._current_judgment(alert),
        }
        now = format_value(self.clock.now())
        with self.store.transaction():
            self.store.execute(
                "INSERT INTO dispatch_orders VALUES (?,?,?,?,?,?)",
                (dispatch_id, incident_id, dedup_key, dumps(content), "issued", now),
            )
        self._emit("dispatch.sent", dispatch_id, content, actor="system")
        return {"dispatch_id": dispatch_id, "created": True}

    def responsibility_chain(self, incident_id: str) -> list[dict[str, Any]]:
        """唯一责任链：按时间排列的责任人链节，班次交接不改变它。"""
        rows = self.store.query(
            "SELECT * FROM incident_events WHERE incident_id=? AND event_type IN"
            " ('incident.assigned','incident.transferred') ORDER BY id",
            (incident_id,),
        )
        chain = []
        for row in rows:
            chain.append({
                "owner": row["to_owner"], "from_owner": row["from_owner"],
                "at": row["at"], "actor": row["actor"], "reviewer": row["reviewer"],
                "reason": row["reason"],
            })
        if not chain:
            raise DomainError(f"处置单不存在或无责任链：{incident_id}")
        return chain

    # ------------------------------------------------------------------ 班次
    def close_shift(self, shift_id: str) -> None:
        with self.store.transaction():
            self.store.execute(
                "UPDATE shifts SET closed_at=? WHERE shift_id=?",
                (format_value(self.clock.now()), shift_id),
            )

    # ------------------------------------------------------------------ 日报
    def publish_daily_report(self, report_id: str, shift_id: str | None = None,
                             as_of: str | None = None) -> dict[str, Any]:
        """按 as_of 入库截止生成日报：每个品类/区域取最新窗口的回放视图。"""
        as_of_dt = parse(as_of) if as_of else self.clock.now()
        triples = self.store.query(
            "SELECT DISTINCT business_line, region, category FROM windows"
        )
        items = []
        for t in sorted(triples, key=lambda r: (r["business_line"], r["region"], r["category"])):
            latest = self.store.query_one(
                "SELECT * FROM windows WHERE business_line=? AND region=? AND category=?"
                " ORDER BY window_start DESC, version DESC LIMIT 1",
                (t["business_line"], t["region"], t["category"]),
            )
            if latest is None:
                continue
            metrics = self._compute_metrics(
                t["business_line"], t["region"], t["category"],
                parse(latest["window_start"]), parse(latest["window_end"]), as_of_dt,
            )
            items.append({
                "business_line": t["business_line"], "region": t["region"],
                "category": t["category"], "window_start": latest["window_start"],
                "coverage_days": metrics["coverage_days"],
                "throughput_pressure": metrics["throughput_pressure"],
                "data_status": metrics["data_status"], "alert_level": metrics["alert_level"],
            })
        now = format_value(self.clock.now())
        if self.store.query_one("SELECT 1 AS x FROM reports WHERE report_id=?", (report_id,)):
            raise DomainError(f"日报已签发，不可覆盖：{report_id}")
        with self.store.transaction():
            self.store.execute(
                "INSERT INTO reports VALUES (?,?,?,?,?)",
                (report_id, shift_id, format_value(as_of_dt), now, dumps(items)),
            )
        self._emit("report.published", report_id,
                   {"shift_id": shift_id, "as_of": format_value(as_of_dt), "items": len(items)})
        return {"report_id": report_id, "as_of": format_value(as_of_dt), "items": items}

    def reconcile_report(self, report_id: str) -> list[dict[str, Any]]:
        """逐项核对：按报告 as_of 重放计算，与发布内容逐字段比对。"""
        report = self.store.query_one("SELECT * FROM reports WHERE report_id=?", (report_id,))
        if report is None:
            raise DomainError(f"日报不存在：{report_id}")
        as_of = parse(report["as_of"])
        results = []
        for item in loads(report["items"]):
            latest = self.store.query_one(
                "SELECT * FROM windows WHERE business_line=? AND region=? AND category=?"
                " AND window_start=? ORDER BY version DESC LIMIT 1",
                (item["business_line"], item["region"], item["category"], item["window_start"]),
            )
            metrics = self._compute_metrics(
                item["business_line"], item["region"], item["category"],
                parse(item["window_start"]), parse(latest["window_end"]), as_of,
            )
            diffs = {}
            for field in ("coverage_days", "throughput_pressure", "data_status", "alert_level"):
                if metrics[field] != item[field]:
                    diffs[field] = {"published": item[field], "recomputed": metrics[field]}
            results.append({
                "key": f"{item['business_line']}|{item['region']}|{item['category']}|{item['window_start']}",
                "ok": not diffs, "diffs": diffs,
            })
        return results

    # ------------------------------------------------------------------ 血缘
    def reconcile_dispatches(self) -> list[dict[str, Any]]:
        """逐项核对全部处置单：冻结判断与所引用窗口版本留档一致，且血缘可追。"""
        results = []
        for order in self.store.query("SELECT * FROM dispatch_orders ORDER BY issued_at"):
            content = loads(order["content"])
            judgment = content.get("judgment", {})
            diffs = {}
            wid = judgment.get("window_id")
            version = judgment.get("window_version")
            if wid and version:
                row = self.store.query_one(
                    "SELECT * FROM windows WHERE window_id=? AND version=?", (wid, version)
                )
                if row is None:
                    diffs["window_version"] = {"published": version, "recomputed": "缺失"}
                else:
                    for field, stored in (
                        ("coverage_days", row["coverage_days"]),
                        ("throughput_pressure", row["throughput_pressure"]),
                        ("data_status", row["data_status"]),
                    ):
                        if field in judgment and judgment[field] != stored:
                            diffs[field] = {"frozen": judgment[field], "archived": stored}
            amendments = self.store.query(
                "SELECT id FROM dispatch_amendments WHERE dispatch_id=?",
                (order["dispatch_id"],),
            )
            results.append({
                "dispatch_id": order["dispatch_id"],
                "incident_id": content.get("incident_id"),
                "owner": content.get("owner"),
                "amendments": len(amendments),
                "ok": not diffs,
                "diffs": diffs,
            })
        return results

    def window_lineage(self, window_id: str, version: int | None = None) -> dict[str, Any]:
        """任一指标可追到来源快照/事件/规则版本。"""
        if version is None:
            row = self.store.query_one(
                "SELECT * FROM windows WHERE window_id=? ORDER BY version DESC LIMIT 1", (window_id,)
            )
        else:
            row = self.store.query_one(
                "SELECT * FROM windows WHERE window_id=? AND version=?", (window_id, version)
            )
        if row is None:
            raise DomainError(f"窗口不存在：{window_id}")
        inputs = loads(row["inputs"])
        snapshots = [
            self.store.query_one("SELECT * FROM snapshots WHERE snapshot_id=?", (sid,))
            for sid in inputs.get("snapshots", [])
        ]
        events = [
            self.store.query_one("SELECT * FROM business_events WHERE biz_event_id=?", (eid,))
            for eid in inputs.get("business_events", [])
        ]
        rule_id, _, rule_version = inputs.get("rule", "@0").partition("@")
        rule = self.store.query_one(
            "SELECT * FROM rules WHERE rule_id=? AND version=?", (rule_id, int(rule_version or 0))
        )
        return {
            "window_id": window_id, "version": row["version"],
            "coverage_days": row["coverage_days"],
            "throughput_pressure": row["throughput_pressure"],
            "data_status": row["data_status"], "alert_level": row["alert_level"],
            "snapshots": [s for s in snapshots if s],
            "business_events": [e for e in events if e],
            "rule": loads(rule["content"]) if rule else None,
        }

    # ---------------------------------------------------------------- 时间线
    def timeline(self, aggregate_id: str | None = None, since: str | None = None,
                 until: str | None = None) -> list[dict[str, Any]]:
        sql = "SELECT * FROM events WHERE 1=1"
        params: list[Any] = []
        if aggregate_id:
            sql += " AND aggregate_id=?"
            params.append(aggregate_id)
        if since:
            sql += " AND occurred_at>=?"
            params.append(since)
        if until:
            sql += " AND occurred_at<=?"
            params.append(until)
        sql += " ORDER BY seq"
        rows = self.store.query(sql, tuple(params))
        for row in rows:
            row["payload"] = loads(row["payload"])
        return rows

    # -------------------------------------------------------------- 恢复补算
    def recover_windows(
        self,
        business_line: str,
        region: str,
        category: str,
        window_starts: list[str],
        window_hours: int = 1,
    ) -> dict[str, Any]:
        """系统恢复后补算遗漏窗口；派单按 dedup_key 去重，不重复派单。"""
        computed, skipped = [], []
        for start_str in window_starts:
            start = parse(start_str)
            end = start + timedelta(hours=window_hours)
            wid = self.window_id(business_line, region, category, format_value(start))
            if self.store.query_one("SELECT 1 AS x FROM windows WHERE window_id=?", (wid,)):
                skipped.append(wid)
                continue
            self.compute_window(business_line, region, category,
                                format_value(start), format_value(end), reason="recovery")
            computed.append(wid)
        dispatched, duplicates = [], []
        for incident in self.store.query(
            "SELECT * FROM incidents WHERE state IN ('assigned','escalated')"
        ):
            result = self.dispatch_for_incident(incident["incident_id"], reason="recovery")
            (dispatched if result["created"] else duplicates).append(result["dispatch_id"])
        return {"computed": computed, "skipped": skipped,
                "dispatched": dispatched, "duplicate_dispatches": duplicates}

    # -------------------------------------------------------------- 隔离推演
    def simulate(
        self,
        mutations: list[dict[str, Any]],
        windows: list[dict[str, str]],
    ) -> dict[str, Any]:
        """what-if 推演：在克隆库上施加假设（仓容/到货变化），主库不受影响。"""
        before = self.store.query_one("SELECT COUNT(*) AS n FROM events")["n"]
        sandbox = self.store.clone()
        sandbox_service = CommandService(sandbox, self.clock)
        for mutation in mutations:
            if mutation["type"] == "snapshot":
                sandbox_service.ingest_snapshot(**mutation["params"])
            elif mutation["type"] == "event":
                sandbox_service.ingest_business_event(**mutation["params"])
            else:
                raise DomainError(f"未知推演变更：{mutation['type']}")
        results = []
        for window in windows:
            metrics = sandbox_service._compute_metrics(
                window["business_line"], window["region"], window["category"],
                parse(window["window_start"]), parse(window["window_end"]),
                self.clock.now(),
            )
            results.append({**window, **metrics})
        after = self.store.query_one("SELECT COUNT(*) AS n FROM events")["n"]
        assert before == after, "推演不得污染主库"
        sandbox.close()
        return {"isolated": True, "results": results}
