"""保供指挥后端测试。"""
from __future__ import annotations

import unittest
from datetime import datetime

from app.clock import Clock, parse
from app.store import Store, loads
from app.service import CommandService, DomainError

T0 = "2026-09-23T00:00:00+08:00"


def build_service(start: str = T0, aquatic_rule: bool = True) -> CommandService:
    clock = Clock(start)
    svc = CommandService(Store(":memory:"), clock)
    svc.register_operator("op1", "夜班值班员", "operator")
    svc.register_operator("op2", "早班值班员", "operator")
    svc.register_operator("cmd1", "夜班指挥长", "commander")
    svc.register_operator("cmd2", "早班指挥长", "commander")
    if aquatic_rule:
        svc.rules.publish("thresholds:aquatic",
                          {"snapshot_max_age_seconds": 1800, "late_threshold_seconds": 1800})
    return svc


class SnapshotIngestTest(unittest.TestCase):
    def setUp(self) -> None:
        self.svc = build_service()

    def test_multi_line_snapshots_share_timeline(self) -> None:
        for line, cat in [("fruit", "apple"), ("grain", "rice"),
                          ("aquatic", "fish"), ("snack", "nuts")]:
            self.svc.ingest_snapshot(line, "east", cat, 1, 100, 20, 20, 100,
                                     "2026-09-23T00:00:00+08:00",
                                     idempotency_key=f"k-{cat}-1")
        events = self.svc.timeline()
        self.assertEqual(sum(e["event_type"] == "snapshot.received" for e in events), 4)

    def test_duplicate_report_creates_no_event(self) -> None:
        args = ("fruit", "east", "apple", 1, 100, 20, 20, 100,
                "2026-09-23T00:00:00+08:00")
        self.svc.ingest_snapshot(*args, idempotency_key="dup")
        before = self.svc.store.query_one("SELECT COUNT(*) AS n FROM events")["n"]
        result = self.svc.ingest_snapshot(*args, idempotency_key="dup")
        after = self.svc.store.query_one("SELECT COUNT(*) AS n FROM events")["n"]
        self.assertTrue(result["duplicate"])
        self.assertEqual(before, after)

    def test_same_version_different_payload_rejected(self) -> None:
        self.svc.ingest_snapshot("fruit", "east", "apple", 1, 100, 20, 20, 100,
                                 "2026-09-23T00:00:00+08:00", idempotency_key="a")
        with self.assertRaises(DomainError):
            self.svc.ingest_snapshot("fruit", "east", "apple", 1, 90, 20, 20, 100,
                                     "2026-09-23T00:00:00+08:00", idempotency_key="b")

    def test_late_snapshot_flagged_and_on_time_not(self) -> None:
        on_time = self.svc.ingest_snapshot(
            "fruit", "east", "apple", 1, 100, 20, 20, 100,
            "2026-09-23T00:00:00+08:00", idempotency_key="on")
        self.assertFalse(on_time["late"])
        self.svc.clock.freeze("2026-09-23T05:00:00+08:00")
        late = self.svc.ingest_snapshot(
            "fruit", "east", "apple", 2, 100, 20, 20, 100,
            "2026-09-23T00:30:00+08:00", idempotency_key="late")
        self.assertTrue(late["late"])


class WindowMetricsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.svc = build_service()

    def _snapshot(self, stock: float, throughput: float, at: str, version: int = 1,
                  capacity: float = 100.0) -> None:
        self.svc.ingest_snapshot("aquatic", "east", "fish", version, stock, 20,
                                 throughput, capacity, at, idempotency_key=f"s{version}")

    def test_coverage_and_pressure_levels(self) -> None:
        self._snapshot(100, 20, "2026-09-23T00:00:00+08:00", 1)
        window = self.svc.compute_window(
            "aquatic", "east", "fish", "2026-09-23T00:00:00+08:00",
            "2026-09-23T01:00:00+08:00")
        self.assertEqual(window["coverage_days"], 5.0)
        self.assertEqual(window["alert_level"], "none")

        self._snapshot(50, 20, "2026-09-23T01:00:00+08:00", 2)
        window = self.svc.compute_window(
            "aquatic", "east", "fish", "2026-09-23T01:00:00+08:00",
            "2026-09-23T02:00:00+08:00")
        self.assertEqual(window["alert_level"], "warning")

        self._snapshot(10, 20, "2026-09-23T02:00:00+08:00", 3)
        window = self.svc.compute_window(
            "aquatic", "east", "fish", "2026-09-23T02:00:00+08:00",
            "2026-09-23T03:00:00+08:00")
        self.assertEqual(window["alert_level"], "critical")

    def test_high_throughput_pressure_escalates_level(self) -> None:
        self._snapshot(100, 120, "2026-09-23T00:00:00+08:00", 1, capacity=100)
        window = self.svc.compute_window(
            "aquatic", "east", "fish", "2026-09-23T00:00:00+08:00",
            "2026-09-23T01:00:00+08:00")
        self.assertGreaterEqual(window["throughput_pressure"], 1.0)
        self.assertEqual(window["alert_level"], "critical")

    def test_missing_distinguished_from_late_and_anomaly(self) -> None:
        self._snapshot(100, 20, "2026-09-23T00:00:00+08:00", 1)
        ok = self.svc.compute_window(
            "aquatic", "east", "fish", "2026-09-23T00:00:00+08:00",
            "2026-09-23T01:00:00+08:00")
        self.assertEqual(ok["data_status"], "ok")
        # 快照超过最大年龄：数据缺失（此时还不能断言供应异常）
        self.svc.clock.freeze("2026-09-23T03:30:00+08:00")
        missing = self.svc.compute_window(
            "aquatic", "east", "fish", "2026-09-23T03:00:00+08:00",
            "2026-09-23T04:00:00+08:00")
        self.assertEqual(missing["data_status"], "missing")
        self.assertEqual(missing["coverage_days"], None)
        # 迟到数据到达并重算：区分出真实供应异常
        self.svc.clock.freeze("2026-09-23T08:31:00+08:00")
        self.svc.ingest_snapshot("aquatic", "east", "fish", 2, 5, 20, 20, 100,
                                 "2026-09-23T03:10:00+08:00", idempotency_key="s2")
        recomputed = self.svc.store.query_one(
            "SELECT * FROM windows WHERE window_id=? ORDER BY version DESC",
            ("aquatic|east|fish|2026-09-23T03:00:00+08:00",))
        self.assertEqual(recomputed["data_status"], "late")
        self.assertEqual(recomputed["alert_level"], "critical")

    def test_window_versions_are_append_only(self) -> None:
        self._snapshot(100, 20, "2026-09-23T00:00:00+08:00", 1)
        wid = ("aquatic", "east", "fish", "2026-09-23T00:00:00+08:00",
               "2026-09-23T01:00:00+08:00")
        self.svc.compute_window(*wid)
        self.svc.compute_window(*wid, reason="late_correction")
        versions = self.svc.store.query(
            "SELECT version FROM windows WHERE window_id=? ORDER BY version",
            ("aquatic|east|fish|2026-09-23T00:00:00+08:00",))
        self.assertEqual([v["version"] for v in versions], [1, 2])


class AlertWorkflowTest(unittest.TestCase):
    def setUp(self) -> None:
        self.svc = build_service()

    def _critical_alert(self) -> str:
        self.svc.ingest_snapshot("aquatic", "east", "fish", 1, 5, 20, 20, 100,
                                 "2026-09-23T00:00:00+08:00", idempotency_key="s1")
        self.svc.compute_window("aquatic", "east", "fish",
                                "2026-09-23T00:00:00+08:00", "2026-09-23T01:00:00+08:00")
        return self.svc.store.query_one(
            "SELECT alert_id FROM alerts WHERE kind='low_coverage'")["alert_id"]

    def test_duplicate_alert_deduped(self) -> None:
        first = self.svc.open_alert("fruit", "east", "apple", "low_coverage", "warning")
        second = self.svc.open_alert("fruit", "east", "apple", "low_coverage", "critical")
        self.assertTrue(first["created"])
        self.assertFalse(second["created"])
        self.assertEqual(first["alert_id"], second["alert_id"])

    def test_dual_review_constraints(self) -> None:
        alert_id = self._critical_alert()
        with self.assertRaises(DomainError):  # 自审
            self.svc.suppress_alert(alert_id, "cmd1", "cmd1", "x")
        with self.assertRaises(DomainError):  # 复核人非指挥长
            self.svc.suppress_alert(alert_id, "op1", "op2", "x")
        self.svc.suppress_alert(alert_id, "op1", "cmd1", "确认抑制")

    def test_suppress_requires_reason(self) -> None:
        alert_id = self._critical_alert()
        with self.assertRaises(DomainError):
            self.svc.suppress_alert(alert_id, "op1", "cmd1", "")

    def test_merge_alerts(self) -> None:
        a = self.svc.open_alert("fruit", "east", "apple", "low_coverage", "warning")["alert_id"]
        b = self.svc.open_alert("fruit", "east", "pear", "low_coverage", "warning")["alert_id"]
        self.svc.merge_alerts(a, b, "op1", "cmd1", "同类合并")
        source = self.svc.store.query_one("SELECT * FROM alerts WHERE alert_id=?", (a,))
        self.assertEqual(source["state"], "resolved")
        self.assertEqual(source["merged_into"], b)

    def test_escalation_changes_level(self) -> None:
        alert_id = self.svc.open_alert("fruit", "east", "apple", "low_coverage",
                                       "warning")["alert_id"]
        self.svc.escalate_alert(alert_id, "op1", "cmd1", "形势恶化")
        row = self.svc.store.query_one("SELECT * FROM alerts WHERE alert_id=?", (alert_id,))
        self.assertEqual(row["level"], "critical")
        self.assertEqual(row["state"], "escalated")

    def test_resolve_freezes_judgment_replayable(self) -> None:
        alert_id = self._critical_alert()
        self.svc.resolve_alert(alert_id, "op1", "cmd1", "称已补足")
        event = self.svc.store.query_one(
            "SELECT payload FROM alert_events WHERE alert_id=? AND event_type='alert.resolved'",
            (alert_id,))
        judgment = loads(event["payload"])["judgment"]
        self.assertEqual(judgment["alert_level"], "critical")
        self.assertIsNotNone(judgment["window_version"])


class IncidentResponsibilityTest(unittest.TestCase):
    def setUp(self) -> None:
        self.svc = build_service()
        self.svc.open_shift("night", "op1")
        self.svc.ingest_snapshot("aquatic", "east", "fish", 1, 5, 20, 20, 100,
                                 "2026-09-23T00:00:00+08:00", idempotency_key="s1")
        self.svc.compute_window("aquatic", "east", "fish",
                                "2026-09-23T00:00:00+08:00", "2026-09-23T01:00:00+08:00")
        alert_id = self.svc.store.query_one(
            "SELECT alert_id FROM alerts WHERE kind='low_coverage'")["alert_id"]
        self.result = self.svc.assign_alert(alert_id, "op1", "op1", "cmd1", "派单")
        self.incident_id = self.result["incident_id"]

    def test_unique_owner_and_chain(self) -> None:
        chain = self.svc.responsibility_chain(self.incident_id)
        self.assertEqual([c["owner"] for c in chain], ["op1"])
        self.svc.transfer_incident(self.incident_id, "op2", "op1", "cmd2", "转交早班")
        chain = self.svc.responsibility_chain(self.incident_id)
        self.assertEqual([c["owner"] for c in chain], ["op1", "op2"])
        incident = self.svc.store.query_one(
            "SELECT * FROM incidents WHERE incident_id=?", (self.incident_id,))
        self.assertEqual(incident["owner"], "op2")  # 唯一现任责任人

    def test_handover_does_not_change_chain(self) -> None:
        self.svc.handover_shift("night", "op1", "op2", "交班")
        self.assertEqual(
            self.svc.responsibility_chain(self.incident_id)[-1]["owner"], "op1")

    def test_transfer_requires_dual_review(self) -> None:
        with self.assertRaises(DomainError):
            self.svc.transfer_incident(self.incident_id, "op2", "op1", "op1", "x")

    def test_dispatch_not_duplicated(self) -> None:
        again = self.svc.dispatch_for_incident(self.incident_id, "assign")
        self.assertFalse(again["created"])


class LateCorrectionTest(unittest.TestCase):
    def test_resolved_alert_and_dispatch_keep_original_then_corrected(self) -> None:
        svc = build_service()
        svc.ingest_snapshot("aquatic", "east", "fish", 1, 100, 20, 20, 100,
                            "2026-09-23T02:00:00+08:00", idempotency_key="v1")
        svc.clock.freeze("2026-09-23T04:00:00+08:00")
        svc.compute_window("aquatic", "east", "fish",
                           "2026-09-23T03:00:00+08:00", "2026-09-23T04:00:00+08:00")
        missing = svc.store.query_one(
            "SELECT alert_id FROM alerts WHERE kind='data_missing'")["alert_id"]
        svc.assign_alert(missing, "op1", "op1", "cmd1", "排查")
        svc.resolve_alert(missing, "op1", "cmd1", "判定为采集缺失")
        incident_id = f"incident-{missing}"

        # 迟到快照：真实短缺
        svc.clock.freeze("2026-09-23T08:31:00+08:00")
        svc.ingest_snapshot("aquatic", "east", "fish", 2, 8, 20, 20, 100,
                            "2026-09-23T03:10:00+08:00", idempotency_key="v2")

        alert = svc.store.query_one("SELECT * FROM alerts WHERE alert_id=?", (missing,))
        self.assertEqual(alert["state"], "corrected")
        incident = svc.store.query_one(
            "SELECT * FROM incidents WHERE incident_id=?", (incident_id,))
        self.assertEqual(incident["state"], "corrected")
        # 原 resolved 判断保留，corrected 追加在后
        types = [r["event_type"] for r in svc.store.query(
            "SELECT event_type FROM incident_events WHERE incident_id=? ORDER BY id",
            (incident_id,))]
        self.assertIn("incident.resolved", types)
        self.assertEqual(types[-1], "incident.corrected")
        # 处置单追加修订而非重开
        amendments = svc.store.query(
            "SELECT * FROM dispatch_amendments")
        self.assertEqual(len(amendments), 1)
        self.assertEqual(loads(amendments[0]["content"])["cause"], "late_data")
        orders = svc.store.query("SELECT * FROM dispatch_orders")
        self.assertEqual(len(orders), 1)
        # 处置单逐项核对：冻结判断与窗口留档一致，修正计入修订
        reconciliation = svc.reconcile_dispatches()
        self.assertEqual(len(reconciliation), 1)
        self.assertTrue(reconciliation[0]["ok"])
        self.assertEqual(reconciliation[0]["amendments"], 1)

    def test_active_data_alert_escalated_by_late_data_without_duplicate(self) -> None:
        """未解除的数据缺失告警，被迟到数据证实为真实异常时升级且不重复开告警。"""
        svc = build_service()
        svc.ingest_snapshot("aquatic", "east", "fish", 1, 100, 20, 20, 100,
                            "2026-09-23T02:00:00+08:00", idempotency_key="v1")
        svc.clock.freeze("2026-09-23T04:00:00+08:00")
        svc.compute_window("aquatic", "east", "fish",
                           "2026-09-23T03:00:00+08:00", "2026-09-23T04:00:00+08:00")
        alert_before = svc.store.query_one("SELECT * FROM alerts WHERE kind='data_missing'")
        # 迟到数据证实短缺（告警仍活动，未解除）
        svc.clock.freeze("2026-09-23T08:31:00+08:00")
        svc.ingest_snapshot("aquatic", "east", "fish", 2, 8, 20, 20, 100,
                            "2026-09-23T03:10:00+08:00", idempotency_key="v2")
        alert_after = svc.store.query_one(
            "SELECT * FROM alerts WHERE alert_id=?", (alert_before["alert_id"],))
        self.assertEqual(alert_after["kind"], "low_coverage")
        self.assertEqual(alert_after["level"], "critical")
        self.assertIn(alert_after["state"], ("alerted", "escalated"))
        # 没有新增重复告警
        self.assertEqual(
            svc.store.query_one("SELECT COUNT(*) AS n FROM alerts")["n"], 1)
        # 升级后仍可派单
        assigned = svc.assign_alert(alert_after["alert_id"], "op2", "op2", "cmd2", "确认短缺")
        self.assertEqual(assigned["incident_id"], f"incident-{alert_after['alert_id']}")


class RecoveryTest(unittest.TestCase):
    def test_recovery_computes_missing_without_duplicate_dispatch(self) -> None:
        svc = build_service()
        # 03:00 窗口已算且已派单
        svc.ingest_snapshot("aquatic", "east", "fish", 1, 5, 20, 20, 100,
                            "2026-09-23T03:00:00+08:00", idempotency_key="v1")
        svc.clock.freeze("2026-09-23T04:00:00+08:00")
        svc.compute_window("aquatic", "east", "fish",
                           "2026-09-23T03:00:00+08:00", "2026-09-23T04:00:00+08:00")
        alert = svc.store.query_one(
            "SELECT alert_id FROM alerts WHERE kind='low_coverage'")["alert_id"]
        svc.assign_alert(alert, "op1", "op1", "cmd1", "派单")
        # 积压快照到达
        for h, version, stock in [(4, 2, 6), (5, 3, 5)]:
            svc.clock.freeze(f"2026-09-23T08:0{h}:00+08:00")
            svc.ingest_snapshot(
                "aquatic", "east", "fish", version, stock, 20, 20, 100,
                f"2026-09-23T0{h}:00:00+08:00", idempotency_key=f"v{version}")
        # 系统恢复，补算 03-06 窗口
        svc.clock.freeze("2026-09-23T09:00:00+08:00")
        result = svc.recover_windows("aquatic", "east", "fish", [
            f"2026-09-23T0{h}:00:00+08:00" for h in range(3, 6)])
        self.assertEqual(len(result["skipped"]), 1)
        self.assertEqual(len(result["computed"]), 2)
        # 同一处置单不重复开具
        orders = svc.store.query("SELECT COUNT(*) AS n FROM dispatch_orders")
        self.assertEqual(orders[0]["n"], 1)


class LineageAndSimulationTest(unittest.TestCase):
    def test_lineage_traces_snapshots_events_and_rule(self) -> None:
        svc = build_service()
        svc.ingest_snapshot("aquatic", "east", "fish", 1, 5, 20, 20, 100,
                            "2026-09-23T00:00:00+08:00", idempotency_key="s1")
        svc.ingest_business_event(
            "evt-arrival", "arrival", "aquatic", "east", "fish",
            "2026-09-23T00:30:00+08:00", {"qty": 2}, idempotency_key="b1")
        svc.compute_window("aquatic", "east", "fish",
                           "2026-09-23T00:00:00+08:00", "2026-09-23T01:00:00+08:00")
        lineage = svc.window_lineage("aquatic|east|fish|2026-09-23T00:00:00+08:00")
        self.assertEqual({s["snapshot_id"] for s in lineage["snapshots"]},
                         {"aquatic|east|fish|v1"})
        self.assertEqual({e["biz_event_id"] for e in lineage["business_events"]},
                         {"evt-arrival"})
        self.assertIn("safety_coverage_days", lineage["rule"])

    def test_simulation_is_isolated(self) -> None:
        svc = build_service()
        svc.ingest_snapshot("aquatic", "east", "fish", 1, 5, 20, 20, 100,
                            "2026-09-23T04:00:00+08:00", idempotency_key="s1")
        snapshot_count = svc.store.query_one("SELECT COUNT(*) AS n FROM snapshots")["n"]
        result = svc.simulate(
            [{"type": "snapshot", "params": dict(
                business_line="aquatic", region="east", category="fish", version=9,
                stock_qty=500, safety_stock=20, daily_throughput=20, capacity=300,
                occurred_at="2026-09-23T04:30:00+08:00", idempotency_key="sim")}],
            [{"business_line": "aquatic", "region": "east", "category": "fish",
              "window_start": "2026-09-23T04:00:00+08:00",
              "window_end": "2026-09-23T05:00:00+08:00"}])
        self.assertTrue(result["isolated"])
        self.assertEqual(result["results"][0]["coverage_days"], 25.0)
        self.assertEqual(result["results"][0]["alert_level"], "none")
        self.assertEqual(
            svc.store.query_one("SELECT COUNT(*) AS n FROM snapshots")["n"],
            snapshot_count)


class ReportTest(unittest.TestCase):
    def test_report_published_then_reconciled_item_by_item(self) -> None:
        svc = build_service()
        svc.ingest_snapshot("fruit", "east", "apple", 1, 100, 20, 20, 100,
                            "2026-09-23T07:00:00+08:00", idempotency_key="f1")
        svc.compute_window("fruit", "east", "apple",
                           "2026-09-23T07:00:00+08:00", "2026-09-23T08:00:00+08:00")
        svc.clock.freeze("2026-09-23T08:00:00+08:00")
        report = svc.publish_daily_report("r1", "night")
        self.assertEqual(len(report["items"]), 1)
        with self.assertRaises(DomainError):
            svc.publish_daily_report("r1", "night")
        reconciliation = svc.reconcile_report("r1")
        self.assertTrue(all(item["ok"] for item in reconciliation))

    def test_report_as_of_replays_historical_view(self) -> None:
        """08:00 发布日报时看不到 08:31 才入库的迟到数据。"""
        svc = build_service()
        svc.clock.freeze("2026-09-23T07:30:00+08:00")
        svc.ingest_snapshot("aquatic", "east", "fish", 1, 100, 20, 20, 100,
                            "2026-09-23T07:00:00+08:00", idempotency_key="v1")
        svc.compute_window("aquatic", "east", "fish",
                           "2026-09-23T07:00:00+08:00", "2026-09-23T08:00:00+08:00")
        svc.clock.freeze("2026-09-23T08:00:00+08:00")
        svc.publish_daily_report("r1", "night")
        # 迟到数据 08:31 才入库
        svc.clock.freeze("2026-09-23T08:31:00+08:00")
        svc.ingest_snapshot("aquatic", "east", "fish", 2, 5, 20, 20, 100,
                            "2026-09-23T07:10:00+08:00", idempotency_key="v2")
        # 按 08:00 截止核对：日报发布所见不被迟到数据改写
        reconciliation = svc.reconcile_report("r1")
        self.assertTrue(all(item["ok"] for item in reconciliation))
        # 当前视图则已是真实异常
        current = svc._compute_metrics(
            "aquatic", "east", "fish",
            parse("2026-09-23T07:00:00+08:00"), parse("2026-09-23T08:00:00+08:00"),
            parse("2026-09-23T09:00:00+08:00"))
        self.assertEqual(current["alert_level"], "critical")


class RuleVersionTest(unittest.TestCase):
    def test_published_rules_append_versions(self) -> None:
        svc = build_service()
        before = svc.rules.latest("thresholds:aquatic")["version"]
        published = svc.rules.publish("thresholds:aquatic", {"safety_coverage_days": 9.0})
        self.assertEqual(published["version"], before + 1)
        self.assertEqual(published["content"]["safety_coverage_days"], 9.0)
        # 历史版本仍可取回
        old = svc.store.query_one(
            "SELECT content FROM rules WHERE rule_id='thresholds:aquatic' AND version=?",
            (before,))
        self.assertNotEqual(loads(old["content"])["safety_coverage_days"], 9.0)


if __name__ == "__main__":
    unittest.main()
