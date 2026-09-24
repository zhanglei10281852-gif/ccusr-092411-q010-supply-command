"""端到端场景：夜班误判、早班发现、迟到校正、补算与日报核对。

时间线（2026-09-23 夜班 → 09-24 早班）：
- 水产(aquatic/fish/A区) 连续数小时库存低于安全线，但快照因来源系统故障
  在夜班结算时缺失，夜班看到的是"无数据"并据此解除了一条早先告警；
- 早班期间迟到快照陆续到达，历史窗口被校正为真实异常；
- 已发布处置单保留原判断，追加修正记录；
- 系统恢复后补算遗漏窗口，不重复派单；
- 日报与全部处置单最终逐项核对通过。
"""
from __future__ import annotations

import unittest
from datetime import timedelta

from supply_command import clock
from supply_command.app import SupplyCommandService
from supply_command.alerts import (
    Actor, Approval, STATE_RESOLVED, STATE_CORRECTED,
    STATE_ESCALATED, STATE_SUPPRESSED,
)
from supply_command.catalog import (
    Catalog, CategoryRule,
    AQUATIC, FRUIT_VEG, GRAIN_OIL, SNACK,
    ROLE_OPERATOR, ROLE_COMMANDER, ROLE_DISPATCH, ROLE_AUDITOR,
)
from supply_command.errors import AuthorizationError, BasisError
from supply_command.lineage import LineageGraph
from supply_command.reports import DailyReport, build_report_items, day_bounds
from supply_command.snapshots import Snapshot
from supply_command.store import EventStore
from supply_command.windows import (
    QUALITY_MISSING, QUALITY_LATE,
    LEVEL_CRITICAL, LEVEL_WARN, LEVEL_NONE,
)
from supply_command.whatif import Scenario, WhatIfSandbox


def snap(business_line: str, category: str, region: str, t, *,
         stock: float, inbound: float = 0.0, outbound: float = 0.0,
         source: str = "aqs-a01", version: int = 1, quality: str = "ok",
         delay_minutes: float = 5.0, note: str = "",
         recorded_at=None) -> Snapshot:
    """构造快照：默认在业务时间 5 分钟后到达（按时数据）。

    delay_minutes 很大时模拟迟到；recorded_at 显式传入可精确定位。
    """
    arrival = recorded_at or (clock.parse(t) + timedelta(minutes=delay_minutes))
    return Snapshot(
        business_line, category, region, t,
        stock=stock, inbound=inbound, outbound=outbound,
        version=version, source=source, quality=quality, note=note,
        recorded_at=arrival,
    )


def make_service() -> SupplyCommandService:
    service = SupplyCommandService(EventStore(":memory:"), Catalog())
    service.publish_rule(CategoryRule(
        AQUATIC, "fish", "A", safe_stock=20.0,
        warn_coverage_days=2.0, critical_coverage_days=1.0,
        throughput_per_hour=5.0, max_late_seconds=1800, max_gap_seconds=1800,
        valid_from=clock.at(2026, 9, 1),
    ))
    service.publish_rule(CategoryRule(
        FRUIT_VEG, "vegetable", "B", safe_stock=30.0,
        warn_coverage_days=1.5, critical_coverage_days=0.5,
        throughput_per_hour=8.0,
        valid_from=clock.at(2026, 9, 1),
    ))
    service.publish_rule(CategoryRule(
        FRUIT_VEG, "vegetable", "C", safe_stock=30.0,
        warn_coverage_days=1.5, critical_coverage_days=0.5,
        throughput_per_hour=8.0,
        valid_from=clock.at(2026, 9, 1),
    ))
    service.publish_rule(CategoryRule(
        GRAIN_OIL, "rice", "C", safe_stock=100.0,
        warn_coverage_days=7.0, critical_coverage_days=3.0,
        throughput_per_hour=2.0,
        valid_from=clock.at(2026, 9, 1),
    ))
    service.publish_rule(CategoryRule(
        SNACK, "nut", "D", safe_stock=10.0,
        warn_coverage_days=3.0, critical_coverage_days=1.0,
        throughput_per_hour=1.0,
        valid_from=clock.at(2026, 9, 1),
    ))
    return service


def approver(user: str, role: str = ROLE_COMMANDER, shift: str = "night") -> Approval:
    return Approval(Actor(user, role, shift), Actor("auditor-wang", ROLE_AUDITOR, shift))


class ScenarioTest(unittest.TestCase):
    def setUp(self) -> None:
        self.svc = make_service()

    def test_01_snapshot_ingest_idempotent_and_versioned(self) -> None:
        # 果蔬快照正常到达
        t = clock.at(2026, 9, 23, 20, 0)
        s1 = snap(FRUIT_VEG, "vegetable", "B", t, stock=80.0,
                  inbound=10, outbound=4, source="pos-b01")
        r1 = self.svc.ingest(s1)
        self.assertFalse(r1.duplicate)

        # 完全相同的重复上报 -> 幂等，不新增事件
        r2 = self.svc.ingest(s1)
        self.assertTrue(r2.duplicate)
        self.assertEqual(
            len([e for e in self.svc.store.all_events() if e.event_type == "snapshot.received"]),
            1)

        # 同一业务时刻的修正数据 -> 新版本追加，不覆盖
        s2 = snap(FRUIT_VEG, "vegetable", "B", t, stock=82.0,
                  inbound=10, outbound=2, source="pos-b01",
                  version=2, quality="corrected")
        r3 = self.svc.ingest(s2)
        self.assertFalse(r3.duplicate)
        self.assertEqual(
            len([e for e in self.svc.store.all_events() if e.event_type == "snapshot.received"]),
            2)

    def test_02_window_metrics_levels_and_pressure(self) -> None:
        t = clock.at(2026, 9, 23, 20, 0)
        # 水产：库存 15 吨 < 安全线 20；半小时出货 5 吨 => 日出货 240 吨/日 =>
        # 覆盖天数 15/240 = 0.0625 < 1.0 => critical
        self.svc.ingest(snap(AQUATIC, "fish", "A", t, stock=15.0, outbound=5.0))
        event = self.svc.close_window(AQUATIC, "fish", "A", t,
                                      evaluation_at=clock.at(2026, 9, 23, 20, 35))
        w = event.payload
        self.assertEqual(w["level"], LEVEL_CRITICAL)
        self.assertEqual(w["quality"], "anomaly")
        self.assertAlmostEqual(w["coverage_days"], 0.0625, places=3)
        # 吞吐压力：实际 10 吨/小时 / 参考 5 吨/小时 = 2
        self.assertAlmostEqual(w["throughput_pressure"], 2.0, places=3)

    def test_03_missing_vs_late_vs_anomaly(self) -> None:
        t = clock.at(2026, 9, 23, 21, 0)
        # 21:00 窗口结算时（21:40，超过缺失宽限）没有任何快照 -> missing
        ev_missing = self.svc.close_window(
            AQUATIC, "fish", "A", t, evaluation_at=clock.at(2026, 9, 23, 21, 40))
        self.assertEqual(ev_missing.event_type, "window.missing")
        self.assertEqual(ev_missing.payload["quality"], QUALITY_MISSING)

        # 22:00 窗口先结算（无数据，missing），23:30 迟到快照才到达
        t22 = clock.at(2026, 9, 23, 22, 0)
        self.svc.close_window(AQUATIC, "fish", "A", t22,
                              evaluation_at=clock.at(2026, 9, 23, 22, 40))
        late_snap = snap(AQUATIC, "fish", "A", clock.at(2026, 9, 23, 22, 5),
                         stock=12.0, outbound=6.0,
                         recorded_at=clock.at(2026, 9, 23, 23, 30))
        result = self.svc.ingest(late_snap)
        self.assertTrue(result.late)
        # 迟到快照触发历史窗口重算
        self.assertEqual(len(result.recalculated_windows), 1)

        windows = self.svc.projection().metric_windows(AQUATIC, "fish", "A")
        recalc = [w for w in windows if w.revision == 2]
        self.assertEqual(len(recalc), 1)
        # 重算后：数据迟到 + 指标真实越线，quality 标记 late（可与缺失、异常区分）
        self.assertEqual(recalc[0].quality, QUALITY_LATE)
        self.assertEqual(recalc[0].level, LEVEL_CRITICAL)
        # 首算结论（missing）仍在，未被抹除
        first = [w for w in windows if w.revision == 1 and w.quality == QUALITY_MISSING]
        self.assertEqual(len(first), 2)

    def test_04_dual_time_views_night_vs_morning(self) -> None:
        # 22:05 的真实快照在 23:30 才到达
        business_t = clock.at(2026, 9, 23, 22, 5)
        self.svc.ingest(snap(AQUATIC, "fish", "A", business_t, stock=12.0,
                             outbound=6.0,
                             recorded_at=clock.at(2026, 9, 23, 23, 30)))

        proj = self.svc.projection()
        # 夜班 22:40 视角：看不到 23:30 才到的快照 -> 无数据
        snap_night, _ = proj.latest_known_snapshot(
            AQUATIC, "fish", "A", clock.at(2026, 9, 23, 22, 40), late=False)
        self.assertIsNone(snap_night)
        # 早班用现在已知数据还原业务时刻 -> 能看到
        snap_now, recorded = proj.latest_known_snapshot(
            AQUATIC, "fish", "A", clock.at(2026, 9, 24, 8, 0), late=True)
        self.assertIsNotNone(snap_now)
        self.assertEqual(snap_now.stock, 12.0)
        self.assertEqual(recorded, clock.at(2026, 9, 23, 23, 30))

    def test_05_alert_lifecycle_dual_control_and_chain(self) -> None:
        t = clock.at(2026, 9, 23, 20, 0)
        self.svc.ingest(snap(AQUATIC, "fish", "A", t, stock=15.0, outbound=5.0))
        w = self.svc.close_window(AQUATIC, "fish", "A", t,
                                  evaluation_at=clock.at(2026, 9, 23, 20, 35))
        cmd = approver("cmdr-li", ROLE_COMMANDER, "night")

        # 无权限角色不能开告警
        with self.assertRaises(AuthorizationError):
            bad = Approval(Actor("u1", ROLE_AUDITOR, "night"),
                           Actor("u2", ROLE_AUDITOR, "night"))
            self.svc.report_alert(AQUATIC, "fish", "A", LEVEL_CRITICAL,
                                  [w.aggregate_id], bad, occurred_at=t + timedelta(minutes=36))

        opened, created = self.svc.report_alert(
            AQUATIC, "fish", "A", LEVEL_CRITICAL, [w.aggregate_id], cmd,
            occurred_at=t + timedelta(minutes=36))
        self.assertTrue(created)
        alert_id = opened.aggregate_id

        # 重复上报：同品类区域已有活跃告警 -> 不新增
        _, created2 = self.svc.report_alert(
            AQUATIC, "fish", "A", LEVEL_CRITICAL, [w.aggregate_id], cmd,
            occurred_at=t + timedelta(minutes=40))
        self.assertFalse(created2)

        # 升级：同一人不能既发起又复核
        same_person = Approval(Actor("cmdr-li", ROLE_COMMANDER, "night"),
                               Actor("cmdr-li", ROLE_COMMANDER, "night"))
        with self.assertRaises(AuthorizationError):
            self.svc.escalate_alert(alert_id, same_person, "严重", "mgr-zhao",
                                    at=t + timedelta(minutes=45))
        self.svc.escalate_alert(alert_id, cmd, "连续低于安全线", "mgr-zhao",
                                at=t + timedelta(minutes=45))
        self.assertEqual(self.svc._load_alert(alert_id).state, STATE_ESCALATED)

        # 转交：班次交接，唯一责任人变化且责任链追加
        dispatch = Approval(Actor("disp-chen", ROLE_DISPATCH, "night"),
                            Actor("disp-chen", ROLE_DISPATCH, "night"))
        self.svc.transfer_alert(alert_id, dispatch, "disp-zhou",
                                at=t + timedelta(hours=1), reason="夜班转早班")
        alert = self.svc._load_alert(alert_id)
        self.assertEqual(alert.current_owner, "disp-zhou")
        self.assertEqual(len(alert.handoffs), 1)
        self.assertEqual(alert.handoffs[0].from_owner, "mgr-zhao")
        # 再转交一次，责任链仍只有一个当前责任人
        self.svc.transfer_alert(
            alert_id,
            Approval(Actor("disp-zhou", ROLE_DISPATCH, "morning"),
                     Actor("disp-zhou", ROLE_DISPATCH, "morning")),
            "cmdr-sun", at=t + timedelta(hours=2), reason="早班接班")
        alert = self.svc._load_alert(alert_id)
        self.assertEqual(alert.current_owner, "cmdr-sun")
        self.assertEqual(len(alert.handoffs), 2)
        self.assertEqual(alert.correlation_id, f"chain:{alert_id}")

        # 抑制需要双人复核（发起人不能自任复核人）
        with self.assertRaises(AuthorizationError):
            self.svc.suppress_alert(
                alert_id,
                Approval(Actor("disp-chen", ROLE_DISPATCH, "night"),
                         Actor("disp-chen", ROLE_DISPATCH, "night")),
                clock.at(2026, 9, 24, 2), "检修", at=t + timedelta(hours=3))
        self.svc.suppress_alert(
            alert_id,
            Approval(Actor("disp-chen", ROLE_DISPATCH, "night"),
                     Actor("auditor-wang", ROLE_AUDITOR, "night")),
            clock.at(2026, 9, 24, 2), "计量设备检修窗口",
            at=t + timedelta(hours=3))
        self.assertEqual(self.svc._load_alert(alert_id).state, STATE_SUPPRESSED)

    def test_06_resolve_requires_reconstructible_basis(self) -> None:
        t = clock.at(2026, 9, 23, 20, 0)
        self.svc.ingest(snap(AQUATIC, "fish", "A", t, stock=15.0, outbound=5.0))
        w = self.svc.close_window(AQUATIC, "fish", "A", t,
                                  evaluation_at=clock.at(2026, 9, 23, 20, 35))
        cmd = approver("cmdr-li")
        opened, _ = self.svc.report_alert(
            AQUATIC, "fish", "A", LEVEL_CRITICAL, [w.aggregate_id], cmd,
            occurred_at=t + timedelta(minutes=36))
        # 仅凭汇总日报（无窗口依据）解除 -> 拒绝
        with self.assertRaises(BasisError):
            self.svc.resolve_alert(opened.aggregate_id, cmd, [],
                                   at=t + timedelta(hours=2))
        # 引用窗口依据 + 双人复核 -> 解除成功
        self.svc.resolve_alert(opened.aggregate_id, cmd, [w.aggregate_id],
                               at=t + timedelta(hours=2))
        self.assertEqual(self.svc._load_alert(opened.aggregate_id).state, STATE_RESOLVED)

    def test_07_late_data_corrects_history_but_keeps_disposition(self) -> None:
        # 20:00 窗口：按时快照显示库存充足（半小时出货 0.2 吨 => 日 9.6 吨，
        # 库存 60 吨覆盖 6.25 天）-> none
        t = clock.at(2026, 9, 23, 20, 0)
        self.svc.ingest(snap(AQUATIC, "fish", "A", t, stock=60.0,
                             inbound=8, outbound=0.2))
        w1 = self.svc.close_window(AQUATIC, "fish", "A", t,
                                   evaluation_at=clock.at(2026, 9, 23, 20, 35))
        self.assertEqual(w1.payload["level"], LEVEL_NONE)

        # 21:00 窗口：出货骤增，库存 15 吨，覆盖 0.06 天 -> 真实异常 critical
        t21 = clock.at(2026, 9, 23, 21, 0)
        self.svc.ingest(snap(AQUATIC, "fish", "A", t21, stock=15.0,
                             inbound=4, outbound=5.0))
        w2 = self.svc.close_window(AQUATIC, "fish", "A", t21,
                                   evaluation_at=clock.at(2026, 9, 23, 21, 35))
        self.assertEqual(w2.payload["level"], LEVEL_CRITICAL)

        cmd = approver("cmdr-li", ROLE_COMMANDER, "night")
        opened, _ = self.svc.report_alert(
            AQUATIC, "fish", "A", LEVEL_CRITICAL, [w2.aggregate_id], cmd,
            occurred_at=clock.at(2026, 9, 23, 21, 36))
        # 夜班派单
        disp, created = self.svc.dispatch_incident(
            opened.aggregate_id, "team-night", "现场核查",
            "按当时指标处置", Actor("cmdr-li", ROLE_COMMANDER, "night"),
            at=clock.at(2026, 9, 23, 21, 40))
        self.assertTrue(created)
        incident_id = disp.aggregate_id
        # 重复派单（恢复补算后再次触发）-> 不新增
        _, created_again = self.svc.dispatch_incident(
            opened.aggregate_id, "team-night", "现场核查",
            "按当时指标处置", Actor("cmdr-li", ROLE_COMMANDER, "night"),
            at=clock.at(2026, 9, 24, 9, 5))
        self.assertFalse(created_again)

        # 夜班依据 w2 解除
        self.svc.resolve_alert(opened.aggregate_id, cmd, [w2.aggregate_id],
                               at=clock.at(2026, 9, 23, 22, 0))

        # 22:00 窗口先缺失结算（夜班只能看到"无数据"）
        t22 = clock.at(2026, 9, 23, 22, 0)
        w3 = self.svc.close_window(AQUATIC, "fish", "A", t22,
                                   evaluation_at=clock.at(2026, 9, 23, 22, 40))
        self.assertEqual(w3.payload["quality"], QUALITY_MISSING)

        # 早班：22:05 的迟到快照 23:30 才到，触发重算
        late = snap(AQUATIC, "fish", "A", clock.at(2026, 9, 23, 22, 5),
                    stock=8.0, outbound=8.0,
                    recorded_at=clock.at(2026, 9, 23, 23, 30))
        r = self.svc.ingest(late)
        self.assertTrue(r.late)
        recalc_id = r.recalculated_windows[0]

        # 迟到数据推翻解除前提：告警 corrected；处置单保留原判断 + 追加修正
        events = self.svc.correct_after_recalc(
            opened.aggregate_id, recalc_id,
            Actor("auditor-wang", ROLE_AUDITOR, "morning"),
            "迟到快照显示 22 点窗口库存仅 8 吨",
            at=clock.at(2026, 9, 24, 8, 30))
        self.assertGreaterEqual(len(events), 2)
        alert = self.svc._load_alert(opened.aggregate_id)
        self.assertEqual(alert.state, STATE_CORRECTED)
        # 解除记录仍在
        self.assertIsNotNone(alert.resolved_at)
        self.assertEqual(alert.resolution_basis, [w2.aggregate_id])

        inc = self.svc._load_incident(incident_id)
        # 原判断完整保留
        self.assertEqual(inc.original.decision, "现场核查")
        self.assertEqual(len(inc.amendments), 1)
        self.assertEqual(inc.amendments[0].causation_event_id, recalc_id)
        # 修正人与原处置人不是同一人（双人复核）
        self.assertNotEqual(inc.amendments[0].amended_by, inc.amendments[0].reviewer)

    def test_08_recover_backfills_gaps_without_duplicate_dispatch(self) -> None:
        # 只在 20:05、22:05 有快照，21 点窗口完全缺失，模拟系统宕机后恢复
        self.svc.ingest(snap(AQUATIC, "fish", "A",
                             clock.at(2026, 9, 24, 20, 5),
                             stock=10.0, outbound=8.0,
                             recorded_at=clock.at(2026, 9, 24, 20, 35)))
        self.svc.ingest(snap(AQUATIC, "fish", "A",
                             clock.at(2026, 9, 24, 22, 5),
                             stock=9.0, outbound=7.0,
                             recorded_at=clock.at(2026, 9, 24, 22, 35)))
        report = self.svc.recover(clock.at(2026, 9, 24, 23, 0))
        # 30 分钟窗口：20:00、20:30、21:00、21:30、22:00、22:30 共 6 个窗口补算
        self.assertEqual(report["windows_backfilled"], 6)
        # 有真实数据的 20:00、22:00 严重越线只补开一条告警（同品类去重）
        self.assertEqual(len(report["alerts_opened"]), 1)

        # 再恢复一次：幂等，不产生新窗口、不重复派单
        before = self.svc.store.count()
        report2 = self.svc.recover(clock.at(2026, 9, 24, 23, 0))
        self.assertEqual(report2["windows_backfilled"], 0)
        self.assertEqual(self.svc.store.count(), before)

        # 无数据窗口如实标为 missing，没有被误当成真实异常派单
        ws = self.svc.projection().metric_windows(AQUATIC, "fish", "A")
        missing_starts = {w.start for w in ws if w.quality == QUALITY_MISSING}
        self.assertIn(clock.at(2026, 9, 24, 21, 0), missing_starts)
        self.assertIn(clock.at(2026, 9, 24, 21, 30), missing_starts)
        # 有真实数据的窗口不被标成缺失
        self.assertNotIn(clock.at(2026, 9, 24, 20, 0), missing_starts)

    def test_08b_recover_ignores_snapshots_arriving_after_recovery(self) -> None:
        # 20:05 的快照 20:35 到达；21:05 的快照要到 23:30 才到达
        self.svc.ingest(snap(AQUATIC, "fish", "A",
                             clock.at(2026, 9, 24, 20, 5),
                             stock=10.0, outbound=8.0,
                             recorded_at=clock.at(2026, 9, 24, 20, 35)))
        # 23:00 系统恢复补算：21 点窗口尚无数据，按缺失补算，不误派单
        report = self.svc.recover(clock.at(2026, 9, 24, 23, 0))
        ws = self.svc.projection().metric_windows(AQUATIC, "fish", "A")
        by_start = {w.start: w for w in ws if w.revision == 1}
        self.assertEqual(
            by_start[clock.at(2026, 9, 24, 21, 0)].quality, QUALITY_MISSING)
        # 只有 20:00 真实异常窗口补开一条告警；21 点缺失窗口不误派
        self.assertEqual(len(report["alerts_opened"]), 1)

        # 23:30 迟到快照真正到达：摄入路径自动重算 21 点窗口
        result = self.svc.ingest(snap(
            AQUATIC, "fish", "A", clock.at(2026, 9, 24, 21, 5),
            stock=10.0, outbound=8.0,
            recorded_at=clock.at(2026, 9, 24, 23, 30)))
        self.assertEqual(len(result.recalculated_windows), 1)
        windows = self.svc.projection().metric_windows(AQUATIC, "fish", "A")
        r2 = [w for w in windows if w.revision == 2]
        self.assertEqual(len(r2), 1)
        self.assertEqual(r2[0].quality, QUALITY_LATE)

    def test_09_lineage_traces_metric_to_source_and_resolution(self) -> None:
        t = clock.at(2026, 9, 23, 20, 0)
        self.svc.ingest(snap(AQUATIC, "fish", "A", t, stock=15.0, outbound=5.0))
        w = self.svc.close_window(AQUATIC, "fish", "A", t,
                                  evaluation_at=clock.at(2026, 9, 23, 20, 35))
        cmd = approver("cmdr-li")
        opened, _ = self.svc.report_alert(
            AQUATIC, "fish", "A", LEVEL_CRITICAL, [w.aggregate_id], cmd,
            occurred_at=t + timedelta(minutes=36))
        self.svc.resolve_alert(opened.aggregate_id, cmd, [w.aggregate_id],
                               at=t + timedelta(hours=2))

        graph = LineageGraph(self.svc.store.all_events())
        trace = graph.trace(opened.aggregate_id)
        kinds = {n.kind for n in trace["upstream"]}
        # 告警 -> 窗口指标 -> 快照 -> 来源，全链可还原
        self.assertIn("window", kinds)
        self.assertIn("snapshot", kinds)
        self.assertIn("source", kinds)
        source_nodes = [n for n in trace["upstream"] if n.kind == "source"]
        self.assertEqual(source_nodes[0].label, "来源：aqs-a01")

        # 直接回答："上一班解除告警的依据能否从数字中还原？"
        explanation = graph.explain_alert_resolution(opened.aggregate_id)
        self.assertTrue(explanation["resolvable_from_aggregates"])
        self.assertGreaterEqual(len(explanation["source_snapshots"]), 1)

    def test_10_whatif_sandbox_is_isolated(self) -> None:
        t = clock.at(2026, 9, 23, 20, 0)
        self.svc.ingest(snap(AQUATIC, "fish", "A", t, stock=15.0, outbound=5.0))
        before = self.svc.store.count()
        sandbox = WhatIfSandbox(self.svc.store.all_events(), self.svc.catalog)
        result = sandbox.run(Scenario(
            name="扩容+增加到货", capacity_delta=600.0, inbound_extra=10.0,
            start=clock.at(2026, 9, 23, 20), end=clock.at(2026, 9, 23, 21)))
        # 推演后覆盖天数显著改善，告警等级发生变化
        self.assertTrue(any(d["alert_would_change"] for d in result.diff()))
        # 主时间线零污染
        self.assertEqual(self.svc.store.count(), before)

    def test_11_daily_report_reconciles_line_by_line(self) -> None:
        t1 = clock.at(2026, 9, 24, 8, 0)
        t2 = clock.at(2026, 9, 24, 8, 30)
        self.svc.ingest(snap(AQUATIC, "fish", "A", t1, stock=55.0,
                             inbound=4, outbound=0.3))
        self.svc.close_window(AQUATIC, "fish", "A", t1,
                              evaluation_at=clock.at(2026, 9, 24, 8, 35))
        self.svc.ingest(snap(AQUATIC, "fish", "A", t2, stock=50.0,
                             inbound=2, outbound=0.4))
        w2 = self.svc.close_window(AQUATIC, "fish", "A", t2,
                                   evaluation_at=clock.at(2026, 9, 24, 9, 5))
        # 第二窗库存仍高于安全线，构造一张处置单验证日报引用核对
        cmd = approver("cmdr-li", ROLE_COMMANDER, "morning")
        opened, _ = self.svc.report_alert(
            AQUATIC, "fish", "A", LEVEL_WARN, [w2.aggregate_id], cmd,
            occurred_at=clock.at(2026, 9, 24, 9, 6))
        self.svc.dispatch_incident(
            opened.aggregate_id, "team-a", "补货", "预防性补货",
            Actor("cmdr-li", ROLE_COMMANDER, "morning"),
            at=clock.at(2026, 9, 24, 9, 10))

        start, end = day_bounds(clock.at(2026, 9, 24, 12))
        proj = self.svc.projection()
        windows = [w for w in proj.metric_windows(AQUATIC, "fish", "A", include_revisions=False)
                   if start <= w.start < end]
        incidents = {
            (AQUATIC, "fish", "A"): [i.incident_id for i in self.svc.list_incidents()]
        }
        items = build_report_items(windows, incidents)
        publish_at = clock.at(2026, 9, 24, 23, 59)
        event = DailyReport.publish_event(
            "daily:2026-09-24", "2026-09-24", items, ROLE_AUDITOR,
            publish_at, recorded_at=publish_at)
        self.svc.store.append(event)

        result = DailyReport.reconcile(self.svc.store.all_events(), "daily:2026-09-24")
        self.assertTrue(result["reconciled"], result["findings"])
        self.assertEqual(result["items_checked"], 1)

        # 日报条目可逐项追到窗口与处置单
        graph = LineageGraph(self.svc.store.all_events())
        trace = graph.trace("daily:2026-09-24")
        kinds = {n.kind for n in trace["upstream"]}
        self.assertIn("window", kinds)
        self.assertIn("incident", kinds)

    def test_12_merge_duplicate_alerts(self) -> None:
        t = clock.at(2026, 9, 24, 10, 0)
        self.svc.ingest(snap(FRUIT_VEG, "vegetable", "B", t, stock=20.0,
                             outbound=8.0, source="pos-b01"))
        w = self.svc.close_window(FRUIT_VEG, "vegetable", "B", t,
                                  evaluation_at=clock.at(2026, 9, 24, 10, 35))
        op = Approval(Actor("op-zhao", ROLE_OPERATOR, "day"),
                      Actor("auditor-wang", ROLE_AUDITOR, "day"))
        e1, c1 = self.svc.report_alert(
            FRUIT_VEG, "vegetable", "B", LEVEL_WARN, [w.aggregate_id], op,
            occurred_at=clock.at(2026, 9, 24, 10, 36))
        self.assertTrue(c1)
        # 第二条由不同人重复上报 -> 直接被去重（同活跃告警）
        op2 = Approval(Actor("op-qian", ROLE_OPERATOR, "day"),
                       Actor("auditor-wang", ROLE_AUDITOR, "day"))
        e2, c2 = self.svc.report_alert(
            FRUIT_VEG, "vegetable", "B", LEVEL_WARN, [w.aggregate_id], op2,
            occurred_at=clock.at(2026, 9, 24, 10, 40))
        self.assertFalse(c2)
        self.assertEqual(e1.aggregate_id, e2.aggregate_id)

        # 显式合并两条独立告警（不同区域）：子告警留归并事件，主告警挂子告警
        self.svc.ingest(snap(FRUIT_VEG, "vegetable", "C", t, stock=18.0,
                             outbound=9.0, source="pos-c01"))
        wc = self.svc.close_window(FRUIT_VEG, "vegetable", "C", t,
                                   evaluation_at=clock.at(2026, 9, 24, 10, 35))
        e3, c3 = self.svc.report_alert(
            FRUIT_VEG, "vegetable", "C", LEVEL_WARN, [wc.aggregate_id], op2,
            occurred_at=clock.at(2026, 9, 24, 10, 41))
        self.assertTrue(c3)
        merge = self.svc.merge_alerts(
            e3.aggregate_id, e1.aggregate_id, op2,
            at=clock.at(2026, 9, 24, 10, 45), duplicate_window_id=wc.aggregate_id)
        self.assertTrue(merge.payload["dedup"])
        child = self.svc._load_alert(e3.aggregate_id)
        self.assertEqual(child.merged_into, e1.aggregate_id)
        parent = self.svc._load_alert(e1.aggregate_id)
        self.assertIn(e3.aggregate_id, parent.merged_children)
        # 责任链归并到主告警
        self.assertEqual(merge.correlation_id, f"chain:{e1.aggregate_id}")

        # 重复合并幂等：不产生新事件
        count_before = self.svc.store.count()
        self.svc.merge_alerts(
            e3.aggregate_id, e1.aggregate_id, op2,
            at=clock.at(2026, 9, 24, 10, 50))
        self.assertEqual(self.svc.store.count(), count_before)

    def test_13_persistence_restart_rebuilds_state(self) -> None:
        import tempfile
        from pathlib import Path
        from supply_command.store import EventStore as ES

        tmpdir = tempfile.mkdtemp()
        db = str(Path(tmpdir) / "supply.db")

        # 第一次启动：发规则、快照、窗口、告警
        store1 = ES(db)
        svc1 = SupplyCommandService(store1)
        svc1.publish_rule(CategoryRule(
            AQUATIC, "fish", "A", safe_stock=20.0,
            warn_coverage_days=2.0, critical_coverage_days=1.0,
            throughput_per_hour=5.0, max_late_seconds=1800, max_gap_seconds=1800,
            valid_from=clock.at(2026, 9, 1)))
        t = clock.at(2026, 9, 23, 20, 0)
        svc1.ingest(snap(AQUATIC, "fish", "A", t, stock=15.0, outbound=5.0))
        w = svc1.close_window(AQUATIC, "fish", "A", t,
                              evaluation_at=clock.at(2026, 9, 23, 20, 35))
        svc1.report_alert(
            AQUATIC, "fish", "A", LEVEL_CRITICAL, [w.aggregate_id],
            approver("cmdr-li"), occurred_at=t + timedelta(minutes=36))
        event_count = store1.count()
        store1.close()

        # 重新打开：规则目录与全部聚合从事件流重建
        store2 = ES(db)
        svc2 = SupplyCommandService(store2)
        self.assertEqual(store2.count(), event_count)
        rule = svc2.catalog.rule_at(AQUATIC, "fish", "A", t)
        self.assertEqual(rule.safe_stock, 20.0)
        alerts = svc2.list_alerts()
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].state, "alerted")
        ws = svc2.projection().metric_windows(AQUATIC, "fish", "A")
        self.assertEqual(len(ws), 1)
        self.assertEqual(ws[0].level, LEVEL_CRITICAL)
        # 重启后重复上报依然幂等
        r = svc2.ingest(snap(AQUATIC, "fish", "A", t, stock=15.0, outbound=5.0))
        self.assertTrue(r.duplicate)
        store2.close()


if __name__ == "__main__":
    unittest.main()
