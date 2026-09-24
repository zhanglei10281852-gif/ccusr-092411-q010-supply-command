"""端到端演示：夜班日报说库存充足，早班发现水产连续数小时低于安全线。

直接运行：

    python3 tools/demo.py

脚本不依赖网络，打印统一时间线、双时态视图对比、责任链与日报核对结果。
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from supply_command import clock  # noqa: E402
from supply_command.app import SupplyCommandService  # noqa: E402
from supply_command.alerts import Actor, Approval  # noqa: E402
from supply_command.catalog import (  # noqa: E402
    AQUATIC, Catalog, CategoryRule,
    ROLE_AUDITOR, ROLE_COMMANDER,
)
from supply_command.lineage import LineageGraph  # noqa: E402
from supply_command.reports import DailyReport, build_report_items, day_bounds  # noqa: E402
from supply_command.snapshots import Snapshot  # noqa: E402
from supply_command.store import EventStore  # noqa: E402

QUALITY_CN = {"ok": "正常", "late": "数据迟到", "missing": "数据缺失", "anomaly": "真实供应异常"}
LEVEL_CN = {"none": "正常", "warn": "预警", "critical": "严重"}


def main() -> None:
    svc = SupplyCommandService(EventStore(":memory:"), Catalog())
    svc.publish_rule(CategoryRule(
        AQUATIC, "fish", "A", safe_stock=20.0,
        warn_coverage_days=2.0, critical_coverage_days=1.0,
        throughput_per_hour=5.0, max_late_seconds=1800, max_gap_seconds=1800,
        valid_from=clock.at(2026, 9, 1)))

    night_cmd = Approval(Actor("cmdr-li", ROLE_COMMANDER, "夜班"),
                         Actor("auditor-wang", ROLE_AUDITOR, "夜班"))
    morning_aud = Actor("auditor-wang", ROLE_AUDITOR, "早班")

    print("=" * 72)
    print("2026-09-23 夜班：21 点水产出货骤增")
    print("=" * 72)
    t21 = clock.at(2026, 9, 23, 21, 0)
    svc.ingest(Snapshot(AQUATIC, "fish", "A", t21, stock=15.0, inbound=4,
                        outbound=5.0, source="aqs-a01",
                        recorded_at=clock.at(2026, 9, 23, 21, 5)))
    w21 = svc.close_window(AQUATIC, "fish", "A", t21,
                           evaluation_at=clock.at(2026, 9, 23, 21, 35))
    print(f"21:00 窗口：等级={LEVEL_CN[w21.payload['level']]}，"
          f"质量={QUALITY_CN[w21.payload['quality']]}，"
          f"覆盖天数={w21.payload['coverage_days']}，"
          f"吞吐压力={w21.payload['throughput_pressure']}")

    opened, _ = svc.report_alert(
        AQUATIC, "fish", "A", "critical", [w21.aggregate_id], night_cmd,
        occurred_at=clock.at(2026, 9, 23, 21, 36))
    svc.dispatch_incident(
        opened.aggregate_id, "team-night", "现场核查", "按当时指标处置",
        Actor("cmdr-li", ROLE_COMMANDER, "夜班"),
        at=clock.at(2026, 9, 23, 21, 40))
    svc.resolve_alert(opened.aggregate_id, night_cmd, [w21.aggregate_id],
                      at=clock.at(2026, 9, 23, 22, 0))
    print(f"22:00 夜班依据窗口 {w21.aggregate_id} 解除告警并保留处置单")

    print()
    print("=" * 72)
    print("22 点来源系统故障：窗口结算时没有任何快照")
    print("=" * 72)
    t22 = clock.at(2026, 9, 23, 22, 0)
    w22 = svc.close_window(AQUATIC, "fish", "A", t22,
                           evaluation_at=clock.at(2026, 9, 23, 22, 40))
    print(f"22:00 窗口首算：质量={QUALITY_CN[w22.payload['quality']]}（不是真实异常，不派单）")

    print()
    print("=" * 72)
    print("2026-09-24 早班：迟到快照 23:30 才到达")
    print("=" * 72)
    result = svc.ingest(Snapshot(
        AQUATIC, "fish", "A", clock.at(2026, 9, 23, 22, 5),
        stock=8.0, outbound=8.0, source="aqs-a01",
        recorded_at=clock.at(2026, 9, 23, 23, 30)))
    print(f"迟到快照触发重算：{result.recalculated_windows}")
    recalc_id = result.recalculated_windows[0]

    corrected = svc.correct_after_recalc(
        opened.aggregate_id, recalc_id, morning_aud,
        "迟到快照显示 22 点窗口库存仅 8 吨，连续低于安全线",
        at=clock.at(2026, 9, 24, 8, 30))
    alert = svc._load_alert(opened.aggregate_id)
    incident = svc.list_incidents()[0]
    print(f"告警状态：resolved -> corrected（解除时间 {alert.resolved_at:%H:%M} 与依据仍保留）")
    print(f"处置单：原判断=「{incident.original.decision}」未改写；"
          f"追加 {len(incident.amendments)} 条修正（双人复核）")

    print()
    print("-" * 72)
    print("双时态对比：夜班 22:40 视角 vs 早班 08:00 视角")
    print("-" * 72)
    proj = svc.projection()
    night_view, _ = proj.latest_known_snapshot(
        AQUATIC, "fish", "A", clock.at(2026, 9, 23, 22, 40), late=False)
    now_view, recorded = proj.latest_known_snapshot(
        AQUATIC, "fish", "A", clock.at(2026, 9, 24, 8, 0), late=True)
    print(f"夜班当时可见：{'无数据（来源系统故障）' if night_view is None else night_view.stock}")
    print(f"早班事后还原：库存 {now_view.stock} 吨（实际 {recorded:%H:%M} 才到）")

    print()
    print("-" * 72)
    print("血缘追溯：任一指标 -> 窗口（含规则版本）-> 快照 -> 来源点位")
    print("-" * 72)
    graph = LineageGraph(svc.store.all_events())
    explanation = graph.explain_alert_resolution(opened.aggregate_id)
    print(f"解除依据可从汇总数字还原：{explanation['resolvable_from_aggregates']}")
    for node in explanation["source_snapshots"]:
        print(f"  上游快照：{node.label}，{node.detail}")

    print()
    print("-" * 72)
    print("日报逐项核对")
    print("-" * 72)
    day = clock.at(2026, 9, 24, 12)
    start, end = day_bounds(day)
    windows = [w for w in proj.metric_windows(AQUATIC, "fish", "A") if w.start < end]
    items = build_report_items(
        windows, {(AQUATIC, "fish", "A"): [incident.incident_id]})
    # 将 23 日窗口归入 23 日日报演示
    start23, end23 = day_bounds(clock.at(2026, 9, 23, 12))
    windows23 = [w for w in proj.metric_windows(AQUATIC, "fish", "A")
                 if start23 <= w.start < end23]
    items23 = build_report_items(
        windows23, {(AQUATIC, "fish", "A"): [incident.incident_id]})
    ev = DailyReport.publish_event(
        "daily:2026-09-23", "2026-09-23", items23, ROLE_AUDITOR,
        clock.at(2026, 9, 23, 23, 59),
        recorded_at=clock.at(2026, 9, 23, 23, 59))
    svc.store.append(ev)
    report = DailyReport.reconcile(svc.store.all_events(), "daily:2026-09-23")
    print(f"核对结论：{'通过' if report['reconciled'] else '存在差异'}，"
          f"检查 {report['items_checked']} 个条目，发现 {len(report['findings'])} 项问题")
    for item in items23:
        print(f"  水产/fish/A：平均覆盖 {item.avg_coverage_days} 天，"
              f"最低 {item.min_coverage_days} 天，质量标记 {item.quality_flags}，"
              f"处置单 {item.incident_ids}")

    print()
    print("-" * 72)
    print("统一时间线（业务时间序）")
    print("-" * 72)
    for entry in proj.timeline():
        late = " [迟到]" if entry.late else ""
        print(f"{entry.occurred_at:%m-%d %H:%M} {entry.event_type:24s} "
              f"{entry.aggregate_id}{late}")

    print()
    print(f"事件总数：{svc.store.count()}（全部只追加，无覆盖、无删除）")


if __name__ == "__main__":
    main()
