"""可运行情景演示：夜班日报"库存充足" vs 早班发现水产连续数小时低于安全线。

运行：python3 tools/demo.py
不依赖任何第三方库，全部走 CommandService。
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.clock import Clock, parse  # noqa: E402
from app.service import CommandService  # noqa: E402
from app.store import Store  # noqa: E402

LINE = "　"


def heading(title: str) -> None:
    print(f"\n{'=' * 72}\n{title}\n{'=' * 72}")


def main() -> None:
    clock = Clock("2026-09-23T02:00:00+08:00")
    svc = CommandService(Store(":memory:"), clock)
    svc.register_operator("op-night", "夜班值班员", "operator")
    svc.register_operator("cmd-night", "夜班指挥长", "commander")
    svc.register_operator("op-day", "早班值班员", "operator")
    svc.register_operator("cmd-day", "早班指挥长", "commander")
    svc.open_shift("night-0923", "op-night")
    # 水产要求 30 分钟内快照
    svc.rules.publish("thresholds:aquatic",
                      {"snapshot_max_age_seconds": 1800, "late_threshold_seconds": 1800})

    heading("① 02:00 四业态快照入库，库存全部充足")
    catalog = [("fruit", "fruit", "果蔬"), ("grain", "rice", "粮油"),
               ("aquatic", "fish", "水产"), ("snack", "nuts", "休闲食品")]
    for line, cat, label in catalog:
        svc.ingest_snapshot(line, "east", cat, 1, 100, 20, 20, 100,
                            "2026-09-23T02:00:00+08:00", idempotency_key=f"snap-{cat}-1")
        w = svc.compute_window(line, "east", cat,
                               "2026-09-23T02:00:00+08:00", "2026-09-23T03:00:00+08:00")
        print(f"{LINE}{label:<5} 覆盖天数={w['coverage_days']:<6} "
              f"数据={w['data_status']:<7} 等级={w['alert_level']}")

    heading("② 03:00-05:00 水产采集中断：04:00 窗口判定为【数据缺失】，非真实异常")
    clock.freeze("2026-09-23T03:00:00+08:00")
    for line, cat, label in catalog:
        if line == "aquatic":
            continue
        svc.ingest_snapshot(line, "east", cat, 2, 100, 20, 20, 100,
                            "2026-09-23T03:00:00+08:00", idempotency_key=f"snap-{cat}-2")
    clock.freeze("2026-09-23T04:00:00+08:00")
    w = svc.compute_window("aquatic", "east", "fish",
                           "2026-09-23T03:00:00+08:00", "2026-09-23T04:00:00+08:00")
    print(f"{LINE}03:00 窗口：数据状态={w['data_status']}，覆盖天数={w['coverage_days']}"
          f"（不臆断供应异常）")
    missing = svc.store.query_one("SELECT alert_id FROM alerts WHERE kind='data_missing'")
    clock.freeze("2026-09-23T04:10:00+08:00")
    svc.assign_alert(missing["alert_id"], "op-night", "op-night", "cmd-night", "排查采集链路")
    clock.freeze("2026-09-23T04:40:00+08:00")
    svc.resolve_alert(missing["alert_id"], "op-night", "cmd-night",
                      "采集网关故障，未见库存异常，按数据缺失解除")
    print(f"{LINE}04:10 派单 → 04:40 双人复核解除，解除时判断已冻结")

    heading("③ 08:00 班次交接 + 夜班日报：汇总数字显示各品类充足")
    clock.freeze("2026-09-23T08:00:00+08:00")
    svc.handover_shift("night-0923", "op-night", "op-day", "夜班转早班")
    report = svc.publish_daily_report("report-2026-09-23-night", "night-0923")
    for item in report["items"]:
        print(f"{LINE}{item['business_line']:<8}{item['category']:<6}"
              f"覆盖={str(item['coverage_days']):<6} 数据={item['data_status']:<7}"
              f" 等级={item['alert_level']}")
    print(f"{LINE}（水产窗口因数据缺失无覆盖天数，汇总中被当作“无异常”）")

    heading("④ 08:31 上游恢复，积压快照迟到到达：水产连续数小时低于安全线")
    clock.freeze("2026-09-23T08:31:00+08:00")
    backlog = [(2, 8, "03:00"), (3, 8, "04:00"), (4, 7, "05:00"),
               (5, 6, "06:00"), (6, 5, "07:00")]
    for version, stock, hour in backlog:
        svc.ingest_snapshot(
            "aquatic", "east", "fish", version, stock, 20, 20, 100,
            f"2026-09-23T{hour}:00+08:00", idempotency_key=f"snap-fish-{version}")
    print(f"{LINE}迟到快照触发 03:00 窗口重算：missing → 真实 critical")
    rec = svc.recover_windows("aquatic", "east", "fish", [
        f"2026-09-23T0{h}:00:00+08:00" for h in range(3, 8)])
    for wid in rec["computed"]:
        row = svc.store.query_one("SELECT * FROM windows WHERE window_id=?", (wid,))
        print(f"{LINE}补算窗口 {row['window_start'][11:16]}  覆盖="
              f"{row['coverage_days']:<5} 等级={row['alert_level']}")

    heading("⑤ 处置单：原判断保留 + 追加修正；责任链唯一且不随班次改变")
    incident_id = f"incident-{missing['alert_id']}"
    for link in svc.responsibility_chain(incident_id):
        print(f"{LINE}责任链：{link['from_owner'] or '∅'} → {link['owner']} @ {link['at'][11:16]}"
              f"（{link['reason']}，复核 {link['reviewer']}）")
    events = svc.store.query(
        "SELECT event_type, actor, reason FROM incident_events WHERE incident_id=? ORDER BY id",
        (incident_id,))
    for e in events:
        print(f"{LINE}处置链节：{e['event_type']:<20} 操作={e['actor'] or '-':<9} {e['reason'] or ''}")
    amendment = svc.store.query_one("SELECT * FROM dispatch_amendments")
    print(f"{LINE}原处置单保留，修订追加：{amendment['reason']}")
    # 再次恢复不重复派单
    again = svc.recover_windows("aquatic", "east", "fish",
                                ["2026-09-23T04:00:00+08:00"])
    print(f"{LINE}重复恢复：新算窗口={len(again['computed'])}，"
          f"重复派单={len(again['duplicate_dispatches'])}（不新增）")

    heading("⑥ 追溯：指挥人员按 04:40 入库截止回放夜班解除依据")
    frozen = svc._compute_metrics(
        "aquatic", "east", "fish",
        parse("2026-09-23T03:00:00+08:00"), parse("2026-09-23T04:00:00+08:00"),
        parse("2026-09-23T04:40:00+08:00"))
    current = svc._compute_metrics(
        "aquatic", "east", "fish",
        parse("2026-09-23T03:00:00+08:00"), parse("2026-09-23T04:00:00+08:00"),
        parse("2026-09-23T09:00:00+08:00"))
    print(f"{LINE}解除时视图（recorded_at≤04:40）：{frozen['data_status']}，等级={frozen['alert_level']}")
    print(f"{LINE}当前视图（迟到校正后）　　　：{current['data_status']}，等级={current['alert_level']}")
    lineage = svc.window_lineage("aquatic|east|fish|2026-09-23T03:00:00+08:00")
    print(f"{LINE}指标血缘：{len(lineage['snapshots'])} 个快照版本，规则 "
          f"{lineage['rule'] is not None and 'thresholds:aquatic'}")

    heading("⑦ 隔离推演：若 04:30 到货 500 单位（主库不变）")
    sim = svc.simulate(
        [{"type": "snapshot", "params": dict(
            business_line="aquatic", region="east", category="fish", version=99,
            stock_qty=500, safety_stock=20, daily_throughput=20, capacity=300,
            occurred_at="2026-09-23T04:30:00+08:00", idempotency_key="sim-1")}],
        [{"business_line": "aquatic", "region": "east", "category": "fish",
          "window_start": "2026-09-23T04:00:00+08:00",
          "window_end": "2026-09-23T05:00:00+08:00"}])
    r = sim["results"][0]
    print(f"{LINE}推演结果：覆盖天数={r['coverage_days']}，吞吐压力={r['throughput_pressure']}"
          f"，等级={r['alert_level']}（隔离={sim['isolated']}）")

    heading("⑧ 日报与全部处置单逐项核对：按发布时 as_of/窗口版本留档复算")
    for item in svc.reconcile_report("report-2026-09-23-night"):
        mark = "✓" if item["ok"] else f"✗ {item['diffs']}"
        print(f"{LINE}日报 {item['key']}  {mark}")
    for item in svc.reconcile_dispatches():
        mark = "✓" if item["ok"] else f"✗ {item['diffs']}"
        print(f"{LINE}处置单 {item['dispatch_id']}（{item['incident_id']}，"
              f"责任人 {item['owner']}，修订 {item['amendments']} 份）  {mark}")
    print("\n演示完成。")


if __name__ == "__main__":
    main()
