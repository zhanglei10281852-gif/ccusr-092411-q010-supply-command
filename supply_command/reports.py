"""日报与核对。

日报是对一天窗口指标的汇总发布，但每条汇总数字都带 ``lineage_refs``
（窗口指标编号 + 处置单编号），因此任何汇总都能逐项还原，
不会再出现"夜班日报说库存充足，却无法还原解除依据"的情况。

核对（reconciliation）检查：
1. 日报每个品类条目的数值与其引用窗口逐一吻合；
2. 当日所有处置单都被日报引用，且处置单依据窗口存在；
3. 被事后修正（amended/corrected）的处置单在日报中标注修正；
4. 日报发布后到达的迟到数据若推翻条目，给出差异但不改写已发布日报
   （日报可发布新版本，旧版本保留）。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from . import clock
from .events import (
    Event,
    WINDOW_CALCULATED,
    WINDOW_RECALCULATED,
    WINDOW_MISSING,
    INCIDENT_ASSIGNED,
    INCIDENT_AMENDED,
    ALERT_CORRECTED,
    DAILY_REPORT_PUBLISHED,
)
from .windows import MetricWindow
from .catalog import ACTION_ROLES
from .errors import AuthorizationError, ValidationError


@dataclass
class ReportItem:
    business_line: str
    category: str
    region: str
    avg_coverage_days: float
    min_coverage_days: float
    end_stock: float
    max_level: str            # 当日最严重等级
    quality_flags: list[str]  # 当日出现过的质量标记（去重）
    incident_ids: list[str]
    window_refs: list[str]

    def to_payload(self) -> dict[str, Any]:
        return {
            "business_line": self.business_line,
            "category": self.category,
            "region": self.region,
            "avg_coverage_days": round(self.avg_coverage_days, 4),
            "min_coverage_days": round(self.min_coverage_days, 4),
            "end_stock": self.end_stock,
            "max_level": self.max_level,
            "quality_flags": list(self.quality_flags),
            "incident_ids": list(self.incident_ids),
            "window_refs": list(self.window_refs),
        }

    @classmethod
    def from_payload(cls, p: dict[str, Any]) -> "ReportItem":
        return cls(
            business_line=p["business_line"],
            category=p["category"],
            region=p["region"],
            avg_coverage_days=p["avg_coverage_days"],
            min_coverage_days=p["min_coverage_days"],
            end_stock=p["end_stock"],
            max_level=p["max_level"],
            quality_flags=list(p.get("quality_flags", [])),
            incident_ids=list(p.get("incident_ids", [])),
            window_refs=list(p.get("window_refs", [])),
        )


def day_bounds(day: datetime) -> tuple[datetime, datetime]:
    day = clock.parse(day)
    start = day.replace(hour=0, minute=0, second=0, microsecond=0)
    return start, start + timedelta(days=1)


def build_report_items(
    windows: list[MetricWindow],
    incidents_by_key: dict[tuple[str, str, str], list[str]],
) -> list[ReportItem]:
    """从窗口（取每个窗口的最高 revision）汇总日报条目。"""
    latest: dict[str, MetricWindow] = {}
    for w in windows:
        if w.aggregate_id not in latest or w.revision >= latest[w.aggregate_id].revision:
            latest[w.aggregate_id] = w

    grouped: dict[tuple[str, str, str], list[MetricWindow]] = {}
    for w in latest.values():
        grouped.setdefault((w.business_line, w.category, w.region), []).append(w)

    items: list[ReportItem] = []
    level_rank = {"none": 0, "warn": 1, "critical": 2}
    for (bl, cat, region), ws in sorted(grouped.items()):
        ws.sort(key=lambda w: w.start)
        finite_cov = [w.coverage_days for w in ws if w.coverage_days != float("inf")]
        items.append(
            ReportItem(
                business_line=bl,
                category=cat,
                region=region,
                avg_coverage_days=sum(finite_cov) / len(finite_cov) if finite_cov else float("inf"),
                min_coverage_days=min((w.coverage_days for w in ws), default=0.0),
                end_stock=ws[-1].stock_end,
                max_level=max((w.level for w in ws), key=lambda lv: level_rank.get(lv, 0)),
                quality_flags=sorted({w.quality for w in ws}),
                incident_ids=list(incidents_by_key.get((bl, cat, region), [])),
                window_refs=[w.aggregate_id for w in ws],
            )
        )
    return items


class DailyReport:
    @staticmethod
    def publish_event(
        report_id: str,
        report_date: str,
        items: list[ReportItem],
        publisher_role: str,
        occurred_at: datetime,
        revision: int = 1,
        recorded_at: datetime | None = None,
    ) -> Event:
        if publisher_role not in ACTION_ROLES["reconcile"] | ACTION_ROLES["report"]:
            raise AuthorizationError(f"角色无权发布日报")
        if not items:
            raise ValidationError("日报不能为空")
        lineage_refs: list[str] = []
        for item in items:
            lineage_refs.extend(item.window_refs)
            lineage_refs.extend(item.incident_ids)
        return Event(
            event_id=f"evt-daily-report-{report_id}",
            event_type=DAILY_REPORT_PUBLISHED,
            aggregate_id=report_id,
            occurred_at=occurred_at,
            payload={
                "report_date": report_date,
                "revision": revision,
                "items": [item.to_payload() for item in items],
                "lineage_refs": sorted(set(lineage_refs)),
            },
            actor=None,
            recorded_at=recorded_at or occurred_at,
        )

    @staticmethod
    def reconcile(events: list[Event], report_id: str) -> dict[str, Any]:
        """逐项核对已发布日报与窗口、处置单。"""
        report_events = [
            e for e in events
            if e.event_type == DAILY_REPORT_PUBLISHED and e.aggregate_id == report_id
        ]
        if not report_events:
            raise ValidationError(f"日报不存在：{report_id}")
        report_event = report_events[-1]

        # 已发布窗口（最高 revision）
        windows: dict[str, MetricWindow] = {}
        for e in events:
            if e.event_type in (WINDOW_CALCULATED, WINDOW_RECALCULATED, WINDOW_MISSING):
                w = MetricWindow.from_payload(e.payload)
                if w.aggregate_id not in windows or w.revision >= windows[w.aggregate_id].revision:
                    windows[w.aggregate_id] = w

        # 处置单与其修正状态
        incidents: dict[str, Event] = {}
        amended: set[str] = set()
        for e in events:
            if e.event_type == INCIDENT_ASSIGNED:
                incidents[e.aggregate_id] = e
            elif e.event_type == INCIDENT_AMENDED:
                amended.add(e.aggregate_id)

        findings: list[dict[str, str]] = []
        referenced_windows: set[str] = set()
        referenced_incidents: set[str] = set()

        for raw_item in report_event.payload["items"]:
            item = ReportItem.from_payload(raw_item)
            ws = [windows[r] for r in item.window_refs if r in windows]
            missing_refs = [r for r in item.window_refs if r not in windows]
            referenced_windows.update(item.window_refs)
            referenced_incidents.update(item.incident_ids)

            if missing_refs:
                findings.append({
                    "severity": "error",
                    "type": "dangling_window_ref",
                    "item": f"{item.category}@{item.region}",
                    "detail": f"日报引用了不存在的窗口：{missing_refs}",
                })
                continue

            # 数值逐项核对（日报按 4 位小数发布，比较时取相同精度）
            finite_cov = [w.coverage_days for w in ws if w.coverage_days != float("inf")]
            actual_avg = sum(finite_cov) / len(finite_cov) if finite_cov else float("inf")
            actual_end = ws[-1].stock_end
            if round(actual_avg, 4) != round(item.avg_coverage_days, 4):
                findings.append({
                    "severity": "error",
                    "type": "coverage_mismatch",
                    "item": f"{item.category}@{item.region}",
                    "detail": f"日报 {item.avg_coverage_days} != 窗口重算 {round(actual_avg, 4)}",
                })
            if abs(actual_end - item.end_stock) > 1e-6:
                findings.append({
                    "severity": "error",
                    "type": "stock_mismatch",
                    "item": f"{item.category}@{item.region}",
                    "detail": f"日报期末库存 {item.end_stock} != 窗口重算 {actual_end}",
                })
            for inc_id in item.incident_ids:
                if inc_id not in incidents:
                    findings.append({
                        "severity": "error",
                        "type": "dangling_incident_ref",
                        "item": f"{item.category}@{item.region}",
                        "detail": f"引用不存在的处置单：{inc_id}",
                    })
                elif inc_id in amended and "anomaly" not in item.quality_flags:
                    findings.append({
                        "severity": "warn",
                        "type": "amendment_not_flagged",
                        "item": f"{item.category}@{item.region}",
                        "detail": f"处置单 {inc_id} 已事后修正，日报未标注",
                    })

        # 当日处置单是否全部被日报覆盖
        report_date = report_event.payload["report_date"]
        for inc_id, ev in incidents.items():
            if ev.occurred_at.date().isoformat() != report_date:
                continue
            if inc_id not in referenced_incidents:
                findings.append({
                    "severity": "error",
                    "type": "incident_missing_from_report",
                    "item": inc_id,
                    "detail": "当日处置单未出现在日报中",
                })

        return {
            "report_id": report_id,
            "revision": report_event.payload["revision"],
            "items_checked": len(report_event.payload["items"]),
            "findings": findings,
            "reconciled": not any(f["severity"] == "error" for f in findings),
        }
