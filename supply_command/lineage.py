"""数据血缘：任一指标都能逐级追到原始快照与来源系统。

链路：日报条目 ──▶ 处置单 ──▶ 告警 ──▶ 窗口指标（含规则版本）──▶ 快照事件 ──▶ 来源点位
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from .events import (
    Event,
    SNAPSHOT_RECEIVED,
    WINDOW_CALCULATED,
    WINDOW_RECALCULATED,
    WINDOW_MISSING,
    ALERT_OPENED,
    ALERT_RESOLVED,
    ALERT_CORRECTED,
    INCIDENT_ASSIGNED,
    INCIDENT_AMENDED,
    DAILY_REPORT_PUBLISHED,
)


@dataclass
class LineageNode:
    node_id: str
    kind: str                    # report | incident | alert | window | snapshot | source
    label: str
    event_id: str = ""
    recorded_at: datetime | None = None
    detail: dict[str, Any] = field(default_factory=dict)


@dataclass
class LineageEdge:
    parent: str   # 上游
    child: str    # 下游
    relation: str


class LineageGraph:
    def __init__(self, events: list[Event]) -> None:
        self.events = events
        self.nodes: dict[str, LineageNode] = {}
        self.edges: list[LineageEdge] = []
        self._build()

    def _add(self, node: LineageNode) -> None:
        self.nodes.setdefault(node.node_id, node)

    def _build(self) -> None:
        # ---- 第一遍：快照、来源、窗口节点，并建立引用别名 ----
        window_alias: dict[str, str] = {}   # 窗口聚合 id / 窗口事件 id -> 节点 id
        best_rev: dict[str, int] = {}
        for e in self.events:
            if e.event_type not in (WINDOW_CALCULATED, WINDOW_RECALCULATED, WINDOW_MISSING):
                continue
            rev = e.payload.get("revision", 1)
            if rev >= best_rev.get(e.aggregate_id, 0):
                best_rev[e.aggregate_id] = rev

        for e in self.events:
            if e.event_type == SNAPSHOT_RECEIVED:
                p = e.payload
                src_id = f"source:{p.get('source', 'unknown')}"
                self._add(LineageNode(src_id, "source", f"来源：{p.get('source', 'unknown')}"))
                snap_id = e.event_id
                self._add(
                    LineageNode(
                        snap_id,
                        "snapshot",
                        f"快照 v{p.get('version', 1)} @ {p['observed_at']}",
                        event_id=e.event_id,
                        recorded_at=e.recorded_at,
                        detail={"stock": p["stock"], "quality": p.get("quality")},
                    )
                )
                self.edges.append(LineageEdge(src_id, snap_id, "produces"))
            elif e.event_type in (WINDOW_CALCULATED, WINDOW_RECALCULATED, WINDOW_MISSING):
                p = e.payload
                node_id = f"{e.aggregate_id}#r{p.get('revision', 1)}"
                self._add(
                    LineageNode(
                        node_id,
                        "window",
                        f"窗口 rev{p.get('revision', 1)} [{p['quality']}]",
                        event_id=e.event_id,
                        recorded_at=e.recorded_at,
                        detail={
                            "coverage_days": p.get("coverage_days"),
                            "level": p.get("level"),
                            "rule_version": p.get("rule_version"),
                            "aggregate_id": e.aggregate_id,
                        },
                    )
                )
                for snap_id in p.get("basis_snapshot_ids", []):
                    self.edges.append(LineageEdge(snap_id, node_id, "computes"))
                # 最高版本承担聚合 id 与事件 id 两种引用
                if p.get("revision", 1) == best_rev.get(e.aggregate_id, 1):
                    window_alias[e.aggregate_id] = node_id
                window_alias[e.event_id] = node_id

        def resolve_window(ref: str) -> str:
            return window_alias.get(ref, ref)

        # ---- 第二遍：告警、处置单、日报 ----
        for e in self.events:
            if e.event_type == ALERT_OPENED:
                p = e.payload
                self._add(LineageNode(e.aggregate_id, "alert", f"告警 {e.aggregate_id}",
                                      event_id=e.event_id, detail={"level": p.get("level")}))
                for wid in p.get("window_ids", []):
                    self.edges.append(LineageEdge(resolve_window(wid), e.aggregate_id, "triggers"))
            elif e.event_type in (ALERT_RESOLVED, ALERT_CORRECTED):
                self._add(LineageNode(e.aggregate_id, "alert", f"告警 {e.aggregate_id}"))
                for wid in e.payload.get("basis_window_ids", []):
                    self.edges.append(
                        LineageEdge(resolve_window(wid), e.aggregate_id, "basis_of")
                    )

        for e in self.events:
            if e.event_type == INCIDENT_ASSIGNED:
                p = e.payload
                self._add(LineageNode(e.aggregate_id, "incident",
                                      f"处置单 {e.aggregate_id}", event_id=e.event_id,
                                      detail={"decision": p.get("decision")}))
                self.edges.append(LineageEdge(p["alert_id"], e.aggregate_id, "dispatches"))
                for wid in p.get("basis_window_ids", []):
                    self.edges.append(
                        LineageEdge(resolve_window(wid), e.aggregate_id, "basis_of")
                    )
            elif e.event_type == INCIDENT_AMENDED:
                self._add(LineageNode(e.aggregate_id, "incident",
                                      f"处置单 {e.aggregate_id}"))
                cause = e.payload.get("causation_event_id")
                if cause:
                    self.edges.append(
                        LineageEdge(resolve_window(cause), e.aggregate_id, "amends")
                    )

        for e in self.events:
            if e.event_type == DAILY_REPORT_PUBLISHED:
                p = e.payload
                self._add(LineageNode(e.aggregate_id, "report",
                                      f"日报 {p.get('report_date', e.aggregate_id)}",
                                      event_id=e.event_id))
                for ref in p.get("lineage_refs", []):
                    self.edges.append(
                        LineageEdge(resolve_window(ref), e.aggregate_id, "included_in")
                    )

    def trace(self, node_id: str) -> dict[str, Any]:
        """返回某节点的完整上游血缘（递归）。"""
        upstream: list[LineageNode] = []
        visited: set[str] = set()

        def walk(nid: str) -> None:
            for edge in self.edges:
                if edge.child == nid and edge.parent not in visited:
                    visited.add(edge.parent)
                    node = self.nodes.get(edge.parent)
                    if node:
                        upstream.append(node)
                    walk(edge.parent)

        walk(node_id)
        target = self.nodes.get(node_id)
        return {
            "target": target,
            "upstream": upstream,
            "fully_resolvable": all(
                n.kind != "window" or n.detail.get("rule_version") is not None
                for n in upstream
            ),
        }

    def explain_alert_resolution(self, alert_id: str) -> dict[str, Any]:
        """还原"上一班解除告警的依据"：沿 resolution_basis 追到快照。"""
        trace = self.trace(alert_id)
        windows = [n for n in trace["upstream"] if n.kind == "window"]
        snapshots = [n for n in trace["upstream"] if n.kind == "snapshot"]
        return {
            "alert_id": alert_id,
            "resolution_windows": windows,
            "source_snapshots": snapshots,
            "resolvable_from_aggregates": len(windows) > 0,
        }
