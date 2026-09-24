"""HTTP API：标准库 http.server 包装，零第三方依赖。

服务以内存事件存储为默认后端（也可传入文件路径持久化）。
所有命令在 JSON body 中携带 actor（user_id/role/shift）；
需要双人复核的动作携带 reviewer。

路由：
  POST /v1/rules                     发布规则版本
  POST /v1/snapshots                 上报快照（幂等）
  POST /v1/windows/close             结算窗口
  POST /v1/recover                   恢复后补算
  GET  /v1/timeline                  统一时间线（双时态过滤）
  GET  /v1/windows                   窗口指标
  POST /v1/alerts                    上报/打开告警（自动去重）
  POST /v1/alerts/merge              合并重复告警
  POST /v1/alerts/escalate           升级（双人复核）
  POST /v1/alerts/transfer           转交（唯一责任链）
  POST /v1/alerts/suppress           抑制（双人复核）
  POST /v1/alerts/resolve            解除（双人复核+窗口依据）
  POST /v1/alerts/correct            迟到数据事后校正
  POST /v1/incidents/dispatch        派单（幂等）
  POST /v1/incidents/resolve         办结
  POST /v1/reports                   发布日报
  GET  /v1/reports/{id}/reconcile    日报逐项核对
  POST /v1/whatif                    沙盘推演（不写回）
  GET  /v1/lineage                   血缘追溯 ?node=...
"""
from __future__ import annotations

import json
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.parse import urlparse, parse_qs

from . import clock
from .app import SupplyCommandService, actor_from
from .alerts import Approval
from .catalog import Catalog, CategoryRule
from .errors import SupplyError
from .lineage import LineageGraph
from .reports import DailyReport, build_report_items, day_bounds
from .snapshots import Snapshot
from .store import EventStore
from .whatif import Scenario, WhatIfSandbox


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, float) and value == float("inf"):
        return "inf"
    return value


class ApiContext:
    def __init__(self, db_path: str = ":memory:") -> None:
        self.store = EventStore(db_path)
        self.catalog = Catalog()
        self.service = SupplyCommandService(self.store, self.catalog)
        self.lock = threading.RLock()


def create_app(ctx: ApiContext):
    service = ctx.service

    def approval(body: dict[str, Any]):
        from .alerts import Approval

        return Approval(actor_from(body["actor"]), actor_from(body["reviewer"]))
    def snapshot_from(body: dict[str, Any]) -> Snapshot:
        return Snapshot(
            business_line=body["business_line"],
            category=body["category"],
            region=body["region"],
            observed_at=body["observed_at"],
            stock=float(body["stock"]),
            inbound=float(body.get("inbound", 0.0)),
            outbound=float(body.get("outbound", 0.0)),
            version=int(body.get("version", 1)),
            source=body.get("source", "unknown"),
            batch=body.get("batch"),
            quality=body.get("quality", "ok"),
            note=body.get("note", ""),
            event_id=body.get("event_id", ""),
            recorded_at=clock.parse(body["recorded_at"]) if body.get("recorded_at") else None,
        )

    def rule_from(body: dict[str, Any]) -> CategoryRule:
        return CategoryRule(
            business_line=body["business_line"],
            category=body["category"],
            region=body["region"],
            safe_stock=float(body["safe_stock"]),
            warn_coverage_days=float(body["warn_coverage_days"]),
            critical_coverage_days=float(body["critical_coverage_days"]),
            throughput_per_hour=float(body.get("throughput_per_hour", 1.0)),
            max_late_seconds=int(body.get("max_late_seconds", 3600)),
            max_gap_seconds=int(body.get("max_gap_seconds", 1800)),
            valid_from=clock.parse(body["valid_from"]) if body.get("valid_from") else None,
            version=int(body.get("version", 1)),
        )

    class Handler(BaseHTTPRequestHandler):
        server_version = "SupplyCommand/1.0"

        def log_message(self, fmt: str, *args: Any) -> None:
            return

        def _send(self, status: int, payload: Any) -> None:
            data = json.dumps(_jsonable(payload), ensure_ascii=False, default=str).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _read_body(self) -> dict[str, Any]:
            length = int(self.headers.get("Content-Length", 0))
            if length == 0:
                return {}
            raw = self.rfile.read(length)
            return json.loads(raw.decode("utf-8"))

        def _handle(self, fn: Callable[[], Any]) -> None:
            try:
                with ctx.lock:
                    result = fn()
                self._send(200, result if result is not None else {"ok": True})
            except SupplyError as exc:
                code = 409 if "Duplicate" in type(exc).__name__ else 400
                self._send(code, {"error": type(exc).__name__, "message": str(exc)})
            except (KeyError, ValueError) as exc:
                self._send(400, {"error": "BadRequest", "message": str(exc)})

        # ---- 路由 ----
        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            path = parsed.path
            query = parse_qs(parsed.query)
            if path == "/v1/timeline":
                self._handle(lambda: self._timeline(query))
            elif path == "/v1/windows":
                self._handle(lambda: self._windows(query))
            elif path.startswith("/v1/reports/") and path.endswith("/reconcile"):
                report_id = path.split("/")[3]
                self._handle(lambda: self._reconcile(report_id))
            elif path == "/v1/lineage":
                self._handle(lambda: self._lineage(query))
            elif path == "/v1/health":
                self._send(200, {"status": "ok", "events": ctx.store.count()})
            else:
                self._send(404, {"error": "NotFound", "message": path})

        def do_POST(self) -> None:
            path = urlparse(self.path).path
            routes = {
                "/v1/rules": self._post_rule,
                "/v1/snapshots": self._post_snapshot,
                "/v1/windows/close": self._post_close_window,
                "/v1/recover": self._post_recover,
                "/v1/alerts": self._post_alert,
                "/v1/alerts/merge": self._post_merge,
                "/v1/alerts/escalate": self._post_escalate,
                "/v1/alerts/transfer": self._post_transfer,
                "/v1/alerts/suppress": self._post_suppress,
                "/v1/alerts/resolve": self._post_resolve,
                "/v1/alerts/correct": self._post_correct,
                "/v1/incidents/dispatch": self._post_dispatch,
                "/v1/incidents/resolve": self._post_incident_resolve,
                "/v1/reports": self._post_report,
                "/v1/whatif": self._post_whatif,
            }
            handler = routes.get(path)
            if handler is None:
                self._send(404, {"error": "NotFound", "message": path})
            else:
                self._handle(handler)

        # ---- 实现 ----
        def _post_rule(self) -> Any:
            body = self._read_body()
            service.publish_rule(rule_from(body))
            return {"ok": True}

        def _post_snapshot(self) -> Any:
            body = self._read_body()
            result = service.ingest(snapshot_from(body))
            return {
                "event_id": result.event_id,
                "duplicate": result.duplicate,
                "late": result.late,
                "recalculated_windows": result.recalculated_windows,
            }

        def _post_close_window(self) -> Any:
            body = self._read_body()
            event = service.close_window(
                body["business_line"], body["category"], body["region"],
                clock.parse(body["start"]),
                minutes=int(body.get("minutes", 30)),
                evaluation_at=clock.parse(body["evaluation_at"]) if body.get("evaluation_at") else None,
            )
            return {"event_id": event.event_id, "window": event.payload}

        def _post_recover(self) -> Any:
            body = self._read_body()
            return service.recover(
                clock.parse(body["until"]), minutes=int(body.get("minutes", 30))
            )

        def _timeline(self, query: dict[str, list[str]]) -> Any:
            def q(name: str) -> str | None:
                return query[name][0] if name in query else None

            proj = service.projection()
            entries = proj.timeline(
                start=clock.parse(q("start")) if q("start") else None,
                end=clock.parse(q("end")) if q("end") else None,
                business_line=q("business_line"),
                category=q("category"),
                region=q("region"),
            )
            return [
                {
                    "occurred_at": e.occurred_at.isoformat(),
                    "recorded_at": e.recorded_at.isoformat(),
                    "event_type": e.event_type,
                    "aggregate_id": e.aggregate_id,
                    "event_id": e.event_id,
                    "late": e.late,
                }
                for e in entries
            ]

        def _windows(self, query: dict[str, list[str]]) -> Any:
            bl = query["business_line"][0]
            cat = query["category"][0]
            region = query["region"][0]
            proj = service.projection()
            ws = proj.metric_windows(
                bl, cat, region,
                include_revisions=query.get("revisions", ["1"])[0] != "0",
            )
            return [w.to_payload() for w in ws]

        def _post_alert(self) -> Any:
            body = self._read_body()
            event, created = service.report_alert(
                body["business_line"], body["category"], body["region"],
                body.get("level", "warn"), body.get("window_ids", []),
                approval(body),
                occurred_at=clock.parse(body["at"]) if body.get("at") else None,
                initial_owner=body.get("initial_owner", ""),
            )
            return {"event_id": event.event_id, "alert_id": event.aggregate_id, "created": created}

        def _post_merge(self) -> Any:
            body = self._read_body()
            event = service.merge_alerts(
                body["child_alert_id"], body["parent_alert_id"], approval(body),
                at=clock.parse(body["at"]) if body.get("at") else None,
                duplicate_window_id=body.get("duplicate_window_id", ""),
            )
            return {"event_id": event.event_id}

        def _post_escalate(self) -> Any:
            body = self._read_body()
            event = service.escalate_alert(
                body["alert_id"], approval(body), body["reason"],
                body["new_owner"],
                at=clock.parse(body["at"]) if body.get("at") else None,
            )
            return {"event_id": event.event_id}

        def _post_transfer(self) -> Any:
            body = self._read_body()
            # 转交不属于双人复核动作，但 Approval 结构需要 reviewer 字段
            actor = actor_from(body["actor"])
            event = service.transfer_alert(
                body["alert_id"],
                Approval(actor, actor),
                body["to_owner"],
                at=clock.parse(body["at"]) if body.get("at") else None,
                reason=body.get("reason", ""),
            )
            return {"event_id": event.event_id}

        def _post_suppress(self) -> Any:
            body = self._read_body()
            event = service.suppress_alert(
                body["alert_id"], approval(body), clock.parse(body["until"]),
                body["reason"], at=clock.parse(body["at"]) if body.get("at") else None,
            )
            return {"event_id": event.event_id}

        def _post_resolve(self) -> Any:
            body = self._read_body()
            event = service.resolve_alert(
                body["alert_id"], approval(body), body["basis_window_ids"],
                at=clock.parse(body["at"]) if body.get("at") else None,
            )
            return {"event_id": event.event_id}

        def _post_correct(self) -> Any:
            body = self._read_body()
            events = service.correct_after_recalc(
                body["alert_id"], body["recalc_event_id"], actor_from(body["reviewer"]),
                body.get("note", ""),
                at=clock.parse(body["at"]) if body.get("at") else None,
            )
            return {"event_ids": [e.event_id for e in events]}

        def _post_dispatch(self) -> Any:
            body = self._read_body()
            event, created = service.dispatch_incident(
                body["alert_id"], body["owner"], body["decision"],
                body.get("rationale", ""), actor_from(body["actor"]),
                at=clock.parse(body["at"]) if body.get("at") else None,
            )
            return {"event_id": event.event_id, "incident_id": event.aggregate_id, "created": created}

        def _post_incident_resolve(self) -> Any:
            body = self._read_body()
            event = service.resolve_incident(
                body["incident_id"], actor_from(body["actor"]), body.get("note", ""),
                at=clock.parse(body["at"]) if body.get("at") else None,
            )
            return {"event_id": event.event_id}

        def _post_report(self) -> Any:
            body = self._read_body()
            day = body["report_date"]
            start, end = day_bounds(clock.parse(day + "T00:00:00+08:00"))
            # 汇总当日全部窗口（最高 revision）
            keys = {
                (e.payload["business_line"], e.payload["category"], e.payload["region"])
                for e in ctx.store.all_events()
                if e.event_type in ("window.calculated", "window.recalculated", "window.missing")
                and start <= e.occurred_at < end
            }
            proj = service.projection()
            windows = []
            for bl, cat, region in sorted(keys):
                windows.extend(proj.metric_windows(bl, cat, region, include_revisions=False))
            windows = [w for w in windows if start <= w.start < end]
            incidents_by_key: dict[tuple[str, str, str], list[str]] = {}
            for inc in service.list_incidents():
                if inc.original and start <= inc.original.decided_at < end:
                    incidents_by_key.setdefault(
                        (inc.business_line, inc.category, inc.region), []
                    ).append(inc.incident_id)
            items = build_report_items(windows, incidents_by_key)
            report_id = f"daily:{day}"
            event = DailyReport.publish_event(
                report_id, day, items, body["actor"]["role"],
                clock.parse(body.get("published_at", day + "T23:59:00+08:00")),
                revision=int(body.get("revision", 1)),
            )
            ctx.store.append(event)
            return {"event_id": event.event_id, "report_id": report_id,
                    "items": len(items)}

        def _reconcile(self, report_id: str) -> Any:
            return DailyReport.reconcile(ctx.store.all_events(), report_id)

        def _post_whatif(self) -> Any:
            body = self._read_body()
            scenario = Scenario(
                name=body["name"],
                capacity_delta=float(body.get("capacity_delta", 0.0)),
                inbound_multiplier=float(body.get("inbound_multiplier", 1.0)),
                inbound_extra=float(body.get("inbound_extra", 0.0)),
                outbound_multiplier=float(body.get("outbound_multiplier", 1.0)),
                affected_categories=body.get("affected_categories"),
                start=clock.parse(body["start"]) if body.get("start") else None,
                end=clock.parse(body["end"]) if body.get("end") else None,
            )
            result = WhatIfSandbox(ctx.store.all_events(), ctx.catalog).run(
                scenario, minutes=int(body.get("minutes", 30))
            )
            return {
                "scenario": result.scenario,
                "diff": result.diff(),
                "projected": [w.to_payload() for w in result.projected],
            }

        def _lineage(self, query: dict[str, list[str]]) -> Any:
            node = query["node"][0]
            graph = LineageGraph(ctx.store.all_events())
            result = graph.trace(node)
            return {
                "target": result["target"].__dict__ if result["target"] else None,
                "upstream": [n.__dict__ for n in result["upstream"]],
                "fully_resolvable": result["fully_resolvable"],
            }

    return Handler


def run(host: str = "127.0.0.1", port: int = 8080, db_path: str = ":memory:") -> ApiContext:
    ctx = ApiContext(db_path)
    server = ThreadingHTTPServer((host, port), create_app(ctx))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    ctx.server = server  # type: ignore[attr-defined]
    return ctx
