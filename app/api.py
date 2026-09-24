"""标准库 HTTP JSON API：不依赖第三方框架即可启动的保供指挥后端。

启动：python3 -m app.api --db command.db --port 8000
状态存于 SQLite 文件；每个请求使用同一个 CommandService。
"""
from __future__ import annotations

import json
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse

from .clock import Clock
from .service import CommandService, DomainError
from .store import Store


class Api:
    def __init__(self, db_path: str = ":memory:") -> None:
        self.clock = Clock()
        self.store = Store(db_path)
        self.service = CommandService(self.store, self.clock)

    def close(self) -> None:
        self.store.close()


def build_handler(api: Api) -> type[BaseHTTPRequestHandler]:
    service = api.service

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args: object) -> None:
            return

        def _send(self, status: int, body: object) -> None:
            data = json.dumps(body, ensure_ascii=False, default=str).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _read(self) -> dict:
            length = int(self.headers.get("Content-Length", 0))
            if length == 0:
                return {}
            return json.loads(self.rfile.read(length).decode("utf-8"))

        def do_GET(self) -> None:  # noqa: N802
            path = urlparse(self.path).path.strip("/").split("/")
            try:
                if path[:1] == ["timeline"]:
                    return self._send(200, service.timeline())
                if path[:2] == ["windows", "lineage"] and len(path) == 3:
                    return self._send(200, service.window_lineage(path[2]))
                if path[:1] == ["reports"] and len(path) == 3 and path[2] == "reconcile":
                    return self._send(200, service.reconcile_report(path[1]))
                if path == ["dispatches", "reconcile"]:
                    return self._send(200, service.reconcile_dispatches())
                self._send(404, {"error": "未知路径"})
            except DomainError as exc:
                self._send(409, {"error": str(exc)})
            except Exception as exc:  # noqa: BLE001
                self._send(500, {"error": str(exc)})

        def do_POST(self) -> None:  # noqa: N802
            path = urlparse(self.path).path.strip("/").split("/")
            body = self._read()
            if "at" in body:
                api.clock.freeze(body.pop("at"))
            try:
                result = self._route(path, body)
                self._send(200, result if result is not None else {"ok": True})
            except DomainError as exc:
                self._send(409, {"error": str(exc)})
            except TypeError as exc:
                self._send(400, {"error": f"参数错误：{exc}"})
            except Exception as exc:  # noqa: BLE001
                self._send(500, {"error": str(exc)})

        def _route(self, path: list[str], b: dict) -> object:
            s = service
            if path == ["operators"]:
                s.register_operator(b["operator_id"], b["name"], b["role"])
                return None
            if path == ["snapshots"]:
                return s.ingest_snapshot(**b)
            if path == ["business-events"]:
                return s.ingest_business_event(**b)
            if path[:1] == ["rules"]:
                return s.rules.publish(f"thresholds:{b.pop('business_line')}"
                                       if "business_line" in b else "thresholds:default", b)
            if path == ["windows", "compute"]:
                return s.compute_window(**b)
            if path == ["alerts", "open"]:
                return s.open_alert(**b)
            if path[:1] == ["alerts"] and len(path) == 3:
                action = path[2]
                handlers = {
                    "merge": lambda: s.merge_alerts(b["source_id"], b["target_id"],
                                                    b["actor"], b["reviewer"], b["reason"]),
                    "escalate": lambda: s.escalate_alert(path[1], b["actor"],
                                                         b["reviewer"], b["reason"]),
                    "suppress": lambda: s.suppress_alert(path[1], b["actor"],
                                                         b["reviewer"], b["reason"]),
                    "resolve": lambda: s.resolve_alert(path[1], b["actor"],
                                                       b["reviewer"], b["reason"]),
                    "assign": lambda: s.assign_alert(path[1], b["owner"], b["actor"],
                                                     b["reviewer"], b["reason"]),
                }
                if action not in handlers:
                    return self._send(404, {"error": "未知动作"})
                return handlers[action]()
            if path[:1] == ["incidents"] and len(path) == 3 and path[2] == "transfer":
                s.transfer_incident(path[1], b["to_owner"], b["actor"],
                                    b["reviewer"], b["reason"])
                return None
            if path[:1] == ["shifts"] and len(path) == 3:
                if path[2] == "open":
                    s.open_shift(path[1], b["operator"])
                elif path[2] == "handover":
                    s.handover_shift(path[1], b["from_operator"], b["to_operator"],
                                     b.get("note", ""))
                else:
                    return self._send(404, {"error": "未知班次动作"})
                return None
            if path == ["reports"]:
                return s.publish_daily_report(b["report_id"], b.get("shift_id"),
                                              b.get("as_of"))
            if path == ["recovery"]:
                return s.recover_windows(**b)
            if path == ["simulate"]:
                return s.simulate(b["mutations"], b["windows"])
            return self._send(404, {"error": "未知路径"})

    return Handler


def serve(db_path: str, port: int) -> None:
    api = Api(db_path)
    server = HTTPServer(("0.0.0.0", port), build_handler(api))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        api.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="多业态保供指挥后端")
    parser.add_argument("--db", default="command.db")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    serve(args.db, args.port)
