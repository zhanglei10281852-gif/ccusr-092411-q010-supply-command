"""HTTP API 集成测试：服务与单线程 HTTP 服务器同在一个后台线程。"""
from __future__ import annotations

import json
import threading
import unittest
import urllib.request
from http.server import HTTPServer

from app.api import Api, build_handler


class ApiTest(unittest.TestCase):
    def setUp(self) -> None:
        self.ready = threading.Event()

        def run() -> None:
            self.api = Api(":memory:")
            self.server = HTTPServer(("127.0.0.1", 0), build_handler(self.api))
            self.port = self.server.server_address[1]
            self.ready.set()
            self.server.serve_forever()
            self.api.close()

        self.thread = threading.Thread(target=run, daemon=True)
        self.thread.start()
        self.ready.wait(2)

    def tearDown(self) -> None:
        self.server.shutdown()
        self.thread.join(timeout=2)

    def call(self, method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
        data = json.dumps(body).encode("utf-8") if body is not None else None
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
            headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request) as response:
                return response.status, json.loads(response.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())

    def test_snapshot_to_window_flow(self) -> None:
        self.assertEqual(self.call("POST", "/operators",
                                   {"operator_id": "o1", "name": "值班", "role": "operator"})[0], 200)
        self.assertEqual(self.call("POST", "/operators",
                                   {"operator_id": "c1", "name": "指挥", "role": "commander"})[0], 200)
        status, body = self.call("POST", "/snapshots", {
            "business_line": "aquatic", "region": "east", "category": "fish",
            "version": 1, "stock_qty": 5, "safety_stock": 20,
            "daily_throughput": 20, "capacity": 100,
            "occurred_at": "2026-09-23T00:00:00+08:00", "idempotency_key": "k1",
            "at": "2026-09-23T00:05:00+08:00"})
        self.assertEqual(status, 200)
        self.assertFalse(body["late"])
        status, body = self.call("POST", "/windows/compute", {
            "business_line": "aquatic", "region": "east", "category": "fish",
            "window_start": "2026-09-23T00:00:00+08:00",
            "window_end": "2026-09-23T01:00:00+08:00",
            "at": "2026-09-23T00:05:00+08:00"})
        self.assertEqual(body["alert_level"], "critical")
        status, timeline = self.call("GET", "/timeline")
        self.assertTrue(any(e["event_type"] == "window.calculated" for e in timeline))

    def test_self_review_rejected_with_409(self) -> None:
        self.call("POST", "/operators", {"operator_id": "c1", "name": "指挥",
                                         "role": "commander"})
        self.call("POST", "/alerts/open", {
            "business_line": "fruit", "region": "east", "category": "apple",
            "kind": "low_coverage", "level": "warning"})
        status, body = self.call("GET", "/timeline")
        alert_id = next(e["aggregate_id"] for e in body
                        if e["event_type"] == "alert.opened")
        status, body = self.call("POST", f"/alerts/{alert_id}/suppress",
                                 {"actor": "c1", "reviewer": "c1", "reason": "x"})
        self.assertEqual(status, 409)
        self.assertIn("同一人", body["error"])


if __name__ == "__main__":
    unittest.main()
