"""HTTP API 端到端冒烟测试（标准库服务器，零依赖）。"""
from __future__ import annotations

import json
import unittest
import urllib.request
import urllib.error

from supply_command import api


def post(url: str, body: dict) -> tuple[int, dict]:
    req = urllib.request.Request(
        url, data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def get(url: str) -> tuple[int, dict | list]:
    with urllib.request.urlopen(url, timeout=5) as resp:
        return resp.status, json.loads(resp.read().decode("utf-8"))


CMD = {"user_id": "cmdr-li", "role": "commander", "shift": "night"}
AUD = {"user_id": "auditor-wang", "role": "auditor", "shift": "night"}
OPS = {"user_id": "op-zhao", "role": "operator", "shift": "day"}
DISP = {"user_id": "disp-chen", "role": "dispatch", "shift": "night"}


class ApiTest(unittest.TestCase):
    def setUp(self) -> None:
        self.ctx = api.ApiContext(":memory:")
        import threading
        from http.server import ThreadingHTTPServer
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), api.create_app(self.ctx))
        self.port = self.server.server_address[1]
        self.base = f"http://127.0.0.1:{self.port}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def tearDown(self) -> None:
        self.server.shutdown()

    def _rule(self) -> None:
        post(f"{self.base}/v1/rules", {
            "business_line": "aquatic", "category": "fish", "region": "A",
            "safe_stock": 20.0, "warn_coverage_days": 2.0,
            "critical_coverage_days": 1.0, "throughput_per_hour": 5.0,
            "max_late_seconds": 1800, "valid_from": "2026-09-01T00:00:00+08:00",
        })

    def test_full_flow(self) -> None:
        self._rule()

        # 健康检查（规则发布已事件化，故此刻有 1 条 rule.published）
        status, body = get(f"{self.base}/v1/health")
        self.assertEqual(status, 200)
        self.assertEqual(body["events"], 1)

        # 上报快照
        status, body = post(f"{self.base}/v1/snapshots", {
            "business_line": "aquatic", "category": "fish", "region": "A",
            "observed_at": "2026-09-23T20:00:00+08:00",
            "recorded_at": "2026-09-23T20:05:00+08:00",
            "stock": 15.0, "outbound": 5.0, "source": "aqs-a01",
        })
        self.assertEqual(status, 200)
        self.assertFalse(body["duplicate"])

        # 重复上报 -> 幂等
        status, body = post(f"{self.base}/v1/snapshots", {
            "business_line": "aquatic", "category": "fish", "region": "A",
            "observed_at": "2026-09-23T20:00:00+08:00",
            "recorded_at": "2026-09-23T20:05:00+08:00",
            "stock": 15.0, "outbound": 5.0, "source": "aqs-a01",
        })
        self.assertTrue(body["duplicate"])

        # 结算窗口
        status, body = post(f"{self.base}/v1/windows/close", {
            "business_line": "aquatic", "category": "fish", "region": "A",
            "start": "2026-09-23T20:00:00+08:00",
            "evaluation_at": "2026-09-23T20:35:00+08:00",
        })
        self.assertEqual(status, 200)
        self.assertEqual(body["window"]["level"], "critical")
        from supply_command.windows import window_aggregate_id
        from supply_command import clock
        wid = window_aggregate_id("aquatic", "fish", "A",
                                  clock.parse("2026-09-23T20:00:00+08:00"))

        # 打开告警
        status, body = post(f"{self.base}/v1/alerts", {
            "business_line": "aquatic", "category": "fish", "region": "A",
            "level": "critical", "window_ids": [wid],
            "actor": CMD, "reviewer": AUD, "at": "2026-09-23T20:36:00+08:00",
        })
        self.assertEqual(status, 200)
        self.assertTrue(body["created"])
        alert_id = body["alert_id"]

        # 升级缺少合规复核人 -> 400
        status, body = post(f"{self.base}/v1/alerts/escalate", {
            "alert_id": alert_id, "reason": "x", "new_owner": "mgr-zhao",
            "actor": CMD, "reviewer": CMD, "at": "2026-09-23T20:45:00+08:00",
        })
        self.assertEqual(status, 400)

        # 合规升级
        status, _ = post(f"{self.base}/v1/alerts/escalate", {
            "alert_id": alert_id, "reason": "连续低于安全线", "new_owner": "mgr-zhao",
            "actor": CMD, "reviewer": AUD, "at": "2026-09-23T20:45:00+08:00",
        })
        self.assertEqual(status, 200)

        # 转交（班次交接）
        status, _ = post(f"{self.base}/v1/alerts/transfer", {
            "alert_id": alert_id, "to_owner": "disp-zhou",
            "actor": DISP, "at": "2026-09-23T21:00:00+08:00",
            "reason": "夜班转早班",
        })
        self.assertEqual(status, 200)

        # 无依据解除 -> 400
        status, body = post(f"{self.base}/v1/alerts/resolve", {
            "alert_id": alert_id, "basis_window_ids": [],
            "actor": CMD, "reviewer": AUD, "at": "2026-09-23T22:00:00+08:00",
        })
        self.assertEqual(status, 400)

        # 带依据解除
        status, _ = post(f"{self.base}/v1/alerts/resolve", {
            "alert_id": alert_id, "basis_window_ids": [wid],
            "actor": CMD, "reviewer": AUD, "at": "2026-09-23T22:00:00+08:00",
        })
        self.assertEqual(status, 200)

        # 派单
        status, body = post(f"{self.base}/v1/incidents/dispatch", {
            "alert_id": alert_id, "owner": "team-night", "decision": "现场核查",
            "actor": CMD, "at": "2026-09-23T21:40:00+08:00",
        })
        self.assertEqual(status, 200)
        self.assertTrue(body["created"])

        # 时间线查询
        status, body = get(
            f"{self.base}/v1/timeline?business_line=aquatic&category=fish&region=A")
        self.assertEqual(status, 200)
        types = {e["event_type"] for e in body}
        self.assertIn("snapshot.received", types)
        self.assertIn("alert.resolved", types)

        # 血缘：解除依据可追溯到来源
        status, body = get(f"{self.base}/v1/lineage?node={alert_id}")
        self.assertEqual(status, 200)
        kinds = {n["kind"] for n in body["upstream"]}
        self.assertEqual({"window", "snapshot", "source"}, kinds)

        # 沙盘推演不写回
        before = self.ctx.store.count()
        status, body = post(f"{self.base}/v1/whatif", {
            "name": "扩容", "capacity_delta": 600.0,
            "start": "2026-09-23T20:00:00+08:00",
            "end": "2026-09-23T21:00:00+08:00",
        })
        self.assertEqual(status, 200)
        self.assertTrue(any(d["alert_would_change"] for d in body["diff"]))
        self.assertEqual(self.ctx.store.count(), before)

    def test_recover_and_report(self) -> None:
        self._rule()
        # 两条真实快照 + 中间缺口
        for obs, rec, stock in (
            ("2026-09-24T20:05:00+08:00", "2026-09-24T20:35:00+08:00", 10.0),
            ("2026-09-24T22:05:00+08:00", "2026-09-24T22:35:00+08:00", 9.0),
        ):
            post(f"{self.base}/v1/snapshots", {
                "business_line": "aquatic", "category": "fish", "region": "A",
                "observed_at": obs, "recorded_at": rec,
                "stock": stock, "outbound": 8.0, "source": "aqs-a01",
            })
        status, body = post(f"{self.base}/v1/recover", {
            "until": "2026-09-24T23:00:00+08:00"})
        self.assertEqual(status, 200)
        self.assertEqual(body["windows_backfilled"], 6)
        # 再次恢复幂等
        status, body2 = post(f"{self.base}/v1/recover", {
            "until": "2026-09-24T23:00:00+08:00"})
        self.assertEqual(body2["windows_backfilled"], 0)

        # 发布日报并核对
        status, body = post(f"{self.base}/v1/reports", {
            "report_date": "2026-09-24", "actor": AUD})
        self.assertEqual(status, 200)
        status, body = get(f"{self.base}/v1/reports/daily:2026-09-24/reconcile")
        self.assertEqual(status, 200)
        self.assertTrue(body["reconciled"], body["findings"])


if __name__ == "__main__":
    unittest.main()
