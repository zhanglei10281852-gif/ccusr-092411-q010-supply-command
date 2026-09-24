"""阈值规则：按业态发布，只追加新版本，永不修改已发布版本。"""
from __future__ import annotations

from typing import Any

from .clock import Clock, format_value
from .store import Store, dumps, loads

DEFAULT_RULE: dict[str, Any] = {
    "safety_coverage_days": 2.0,
    "warning_coverage_days": 3.0,
    "pressure_warning": 0.8,
    "pressure_critical": 1.0,
    "late_threshold_seconds": 3600,
    "snapshot_max_age_seconds": 7200,
}
DEFAULT_RULE_ID = "thresholds:default"


class RuleBook:
    def __init__(self, store: Store, clock: Clock) -> None:
        self.store = store
        self.clock = clock
        if self.latest(DEFAULT_RULE_ID) is None:
            self.publish(DEFAULT_RULE_ID, DEFAULT_RULE)

    def publish(self, rule_id: str, content: dict[str, Any]) -> dict[str, Any]:
        row = self.store.query_one(
            "SELECT MAX(version) AS v FROM rules WHERE rule_id = ?", (rule_id,)
        )
        version = (row["v"] or 0) + 1
        merged = {**DEFAULT_RULE, **content}
        with self.store.transaction():
            self.store.execute(
                "INSERT INTO rules(rule_id, version, content, published_at) VALUES (?,?,?,?)",
                (rule_id, version, dumps(merged), format_value(self.clock.now())),
            )
        return {"rule_id": rule_id, "version": version, "content": merged}

    def latest(self, rule_id: str) -> dict[str, Any] | None:
        row = self.store.query_one(
            "SELECT * FROM rules WHERE rule_id = ? ORDER BY version DESC LIMIT 1", (rule_id,)
        )
        if row is None:
            return None
        return {"rule_id": rule_id, "version": row["version"], "content": loads(row["content"])}

    def for_line(self, business_line: str) -> dict[str, Any]:
        """优先取业态专属规则，缺省回退到默认规则。"""
        specific = self.latest(f"thresholds:{business_line}")
        if specific is not None:
            return specific
        rule = self.latest(DEFAULT_RULE_ID)
        assert rule is not None
        return rule
