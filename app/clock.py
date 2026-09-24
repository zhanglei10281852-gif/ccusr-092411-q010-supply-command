"""时间工具：统一使用带时区的 ISO 8601 字符串。"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

CST = timezone(timedelta(hours=8))


def parse(value: str | datetime) -> datetime:
    """解析 ISO 8601 字符串，拒绝无时区时间。"""
    if isinstance(value, datetime):
        moment = value
    else:
        moment = datetime.fromisoformat(value)
    if moment.tzinfo is None:
        raise ValueError(f"时间必须包含时区：{value}")
    return moment


def format_value(moment: datetime) -> str:
    """保留原始时区偏移：同一市场统一 +08:00，字符串可直接比较排序。"""
    return moment.isoformat()


class Clock:
    """可冻结、可拨快的时钟，便于测试迟到与恢复补算。"""

    def __init__(self, now: str | datetime | None = None) -> None:
        self._now: datetime | None = parse(now) if now is not None else None

    def now(self) -> datetime:
        return self._now if self._now is not None else datetime.now(timezone.utc)

    def freeze(self, moment: str | datetime) -> None:
        self._now = parse(moment)

    def advance(self, **kwargs: int) -> None:
        self._now = self.now() + timedelta(**kwargs)
