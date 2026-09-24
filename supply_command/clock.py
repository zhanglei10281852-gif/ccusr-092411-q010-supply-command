"""时间工具：统一使用带时区的 ISO 8601（业务默认 +08:00）。"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

CST = timezone(timedelta(hours=8))


def now() -> datetime:
    return datetime.now(CST)


def at(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> datetime:
    """构造便于测试/演示的东八区时间。"""
    return datetime(year, month, day, hour, minute, tzinfo=CST)


def parse(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        raise ValueError(f"时间必须带时区：{value!r}")
    return dt


def iso(dt: datetime) -> str:
    return parse(dt).isoformat()


def floor_window(ts: datetime, minutes: int) -> datetime:
    ts = parse(ts)
    discard = (ts.minute % minutes) * 60 + ts.second + ts.microsecond
    return (ts - timedelta(seconds=discard)).replace(microsecond=0)


def iter_windows(start: datetime, end: datetime, minutes: int):
    """生成左闭右开窗口 [w, w+step)，覆盖 [start, end)。"""
    step = timedelta(minutes=minutes)
    w = floor_window(start, minutes)
    while w < end:
        yield w, w + step
        w += step
