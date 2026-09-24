"""领域错误。"""
from __future__ import annotations


class SupplyError(Exception):
    """所有领域错误的基类。"""


class ValidationError(SupplyError):
    """上报数据不合法。"""


class AuthorizationError(SupplyError):
    """角色不足或违反双人复核约束。"""


class DuplicateError(SupplyError):
    """事件编号或幂等键冲突。"""


class IllegalTransition(SupplyError):
    """告警/处置单状态机不允许该动作。"""


class BasisError(SupplyError):
    """处置依据不足（例如仅凭汇总日报解除告警）。"""


class NotFound(SupplyError):
    """聚合不存在。"""
