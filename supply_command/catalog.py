"""业态目录：业务线、品类、区域、角色与阈值规则的带版本定义。

规则（安全库存、覆盖天数阈值、数据延迟容忍度）按版本发布，
历史窗口永远按当时生效的规则版本计算（"发布后的规则以新版本追加"）。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from . import clock
from .errors import ValidationError, NotFound

# 四条业态
FRUIT_VEG = "fruit_veg"        # 果蔬
GRAIN_OIL = "grain_oil"        # 粮油
AQUATIC = "aquatic"            # 水产
SNACK = "snack"                # 休闲食品

BUSINESS_LINES = (FRUIT_VEG, GRAIN_OIL, AQUATIC, SNACK)

# 角色
ROLE_OPERATOR = "operator"          # 值守员：上报、合并、申请
ROLE_COMMANDER = "commander"        # 指挥员：升级、解除
ROLE_DISPATCH = "dispatch"          # 调度员：转交、抑制
ROLE_AUDITOR = "auditor"            # 复核人：双人复核
ROLE_ADMIN = "admin"

# 动作 -> 允许的发起角色
ACTION_ROLES: dict[str, frozenset[str]] = {
    "report": frozenset({ROLE_OPERATOR, ROLE_COMMANDER, ROLE_ADMIN}),
    "merge": frozenset({ROLE_OPERATOR, ROLE_COMMANDER}),
    "escalate": frozenset({ROLE_COMMANDER}),
    "transfer": frozenset({ROLE_DISPATCH, ROLE_COMMANDER}),
    "suppress": frozenset({ROLE_DISPATCH, ROLE_COMMANDER}),
    "resolve": frozenset({ROLE_COMMANDER}),
    "assign": frozenset({ROLE_DISPATCH, ROLE_COMMANDER}),
    "amend": frozenset({ROLE_AUDITOR, ROLE_COMMANDER}),
    "reconcile": frozenset({ROLE_AUDITOR, ROLE_ADMIN}),
}

# 需要双人复核（发起人 + 复核人，且二者不得为同一人）的动作
DUAL_CONTROL_ACTIONS: frozenset[str] = frozenset(
    {"escalate", "suppress", "resolve", "amend"}
)


@dataclass(frozen=True)
class CategoryRule:
    """某品类在一个版本区间内生效的规则。"""

    business_line: str
    category: str
    region: str
    safe_stock: float                 # 安全库存（吨）
    warn_coverage_days: float         # 覆盖天数低于此值进入预警
    critical_coverage_days: float     # 低于此值为严重
    throughput_per_hour: float        # 参考吞吐（吨/小时），用于吞吐压力
    max_late_seconds: int = 3600      # 数据延迟超过此值标记为"迟到"
    max_gap_seconds: int = 1800       # 超过此时长无快照判定为"数据缺失"
    valid_from: datetime | None = None
    version: int = 1

    def __post_init__(self) -> None:
        if self.business_line not in BUSINESS_LINES:
            raise ValidationError(f"未知业态：{self.business_line}")
        if self.warn_coverage_days <= 0 or self.critical_coverage_days <= 0:
            raise ValidationError("覆盖天数阈值必须为正")
        if self.critical_coverage_days > self.warn_coverage_days:
            raise ValidationError("严重阈值不得高于预警阈值")
        if self.safe_stock < 0:
            raise ValidationError("安全库存不能为负")


@dataclass
class Catalog:
    """内存目录。规则按 (品类, 区域, version) 保存，查询时按时间取生效版本。"""

    rules: dict[tuple[str, str, str], list[CategoryRule]] = field(default_factory=dict)

    def publish_rule(self, rule: CategoryRule) -> CategoryRule:
        key = (rule.business_line, rule.category, rule.region)
        versions = self.rules.setdefault(key, [])
        if any(r.version == rule.version for r in versions):
            raise ValidationError(f"规则版本已存在：{key} v{rule.version}")
        versions.append(rule)
        versions.sort(key=lambda r: (r.valid_from or datetime.min.replace(tzinfo=clock.CST)))
        return rule

    def rule_at(
        self, business_line: str, category: str, region: str, when: datetime
    ) -> CategoryRule:
        """取 ``when`` 时刻生效的规则；valid_from 为空视为自古生效。"""
        when = clock.parse(when)
        versions = self.rules.get((business_line, category, region))
        if not versions:
            raise NotFound(f"无规则：{business_line}/{category}/{region}")
        chosen = versions[0]
        for rule in versions:
            start = rule.valid_from or datetime.min.replace(tzinfo=clock.CST)
            if start <= when:
                chosen = rule
        return chosen

    def current_rule(
        self, business_line: str, category: str, region: str
    ) -> CategoryRule:
        return self.rule_at(business_line, category, region, clock.now())
