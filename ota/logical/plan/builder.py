from ota.logical.expr.base import LogicalExpr
from ota.schema import Schema

from .base import LogicalPlan
from .impls import LogicalProjection


class LogicalPlanBuilder:
    _plan: LogicalPlan

    def __init__(self, plan: LogicalPlan) -> None:
        self._plan = plan

    def get_schema(self) -> Schema:
        return self._plan.get_schema()

    def get_logical_plan(self) -> LogicalPlan:
        return self._plan

    def project(self, exprs: list[LogicalExpr]) -> "LogicalPlanBuilder":
        return LogicalPlanBuilder(LogicalProjection(self._plan, exprs))
