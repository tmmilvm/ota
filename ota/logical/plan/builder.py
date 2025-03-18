from __future__ import annotations

from ota.logical.expr.abc import LogicalExpr
from ota.schema import Schema

from .abc import LogicalPlan
from .impls import LogicalProjection, LogicalSelection


class LogicalPlanBuilder:
    _plan: LogicalPlan

    def __init__(self, plan: LogicalPlan) -> None:
        self._plan = plan

    def get_schema(self) -> Schema:
        return self._plan.get_schema()

    def get_logical_plan(self) -> LogicalPlan:
        return self._plan

    def project(self, exprs: list[LogicalExpr]) -> LogicalPlanBuilder:
        return LogicalPlanBuilder(LogicalProjection(self._plan, exprs))

    def select(self, expr: LogicalExpr) -> LogicalPlanBuilder:
        return LogicalPlanBuilder(LogicalSelection(self._plan, expr))
