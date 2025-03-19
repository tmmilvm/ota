from __future__ import annotations

from ota.logical.expr.abc import LogicalAggregateExpr, LogicalExpr
from ota.schema import Schema

from .abc import LogicalPlan
from .impls import LogicalAggregate, LogicalProjection, LogicalSelection


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

    def aggregate(
        self,
        grouping_exprs: list[LogicalExpr],
        aggregation_exprs: list[LogicalAggregateExpr],
    ) -> LogicalPlanBuilder:
        return LogicalPlanBuilder(
            LogicalAggregate(self._plan, grouping_exprs, aggregation_exprs)
        )
