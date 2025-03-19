from ota.data_loader import DataLoader
from ota.logical.expr.abc import LogicalAggregateExpr, LogicalExpr
from ota.schema import Schema

from .abc import LogicalPlan


class LogicalScan(LogicalPlan):
    _data_loader: DataLoader
    _projection: list[str]
    _schema: Schema

    def __init__(self, data_loader: DataLoader, projection: list[str]) -> None:
        self._data_loader = data_loader
        self._projection = projection
        if len(projection) == 0:
            self._schema = self._data_loader.get_schema()
        else:
            self._schema = self._data_loader.get_schema().select(projection)

    def __str__(self) -> str:
        source_name = self._data_loader.get_source_name()
        return f"Scan: {source_name}, projection={self._projection}"

    def get_schema(self) -> Schema:
        return self._schema

    def get_children(self) -> list[LogicalPlan]:
        return []

    def get_data_loader(self) -> DataLoader:
        return self._data_loader

    def get_projection(self) -> list[str]:
        return self._projection


class LogicalProjection(LogicalPlan):
    _input_plan: LogicalPlan
    _exprs: list[LogicalExpr]

    def __init__(
        self, input_plan: LogicalPlan, exprs: list[LogicalExpr]
    ) -> None:
        self._input_plan = input_plan
        self._exprs = exprs

    def __str__(self) -> str:
        return f"Projection: {self._exprs}"

    def get_schema(self) -> Schema:
        return Schema(
            [expr.to_schema_field(self._input_plan) for expr in self._exprs]
        )

    def get_children(self) -> list[LogicalPlan]:
        return [self._input_plan]

    def get_input_plan(self) -> LogicalPlan:
        return self._input_plan

    def get_exprs(self) -> list[LogicalExpr]:
        return self._exprs


class LogicalSelection(LogicalPlan):
    _input_plan: LogicalPlan
    _expr: LogicalExpr

    def __init__(self, input_plan: LogicalPlan, expr: LogicalExpr) -> None:
        self._input_plan = input_plan
        self._expr = expr

    def __str__(self) -> str:
        return f"Selection: {self._expr}"

    def get_schema(self) -> Schema:
        return self._input_plan.get_schema()

    def get_children(self) -> list["LogicalPlan"]:
        return [self._input_plan]

    def get_input_plan(self) -> LogicalPlan:
        return self._input_plan

    def get_expr(self) -> LogicalExpr:
        return self._expr


class LogicalAggregate(LogicalPlan):
    _input_plan: LogicalPlan
    _grouping_exprs: list[LogicalExpr]
    _aggregation_exprs: list[LogicalAggregateExpr]

    def __init__(
        self,
        input_plan: LogicalPlan,
        grouping_exprs: list[LogicalExpr],
        aggregation_exprs: list[LogicalAggregateExpr],
    ) -> None:
        self._input_plan = input_plan
        self._grouping_exprs = grouping_exprs
        self._aggregation_exprs = aggregation_exprs

    def __str__(self) -> str:
        return (
            f"Aggregate: groupingExprs={self._grouping_exprs}, "
            f"aggregationExprs={self._aggregation_exprs}"
        )

    def get_schema(self) -> Schema:
        return Schema(
            [
                expr.to_schema_field(self._input_plan)
                for expr in self._grouping_exprs + self._aggregation_exprs
            ]
        )

    def get_children(self) -> list["LogicalPlan"]:
        return [self._input_plan]

    def get_input_plan(self) -> LogicalPlan:
        return self._input_plan

    def get_grouping_exprs(self) -> list[LogicalExpr]:
        return self._grouping_exprs

    def get_aggregation_exprs(self) -> list[LogicalAggregateExpr]:
        return self._aggregation_exprs
