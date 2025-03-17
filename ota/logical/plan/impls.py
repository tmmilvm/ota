from ota.data_loader import DataLoader
from ota.logical.expr.base import LogicalExpr
from ota.schema import Schema

from .base import LogicalPlan


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
        fields = {}
        for expr in self._exprs:
            fields = fields | expr.to_schema_field(self._input_plan)
        return Schema(fields)

    def get_children(self) -> list[LogicalPlan]:
        return [self._input_plan]

    def get_input_plan(self) -> LogicalPlan:
        return self._input_plan

    def get_exprs(self) -> list[LogicalExpr]:
        return self._exprs
