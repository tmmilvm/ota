from typing import Generator

from ota.data_loader import DataLoader
from ota.physical.expr.base import PhysicalExpr
from ota.row_batch import RowBatch
from ota.schema import Schema

from .base import PhysicalPlan


class PhysicalScan(PhysicalPlan):
    _data_loader: DataLoader
    _projection: list[str]

    def __init__(self, data_loader: DataLoader, projection: list[str]) -> None:
        self._data_loader = data_loader
        self._projection = projection

    def __str__(self) -> str:
        return (
            f"Scan: schema={self.get_schema()}, projection={self._projection}"
        )

    def get_schema(self) -> Schema:
        return self._data_loader.get_schema().select(self._projection)

    def get_children(self) -> list["PhysicalPlan"]:
        return []

    def execute(self) -> Generator[RowBatch, None, None]:
        yield from self._data_loader.load(self._projection)


class PhysicalProjection(PhysicalPlan):
    _input_plan: PhysicalPlan
    _schema: Schema
    _exprs: list[PhysicalExpr]

    def __init__(
        self,
        input_plan: PhysicalPlan,
        schema: Schema,
        exprs: list[PhysicalExpr],
    ) -> None:
        self._input_plan = input_plan
        self._schema = schema
        self._exprs = exprs

    def __str__(self) -> str:
        return f"Projection: {self._exprs}"

    def get_schema(self) -> Schema:
        return self._schema

    def get_children(self) -> list["PhysicalPlan"]:
        return [self._input_plan]

    def execute(self) -> Generator[RowBatch, None, None]:
        for batch in self._input_plan.execute():
            columns = map(lambda expr: expr.evaluate(batch), self._exprs)
            yield RowBatch(self._schema, list(columns))
