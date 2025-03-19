from typing import Any, Generator

from ota.column import Column
from ota.data_loader import DataLoader
from ota.physical.expr.abc import PhysicalAggregateExpr, PhysicalExpr
from ota.row_batch import RowBatch
from ota.schema import Schema

from .abc import PhysicalPlan


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


class PhysicalSelection(PhysicalPlan):
    _input_plan: PhysicalPlan
    _expr: PhysicalExpr

    def __init__(self, input_plan: PhysicalPlan, expr: PhysicalExpr) -> None:
        self._input_plan = input_plan
        self._expr = expr

    def __str__(self) -> str:
        return f"Selection: {self._expr}"

    def get_schema(self) -> Schema:
        return self._input_plan.get_schema()

    def get_children(self) -> list["PhysicalPlan"]:
        return [self._input_plan]

    def execute(self) -> Generator[RowBatch, None, None]:
        for batch in self._input_plan.execute():
            expr_result = self._expr.evaluate(batch)

            selected_row_indices = [
                i for i in range(expr_result.size()) if expr_result[i]
            ]

            filtered_columns = []
            for col_index in range(batch.num_columns()):
                column = batch.get_column(col_index)
                filtered_values = []
                for row_index in selected_row_indices:
                    filtered_values.append(column[row_index])
                filtered_columns.append(
                    Column(column.get_data_type(), filtered_values)
                )

            yield RowBatch(batch.get_schema(), filtered_columns)


class PhysicalAggregate(PhysicalPlan):
    _input_plan: PhysicalPlan
    _grouping_exprs: list[PhysicalExpr]
    _aggregation_exprs: list[PhysicalAggregateExpr]
    _schema: Schema

    def __init__(
        self,
        input_plan: PhysicalPlan,
        grouping_exprs: list[PhysicalExpr],
        aggregation_exprs: list[PhysicalAggregateExpr],
        schema: Schema,
    ) -> None:
        self._input_plan = input_plan
        self._grouping_exprs = grouping_exprs
        self._aggregation_exprs = aggregation_exprs
        self._schema = schema

    def __str__(self) -> str:
        return (
            f"Aggregate: groupingExprs={self._grouping_exprs}, "
            f"aggregationExprs={self._aggregation_exprs}"
        )

    def get_schema(self) -> Schema:
        return self._schema

    def get_children(self) -> list["PhysicalPlan"]:
        return [self._input_plan]

    def execute(self) -> Generator[RowBatch, None, None]:
        hash_map: dict[
            tuple[Any, ...], list[PhysicalAggregateExpr.Accumulator]
        ] = dict()

        for batch in self._input_plan.execute():
            grouping_keys = list(
                map(lambda expr: expr.evaluate(batch), self._grouping_exprs)
            )

            for row_index in range(batch.num_rows()):
                row_grouping_key = tuple(
                    grouping_key[row_index] for grouping_key in grouping_keys
                )
                if row_grouping_key not in hash_map:
                    hash_map[row_grouping_key] = [
                        expr.create_accumulator()
                        for expr in self._aggregation_exprs
                    ]

                values_to_accumulate = [
                    expr.get_input_expr().evaluate(batch)
                    for expr in self._aggregation_exprs
                ]
                for accumulator_index in range(len(hash_map[row_grouping_key])):
                    value = values_to_accumulate[accumulator_index][row_index]
                    hash_map[row_grouping_key][accumulator_index].accumulate(
                        value
                    )

        aggregate_columns = [
            Column(
                self._schema.get_data_type(column_name), [None] * len(hash_map)
            )
            for column_name in self._schema.get_field_names()
        ]
        for row_index, row_grouping_key in enumerate(hash_map):
            for index in range(len(self._grouping_exprs)):
                aggregate_columns[index][row_index] = row_grouping_key[index]
            for index in range(len(self._aggregation_exprs)):
                aggregate_columns[len(self._grouping_exprs) + index][
                    row_index
                ] = hash_map[row_grouping_key][index].get_value()

        yield RowBatch(self._schema, aggregate_columns)
