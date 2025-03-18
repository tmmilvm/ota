from abc import ABC, abstractmethod
from typing import Any

from ota.column import Column
from ota.row_batch import RowBatch
from ota.schema import DataType


class PhysicalExpr(ABC):
    @abstractmethod
    def __str__(self) -> str: ...

    @abstractmethod
    def evaluate(self, input_batch: RowBatch) -> Column: ...


class PhysicalBinaryExpr(PhysicalExpr):
    _left_expr: PhysicalExpr
    _right_expr: PhysicalExpr

    def __init__(
        self, left_expr: PhysicalExpr, right_expr: PhysicalExpr
    ) -> None:
        self._left_expr = left_expr
        self._right_expr = right_expr

    @abstractmethod
    def _evaluate(self, left_value: Column, right_value: Column): ...

    def evaluate(self, input_batch: RowBatch):
        left_value = self._left_expr.evaluate(input_batch)
        right_value = self._right_expr.evaluate(input_batch)
        assert left_value.size() == right_value.size()

        if left_value.get_data_type() != right_value.get_data_type():
            raise RuntimeError("Type mismatch in binary expression")

        return self._evaluate(left_value, right_value)


class PhysicalMathExpr(PhysicalBinaryExpr):
    @abstractmethod
    def _evaluate_impl(
        self,
        left_operand: int | float,
        right_operand: int | float,
        data_type: DataType,
    ): ...

    def _evaluate(self, left_value: Column, right_value: Column):
        data_type = left_value.get_data_type()
        values = [
            self._evaluate_impl(left_value[i], right_value[i], data_type)
            for i in range(left_value.size())
        ]
        return Column(data_type, values)


class PhysicalBooleanExpr(PhysicalBinaryExpr):
    @abstractmethod
    def _evaluate_impl(
        self,
        left_operand: Any,
        right_operand: Any,
        data_type: DataType,
    ): ...

    def _evaluate(self, left_value: Column, right_value: Column):
        values = [
            self._evaluate_impl(
                left_value[i], right_value[i], left_value.get_data_type()
            )
            for i in range(left_value.size())
        ]
        return Column(DataType.Bool, values)
