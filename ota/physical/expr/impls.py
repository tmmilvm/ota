from typing import Any

from ota.column import Column
from ota.row_batch import RowBatch
from ota.schema import DataType

from .abc import (
    PhysicalAggregateExpr,
    PhysicalBooleanExpr,
    PhysicalExpr,
    PhysicalMathExpr,
)


class PhysicalColumnExpr(PhysicalExpr):
    _index: int

    def __init__(self, index: int) -> None:
        self._index = index

    def __str__(self) -> str:
        return f"#{self._index}"

    def evaluate(self, input_batch: RowBatch) -> Column:
        return input_batch.get_column(self._index)


class PhysicalMathExprAdd(PhysicalMathExpr):
    def __str__(self) -> str:
        return f"{self._left_expr}+{self._right_expr}"

    def _evaluate_impl(
        self,
        left_operand: int | float,
        right_operand: int | float,
        data_type: DataType,
    ):
        match data_type:
            case DataType.Int:
                return left_operand + right_operand
            case _:
                raise RuntimeError("Unsupported data type")


class PhysicalMathExprSubtract(PhysicalMathExpr):
    def __str__(self) -> str:
        return f"{self._left_expr}-{self._right_expr}"

    def _evaluate_impl(
        self,
        left_operand: int | float,
        right_operand: int | float,
        data_type: DataType,
    ):
        match data_type:
            case DataType.Int:
                return left_operand - right_operand
            case _:
                raise RuntimeError("Unsupported data type")


class PhysicalMathExprMultiply(PhysicalMathExpr):
    def __str__(self) -> str:
        return f"{self._left_expr}*{self._right_expr}"

    def _evaluate_impl(
        self,
        left_operand: int,
        right_operand: int,
        data_type: DataType,
    ):
        match data_type:
            case DataType.Int:
                return left_operand * right_operand
            case _:
                raise RuntimeError("Unsupported data type")


class PhysicalMathExprDivide(PhysicalMathExpr):
    def __str__(self) -> str:
        return f"{self._left_expr}/{self._right_expr}"

    def _evaluate_impl(
        self,
        left_operand: int,
        right_operand: int,
        data_type: DataType,
    ):
        match data_type:
            case DataType.Int:
                return int(left_operand / right_operand)
            case _:
                raise RuntimeError("Unsupported data type")


class PhysicalMathExprModulo(PhysicalMathExpr):
    def __str__(self) -> str:
        return f"{self._left_expr}%{self._right_expr}"

    def _evaluate_impl(
        self,
        left_operand: int,
        right_operand: int,
        data_type: DataType,
    ):
        match data_type:
            case DataType.Int:
                return left_operand % right_operand
            case _:
                raise RuntimeError("Unsupported data type")


class PhysicalBooleanExprEq(PhysicalBooleanExpr):
    def __str__(self) -> str:
        return f"{self._left_expr} = {self._right_expr}"

    def _evaluate_impl(
        self, left_operand: Any, right_operand: Any, data_type: DataType
    ):
        assert data_type == DataType.Int
        return left_operand == right_operand


class PhysicalBooleanExprNeq(PhysicalBooleanExpr):
    def __str__(self) -> str:
        return f"{self._left_expr} != {self._right_expr}"

    def _evaluate_impl(
        self, left_operand: Any, right_operand: Any, data_type: DataType
    ):
        assert data_type == DataType.Int
        return left_operand != right_operand


class PhysicalBooleanExprGt(PhysicalBooleanExpr):
    def __str__(self) -> str:
        return f"{self._left_expr} > {self._right_expr}"

    def _evaluate_impl(
        self, left_operand: Any, right_operand: Any, data_type: DataType
    ):
        assert data_type == DataType.Int
        return left_operand > right_operand


class PhysicalBooleanExprGtEq(PhysicalBooleanExpr):
    def __str__(self) -> str:
        return f"{self._left_expr} >= {self._right_expr}"

    def _evaluate_impl(
        self, left_operand: Any, right_operand: Any, data_type: DataType
    ):
        assert data_type == DataType.Int
        return left_operand >= right_operand


class PhysicalBooleanExprLt(PhysicalBooleanExpr):
    def __str__(self) -> str:
        return f"{self._left_expr} < {self._right_expr}"

    def _evaluate_impl(
        self, left_operand: Any, right_operand: Any, data_type: DataType
    ):
        assert data_type == DataType.Int
        return left_operand < right_operand


class PhysicalBooleanExprLtEq(PhysicalBooleanExpr):
    def __str__(self) -> str:
        return f"{self._left_expr} <= {self._right_expr}"

    def _evaluate_impl(
        self, left_operand: Any, right_operand: Any, data_type: DataType
    ):
        assert data_type == DataType.Int
        return left_operand <= right_operand


class PhysicalBooleanExprAnd(PhysicalBooleanExpr):
    def __str__(self) -> str:
        return f"{self._left_expr} AND {self._right_expr}"

    def _evaluate_impl(
        self, left_operand: Any, right_operand: Any, data_type: DataType
    ):
        match data_type:
            case DataType.Int():
                return (left_operand == 1) and (right_operand == 1)
            case DataType.Bool():
                return left_operand and right_operand
            case _:
                raise RuntimeError("Unsupported data type")


class PhysicalBooleanExprOr(PhysicalBooleanExpr):
    def __str__(self) -> str:
        return f"{self._left_expr} OR {self._right_expr}"

    def _evaluate_impl(
        self, left_operand: Any, right_operand: Any, data_type: DataType
    ):
        match data_type:
            case DataType.Int():
                return (left_operand == 1) or (right_operand == 1)
            case DataType.Bool():
                return left_operand or right_operand
            case _:
                raise RuntimeError("Unsupported data type")


class PhysicalLiteralIntExpr(PhysicalExpr):
    _of: int

    def __init__(self, of: int) -> None:
        self._of = of

    def __str__(self) -> str:
        return str(self._of)

    def evaluate(self, input_batch: RowBatch) -> Column:
        # TODO: Create a literal column class to optimize this
        return Column(DataType.Int, [self._of] * input_batch.num_rows())


class PhysicalAggregateExprSum(PhysicalAggregateExpr):
    class Accumulator(PhysicalAggregateExpr.Accumulator):
        _value: Any

        def __init__(self):
            self._value = 0

        def accumulate(self, value: Any) -> None:
            match value:
                case int():
                    self._value += value
                case _:
                    raise RuntimeError("Unsupported data type")

        def get_value(self) -> Any:
            return self._value

    def __str__(self) -> str:
        return f"SUM({self._input_expr})"

    def create_accumulator(self) -> PhysicalAggregateExpr.Accumulator:
        return PhysicalAggregateExprSum.Accumulator()


class PhysicalAggregateExprMin(PhysicalAggregateExpr):
    class Accumulator(PhysicalAggregateExpr.Accumulator):
        _value: Any

        def __init__(self):
            self._value = None

        def accumulate(self, value: Any) -> None:
            if self._value is None:
                self._value = value
                return

            match value:
                case int() | str():
                    self._value = min(self._value, value)
                case _:
                    raise RuntimeError("Unsupported data type")

        def get_value(self) -> Any:
            return self._value

    def __str__(self) -> str:
        return f"MIN({self._input_expr})"

    def create_accumulator(self) -> PhysicalAggregateExpr.Accumulator:
        return PhysicalAggregateExprMin.Accumulator()


class PhysicalAggregateExprMax(PhysicalAggregateExpr):
    class Accumulator(PhysicalAggregateExpr.Accumulator):
        _value: Any

        def __init__(self):
            self._value = None

        def accumulate(self, value: Any) -> None:
            if self._value is None:
                self._value = value
                return

            match value:
                case int() | str():
                    self._value = max(self._value, value)
                case _:
                    raise RuntimeError("Unsupported data type")

        def get_value(self) -> Any:
            return self._value

    def __str__(self) -> str:
        return f"MAX({self._input_expr})"

    def create_accumulator(self) -> PhysicalAggregateExpr.Accumulator:
        return PhysicalAggregateExprMax.Accumulator()


class PhysicalAggregateExprAvg(PhysicalAggregateExpr):
    class Accumulator(PhysicalAggregateExpr.Accumulator):
        _total: Any
        _num_values: int

        def __init__(self):
            self._total = 0
            self._num_values = 0

        def accumulate(self, value: Any) -> None:
            match value:
                case int():
                    self._total += value
                    self._num_values += 1
                case _:
                    raise RuntimeError("Unsupported data type")

        def get_value(self) -> Any:
            return int(self._total / self._num_values)

    def __str__(self) -> str:
        return f"AVG({self._input_expr})"

    def create_accumulator(self) -> PhysicalAggregateExpr.Accumulator:
        return PhysicalAggregateExprAvg.Accumulator()


class PhysicalAggregateExprCount(PhysicalAggregateExpr):
    class Accumulator(PhysicalAggregateExpr.Accumulator):
        _count: Any

        def __init__(self):
            self._count = 0

        def accumulate(self, value: Any) -> None:
            self._count += 1

        def get_value(self) -> Any:
            return self._count

    def __str__(self) -> str:
        return f"COUNT({self._input_expr})"

    def create_accumulator(self) -> PhysicalAggregateExpr.Accumulator:
        return PhysicalAggregateExprCount.Accumulator()
