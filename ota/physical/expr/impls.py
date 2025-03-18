from ota.column import Column
from ota.row_batch import RowBatch

from ota.schema import DataType
from .abc import PhysicalExpr, PhysicalMathExpr


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
                return int(left_operand) + int(right_operand)
            case _:
                raise RuntimeError("Unsupported data type")
