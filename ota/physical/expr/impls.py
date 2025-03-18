from ota.column import Column
from ota.row_batch import RowBatch

from .abc import PhysicalExpr


class PhysicalColumnExpr(PhysicalExpr):
    _index: int

    def __init__(self, index: int) -> None:
        self._index = index

    def __str__(self) -> str:
        return f"#{self._index}"

    def evaluate(self, input_batch: RowBatch) -> Column:
        return input_batch.get_column(self._index)
