from abc import ABC, abstractmethod

from ota.column import Column
from ota.row_batch import RowBatch


class PhysicalExpr(ABC):
    @abstractmethod
    def __str__(self) -> str: ...

    @abstractmethod
    def evaluate(self, input_batch: RowBatch) -> Column: ...
