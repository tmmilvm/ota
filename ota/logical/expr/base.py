from abc import ABC, abstractmethod

from ota.logical.plan.base import LogicalPlan
from ota.schema import DataType


class LogicalExpr(ABC):
    @abstractmethod
    def __str__(self): ...

    @abstractmethod
    def to_schema_field(self, plan: LogicalPlan) -> dict[str, DataType]: ...
