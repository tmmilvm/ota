from abc import ABC, abstractmethod

from ota.logical.plan.abc import LogicalPlan
from ota.schema import DataType


class LogicalExpr(ABC):
    @abstractmethod
    def __str__(self): ...

    @abstractmethod
    def to_schema_field(self, plan: LogicalPlan) -> dict[str, DataType]: ...


class LogicalBinaryExpr(LogicalExpr):
    _operator: str
    _left_operand: LogicalExpr
    _right_operand: LogicalExpr

    def __init__(
        self,
        operator: str,
        left_operand: LogicalExpr,
        right_operand: LogicalExpr,
    ) -> None:
        self._operator = operator
        self._left_operand = left_operand
        self._right_operand = right_operand

    def __str__(self):
        return f"{self._left_operand} {self._operator} {self._right_operand}"

    @abstractmethod
    def to_schema_field(self, plan: LogicalPlan) -> dict[str, DataType]: ...

    def get_left_operand(self):
        return self._left_operand

    def get_right_operand(self):
        return self._right_operand


class LogicalMathExpr(LogicalBinaryExpr):
    def __init__(
        self,
        operator: str,
        left_operand: LogicalExpr,
        right_operand: LogicalExpr,
    ) -> None:
        if type(self) is LogicalMathExpr:
            raise RuntimeError("Can't instantiate LogicalMathExpr directly")
        super().__init__(operator, left_operand, right_operand)

    def to_schema_field(self, plan: LogicalPlan) -> dict[str, DataType]:
        return {
            self._operator: list(
                self._left_operand.to_schema_field(plan).values()
            )[0]
        }
