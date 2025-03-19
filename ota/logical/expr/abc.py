from abc import ABC, abstractmethod

from ota.logical.plan.abc import LogicalPlan
from ota.schema import DataType, SchemaField


class LogicalExpr(ABC):
    @abstractmethod
    def __str__(self) -> str: ...

    @abstractmethod
    def to_schema_field(self, plan: LogicalPlan) -> SchemaField: ...


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

    def __str__(self) -> str:
        return f"{self._left_operand} {self._operator} {self._right_operand}"

    @abstractmethod
    def to_schema_field(self, plan: LogicalPlan) -> SchemaField: ...

    def get_left_operand(self) -> LogicalExpr:
        return self._left_operand

    def get_right_operand(self) -> LogicalExpr:
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

    def to_schema_field(self, plan: LogicalPlan) -> SchemaField:
        data_type = self._left_operand.to_schema_field(plan).data_type
        return SchemaField(self._operator, data_type)


class LogicalBooleanExpr(LogicalBinaryExpr):
    def __init__(
        self,
        operator: str,
        left_operand: LogicalExpr,
        right_operand: LogicalExpr,
    ) -> None:
        if type(self) is LogicalBooleanExpr:
            raise RuntimeError("Can't instantiate LogicalBooleanExpr directly")
        super().__init__(operator, left_operand, right_operand)

    def to_schema_field(self, plan: LogicalPlan) -> SchemaField:
        return SchemaField(self._operator, DataType.Bool)


class LogicalAggregateExpr(LogicalExpr):
    _name: str
    _expr: LogicalExpr

    def __init__(self, name: str, expr: LogicalExpr) -> None:
        if type(self) is LogicalAggregateExpr:
            raise RuntimeError(
                "Can't instantiate LogicalAggregateExpr directly"
            )
        self._name = name
        self._expr = expr

    def __str__(self) -> str:
        return f"{self._name}({self._expr})"

    def to_schema_field(self, plan: LogicalPlan) -> SchemaField:
        return SchemaField(
            self._name, self._expr.to_schema_field(plan).data_type
        )

    def get_expr(self) -> LogicalExpr:
        return self._expr
