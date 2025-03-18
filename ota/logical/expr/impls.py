from ota.logical.plan.abc import LogicalPlan
from ota.schema import DataType

from .abc import LogicalExpr, LogicalMathExpr


class LogicalColumnExpr(LogicalExpr):
    _column_name: str

    def __init__(self, column_name) -> None:
        self._column_name = column_name

    def __str__(self):
        return f"#{self._column_name}"

    def to_schema_field(self, plan: LogicalPlan) -> dict[str, DataType]:
        return {
            self._column_name: plan.get_schema().get_data_type(
                self._column_name
            )
        }

    def get_column_name(self) -> str:
        return self._column_name


class LogicalMathExprAdd(LogicalMathExpr):
    def __init__(
        self,
        left_operand: LogicalExpr,
        right_operand: LogicalExpr,
    ) -> None:
        super().__init__("+", left_operand, right_operand)


class LogicalMathExprSubtract(LogicalMathExpr):
    def __init__(
        self,
        left_operand: LogicalExpr,
        right_operand: LogicalExpr,
    ) -> None:
        super().__init__("-", left_operand, right_operand)


class LogicalMathExprMultiply(LogicalMathExpr):
    def __init__(
        self,
        left_operand: LogicalExpr,
        right_operand: LogicalExpr,
    ) -> None:
        super().__init__("*", left_operand, right_operand)


class LogicalMathExprDivide(LogicalMathExpr):
    def __init__(
        self,
        left_operand: LogicalExpr,
        right_operand: LogicalExpr,
    ) -> None:
        super().__init__("/", left_operand, right_operand)


class LogicalMathExprModulo(LogicalMathExpr):
    def __init__(
        self,
        left_operand: LogicalExpr,
        right_operand: LogicalExpr,
    ) -> None:
        super().__init__("%", left_operand, right_operand)
