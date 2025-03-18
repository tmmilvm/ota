from ota.logical.plan.abc import LogicalPlan
from ota.schema import DataType, SchemaField

from .abc import LogicalExpr, LogicalMathExpr


class LogicalColumnExpr(LogicalExpr):
    _column_name: str

    def __init__(self, column_name) -> None:
        self._column_name = column_name

    def __str__(self):
        return f"#{self._column_name}"

    def to_schema_field(self, plan: LogicalPlan) -> SchemaField:
        return SchemaField(
            self._column_name,
            plan.get_schema().get_data_type(self._column_name),
        )

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


class LogicalLiteralIntExpr(LogicalExpr):
    """An integer literal.

    Attributes:
        _number: An integer.
    """
    _number: int

    def __init__(self, number: int) -> None:
        """Initializer.

        Args:
            number: The number.
        """
        self._number = number

    def __str__(self) -> str:
        """Returns a string representation of the literal.

        Returns:
            The literal as a string.
        """
        return str(self._number)

    def get_value(self) -> int:
        """Returns the value of the literal.

        Returns:
            The value.
        """
        return self._number

    def to_schema_field(self, plan: LogicalPlan) -> SchemaField:
        """Returns a schema field with data type `DataType.Int`.

        Returns:
            A schema field for the literal.
        """
        return SchemaField(str(self._number), DataType.Int)
