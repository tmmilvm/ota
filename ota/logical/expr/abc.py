from abc import ABC, abstractmethod

from ota.logical.plan.abc import LogicalPlan
from ota.schema import SchemaField


class LogicalExpr(ABC):
    """Captures basic metadata about an expression."""

    @abstractmethod
    def __str__(self):
        """Returns a string representation of the expression.

        Returns:
            A string.
        """
        ...

    @abstractmethod
    def to_schema_field(self, plan: LogicalPlan) -> SchemaField:
        """Returns a schema field with the output data type of the expression.

        For some expressions the returned data type will depend on the type of
        data the expression is run against, for others the data type is always
        the same.

        Args:
            plan: The input data.
        Returns:
            A schema field.
        """
        ...


class LogicalBinaryExpr(LogicalExpr):
    """An expression that takes two inputs.

    Attributes:
        _operator: The operator.
        _left_operand: Left operand.
        _right_operand: Right operand.
    """

    _operator: str
    _left_operand: LogicalExpr
    _right_operand: LogicalExpr

    def __init__(
        self,
        operator: str,
        left_operand: LogicalExpr,
        right_operand: LogicalExpr,
    ) -> None:
        """Initializer.

        Args:
            operator: The operator.
            left_operand: Left operand.
            right_operand: Right operand.
        """
        self._operator = operator
        self._left_operand = left_operand
        self._right_operand = right_operand

    def __str__(self):
        return f"{self._left_operand} {self._operator} {self._right_operand}"

    @abstractmethod
    def to_schema_field(self, plan: LogicalPlan) -> SchemaField: ...

    def get_left_operand(self):
        """Returns the left operand.

        Returns:
            The left operand.
        """
        return self._left_operand

    def get_right_operand(self):
        """Returns the right operand.

        Returns:
            The right operand.
        """
        return self._right_operand


class LogicalMathExpr(LogicalBinaryExpr):
    """A mathematical expression taking two inputs.

    Attributes:
        _operator: The operator.
        _left_operand: Left operand.
        _right_operand: Right operand.
    """

    def __init__(
        self,
        operator: str,
        left_operand: LogicalExpr,
        right_operand: LogicalExpr,
    ) -> None:
        """Initializer.

        Args:
            operator: The operator.
            left_operand: Left operand.
            right_operand: Right operand.
        """
        if type(self) is LogicalMathExpr:
            raise RuntimeError("Can't instantiate LogicalMathExpr directly")
        super().__init__(operator, left_operand, right_operand)

    def to_schema_field(self, plan: LogicalPlan) -> SchemaField:
        """Returns a schema field with the output data type of the expression.

        The data type is set to the data type of the left operand.

        Args:
            plan: The input data.
        Returns:
            A schema field.
        """
        data_type = self._left_operand.to_schema_field(plan).data_type
        return SchemaField(self._operator, data_type)
