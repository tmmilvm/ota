from typing import cast

from ota.logical.expr.abc import LogicalBinaryExpr, LogicalExpr
from ota.logical.expr.impls import (
    LogicalColumnExpr,
    LogicalLiteralIntExpr,
    LogicalMathExprAdd,
    LogicalMathExprDivide,
    LogicalMathExprModulo,
    LogicalMathExprMultiply,
    LogicalMathExprSubtract,
)
from ota.logical.plan.abc import LogicalPlan
from ota.logical.plan.impls import LogicalProjection, LogicalScan
from ota.physical.expr.abc import PhysicalBinaryExpr, PhysicalExpr
from ota.physical.expr.impls import (
    PhysicalColumnExpr,
    PhysicalLiteralIntExpr,
    PhysicalMathExprAdd,
    PhysicalMathExprDivide,
    PhysicalMathExprModulo,
    PhysicalMathExprMultiply,
    PhysicalMathExprSubtract,
)
from ota.physical.plan.impls import PhysicalProjection, PhysicalScan
from ota.schema import Schema


def create_physical_plan(logical_plan: LogicalPlan):
    match logical_plan:
        case LogicalScan():
            logical_plan = cast(LogicalScan, logical_plan)
            return PhysicalScan(
                logical_plan.get_data_loader(), logical_plan.get_projection()
            )
        case LogicalProjection():
            logical_plan = cast(LogicalProjection, logical_plan)
            return _create_physical_projection(logical_plan)
        case _:
            raise RuntimeError(f"Unsupported plan: {logical_plan}")


def _create_physical_projection(
    logical_plan: LogicalProjection,
) -> PhysicalProjection:
    input_plan = create_physical_plan(logical_plan.get_input_plan())
    projection_schema = Schema(
        [
            expr.to_schema_field(logical_plan.get_input_plan())
            for expr in logical_plan.get_exprs()
        ]
    )
    projection_exprs = list(
        map(
            lambda expr: _create_physical_expr(
                expr, logical_plan.get_input_plan()
            ),
            logical_plan.get_exprs(),
        )
    )
    return PhysicalProjection(input_plan, projection_schema, projection_exprs)


def _create_physical_expr(
    logical_expr: LogicalExpr, input_plan: LogicalPlan
) -> PhysicalExpr:
    match logical_expr:
        case LogicalColumnExpr():
            column_expr = cast(LogicalColumnExpr, logical_expr)
            return _create_physical_column_expr(column_expr, input_plan)
        case LogicalBinaryExpr():
            binary_expr = cast(LogicalBinaryExpr, logical_expr)
            return _create_physical_binary_expr(binary_expr, input_plan)
        case LogicalLiteralIntExpr():
            literal_int_expr = cast(LogicalLiteralIntExpr, logical_expr)
            return PhysicalLiteralIntExpr(literal_int_expr.get_value())
        case _:
            raise RuntimeError(f"Unsupported expr: {logical_expr}")


def _create_physical_column_expr(
    logical_expr: LogicalColumnExpr, input_plan: LogicalPlan
) -> PhysicalColumnExpr:
    column_name = logical_expr.get_column_name()
    column_names = input_plan.get_schema().get_field_names()
    try:
        index = column_names.index(column_name)
    except ValueError:
        raise IndexError(f"No column named {column_name}")
    return PhysicalColumnExpr(index)


def _create_physical_binary_expr(
    logical_expr: LogicalBinaryExpr, input_plan: LogicalPlan
) -> PhysicalBinaryExpr:
    left_expr = _create_physical_expr(
        logical_expr.get_left_operand(), input_plan
    )
    right_expr = _create_physical_expr(
        logical_expr.get_right_operand(), input_plan
    )
    match logical_expr:
        case LogicalMathExprAdd():
            return PhysicalMathExprAdd(left_expr, right_expr)
        case LogicalMathExprSubtract():
            return PhysicalMathExprSubtract(left_expr, right_expr)
        case LogicalMathExprMultiply():
            return PhysicalMathExprMultiply(left_expr, right_expr)
        case LogicalMathExprDivide():
            return PhysicalMathExprDivide(left_expr, right_expr)
        case LogicalMathExprModulo():
            return PhysicalMathExprModulo(left_expr, right_expr)
        case _:
            raise RuntimeError(f"Unsupported expr: {logical_expr}")
