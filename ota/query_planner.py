from typing import cast

from ota.logical.expr.abc import LogicalBinaryExpr, LogicalExpr
from ota.logical.expr.impls import LogicalColumnExpr, LogicalMathExprAdd
from ota.logical.plan.abc import LogicalPlan
from ota.logical.plan.impls import LogicalProjection, LogicalScan
from ota.physical.expr.abc import PhysicalExpr
from ota.physical.expr.impls import PhysicalColumnExpr, PhysicalMathExprAdd
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
    schema_fields = {}
    for expr in logical_plan.get_exprs():
        schema_fields |= expr.to_schema_field(logical_plan.get_input_plan())
    projection_schema = Schema(schema_fields)
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
            left_expr = _create_physical_expr(
                binary_expr.get_left_operand(), input_plan
            )
            right_expr = _create_physical_expr(
                binary_expr.get_right_operand(), input_plan
            )
            match binary_expr:
                case LogicalMathExprAdd():
                    return PhysicalMathExprAdd(left_expr, right_expr)
                case _:
                    raise RuntimeError(f"Unsupported expr: {logical_expr}")
        case _:
            raise RuntimeError(f"Unsupported expr: {logical_expr}")


def _create_physical_column_expr(
    logical_expr: LogicalColumnExpr, input_plan: LogicalPlan
) -> PhysicalColumnExpr:
    column_name = logical_expr.get_column_name()
    column_names = list(input_plan.get_schema().get_fields().keys())
    try:
        index = column_names.index(column_name)
    except ValueError:
        raise IndexError(f"No column named {column_name}")
    return PhysicalColumnExpr(index)
