from typing import cast

from ota.logical.expr.base import LogicalExpr
from ota.logical.expr.impls import LogicalColumnExpr
from ota.logical.plan.base import LogicalPlan
from ota.logical.plan.impls import LogicalProjection, LogicalScan
from ota.physical.expr.base import PhysicalExpr
from ota.physical.expr.impls import PhysicalColumnExpr
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
            input_plan = create_physical_plan(logical_plan.get_input_plan())
            schema_fields = {}
            for expr in logical_plan.get_exprs():
                schema_fields = schema_fields | expr.to_schema_field(
                    logical_plan.get_input_plan()
                )
            projection_schema = Schema(schema_fields)
            projection_exprs = list(
                map(
                    lambda expr: _create_physical_expr(
                        expr, logical_plan.get_input_plan()
                    ),
                    logical_plan.get_exprs(),
                )
            )
            return PhysicalProjection(
                input_plan, projection_schema, projection_exprs
            )
        case _:
            raise RuntimeError(f"Unsupported plan: {logical_plan}")


def _create_physical_expr(
    from_expr: LogicalExpr, input_plan: LogicalPlan
) -> PhysicalExpr:
    assert isinstance(from_expr, LogicalColumnExpr)
    match from_expr:
        case LogicalColumnExpr():
            from_expr = cast(LogicalColumnExpr, from_expr)
            return _create_physical_column_expr(from_expr, input_plan)
        case _:
            raise RuntimeError(f"Unsupported expr: {from_expr}")


def _create_physical_column_expr(
    from_expr: LogicalColumnExpr, input_plan: LogicalPlan
) -> PhysicalColumnExpr:
    column_name = from_expr.get_column_name()
    column_names = list(input_plan.get_schema().get_fields().keys())
    try:
        index = column_names.index(column_name)
    except ValueError:
        raise IndexError(f"No column named {column_name}")
    return PhysicalColumnExpr(index)
