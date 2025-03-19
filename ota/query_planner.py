from typing import cast

from ota.logical.expr.abc import LogicalBinaryExpr, LogicalExpr
from ota.logical.expr.impls import (
    LogicalAggregateExprAvg,
    LogicalAggregateExprCount,
    LogicalAggregateExprMax,
    LogicalAggregateExprMin,
    LogicalAggregateExprSum,
    LogicalBooleanExprAnd,
    LogicalBooleanExprEq,
    LogicalBooleanExprGt,
    LogicalBooleanExprGtEq,
    LogicalBooleanExprLt,
    LogicalBooleanExprLtEq,
    LogicalBooleanExprNeq,
    LogicalBooleanExprOr,
    LogicalColumnExpr,
    LogicalLiteralIntExpr,
    LogicalMathExprAdd,
    LogicalMathExprDivide,
    LogicalMathExprModulo,
    LogicalMathExprMultiply,
    LogicalMathExprSubtract,
)
from ota.logical.plan.abc import LogicalPlan
from ota.logical.plan.impls import (
    LogicalAggregate,
    LogicalProjection,
    LogicalScan,
    LogicalSelection,
)
from ota.physical.expr.abc import PhysicalBinaryExpr, PhysicalExpr
from ota.physical.expr.impls import (
    PhysicalAggregateExprAvg,
    PhysicalAggregateExprCount,
    PhysicalAggregateExprMax,
    PhysicalAggregateExprMin,
    PhysicalAggregateExprSum,
    PhysicalBooleanExprAnd,
    PhysicalBooleanExprEq,
    PhysicalBooleanExprGt,
    PhysicalBooleanExprGtEq,
    PhysicalBooleanExprLt,
    PhysicalBooleanExprLtEq,
    PhysicalBooleanExprNeq,
    PhysicalBooleanExprOr,
    PhysicalColumnExpr,
    PhysicalLiteralIntExpr,
    PhysicalMathExprAdd,
    PhysicalMathExprDivide,
    PhysicalMathExprModulo,
    PhysicalMathExprMultiply,
    PhysicalMathExprSubtract,
)
from ota.physical.plan.abc import PhysicalPlan
from ota.physical.plan.impls import (
    PhysicalAggregate,
    PhysicalProjection,
    PhysicalScan,
    PhysicalSelection,
)
from ota.schema import Schema


def create_physical_plan(logical_plan: LogicalPlan) -> PhysicalPlan:
    match logical_plan:
        case LogicalScan():
            logical_plan = cast(LogicalScan, logical_plan)
            return PhysicalScan(
                logical_plan.get_data_loader(), logical_plan.get_projection()
            )
        case LogicalProjection():
            logical_plan = cast(LogicalProjection, logical_plan)
            return _create_physical_projection(logical_plan)
        case LogicalSelection():
            logical_plan = cast(LogicalSelection, logical_plan)
            return _create_physical_selection(logical_plan)
        case LogicalAggregate():
            logical_plan = cast(LogicalAggregate, logical_plan)
            return _create_physical_aggregate(logical_plan)
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


def _create_physical_selection(
    logical_plan: LogicalSelection,
) -> PhysicalSelection:
    input_plan = create_physical_plan(logical_plan.get_input_plan())
    filter_expr = _create_physical_expr(
        logical_plan.get_expr(), logical_plan.get_input_plan()
    )
    return PhysicalSelection(input_plan, filter_expr)


def _create_physical_aggregate(
    logical_plan: LogicalAggregate,
) -> PhysicalAggregate:
    input_plan = create_physical_plan(logical_plan.get_input_plan())
    grouping_exprs = [
        _create_physical_expr(expr, logical_plan.get_input_plan())
        for expr in logical_plan.get_grouping_exprs()
    ]
    aggregation_exprs = []
    for expr in logical_plan.get_aggregation_exprs():
        match expr:
            case LogicalAggregateExprSum():
                physical_cls = PhysicalAggregateExprSum
            case LogicalAggregateExprMin():
                physical_cls = PhysicalAggregateExprMin
            case LogicalAggregateExprMax():
                physical_cls = PhysicalAggregateExprMax
            case LogicalAggregateExprAvg():
                physical_cls = PhysicalAggregateExprAvg
            case LogicalAggregateExprCount():
                physical_cls = PhysicalAggregateExprCount
            case _:
                raise RuntimeError("Unsupported aggregate expr")
        aggregation_exprs.append(
            physical_cls(
                _create_physical_expr(
                    expr.get_expr(),
                    logical_plan.get_input_plan(),
                )
            )
        )
    return PhysicalAggregate(
        input_plan,
        grouping_exprs,
        aggregation_exprs,
        logical_plan.get_schema(),
    )


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
        case LogicalBooleanExprEq():
            return PhysicalBooleanExprEq(left_expr, right_expr)
        case LogicalBooleanExprNeq():
            return PhysicalBooleanExprNeq(left_expr, right_expr)
        case LogicalBooleanExprGt():
            return PhysicalBooleanExprGt(left_expr, right_expr)
        case LogicalBooleanExprGtEq():
            return PhysicalBooleanExprGtEq(left_expr, right_expr)
        case LogicalBooleanExprLt():
            return PhysicalBooleanExprLt(left_expr, right_expr)
        case LogicalBooleanExprLtEq():
            return PhysicalBooleanExprLtEq(left_expr, right_expr)
        case LogicalBooleanExprAnd():
            return PhysicalBooleanExprAnd(left_expr, right_expr)
        case LogicalBooleanExprOr():
            return PhysicalBooleanExprOr(left_expr, right_expr)
        case _:
            raise RuntimeError(f"Unsupported expr: {logical_expr}")
