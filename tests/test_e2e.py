import csv

import pytest

from ota.execution_context import ExecutionContext
from ota.logical.expr.impls import (
    LogicalColumnExpr,
    LogicalMathExprAdd,
    LogicalMathExprDivide,
    LogicalMathExprModulo,
    LogicalMathExprMultiply,
    LogicalMathExprSubtract,
)
from ota.schema import DataType, Schema


@pytest.fixture
def test_csv_file(tmp_path):
    test_csv_path = tmp_path / "test.csv"
    field_names = ["a", "b"]

    with open(test_csv_path, "w", newline="") as csv_file:
        csv_writer = csv.DictWriter(csv_file, fieldnames=field_names)
        csv_writer.writeheader()
        for i in range(1000):
            csv_writer.writerow({"a": i, "b": i + 1})

    return test_csv_path


def test_e2e(test_csv_file):
    schema = Schema({"a": DataType.Int, "b": DataType.Int})

    ctx = ExecutionContext()
    plan = (
        ctx.csv(test_csv_file, schema)
        .project(
            [
                LogicalColumnExpr("a"),
                LogicalColumnExpr("b"),
                LogicalMathExprAdd(
                    LogicalColumnExpr("a"), LogicalColumnExpr("b")
                ),
                LogicalMathExprSubtract(
                    LogicalColumnExpr("a"), LogicalColumnExpr("b")
                ),
                LogicalMathExprMultiply(
                    LogicalColumnExpr("a"), LogicalColumnExpr("b")
                ),
                LogicalMathExprDivide(
                    LogicalColumnExpr("a"), LogicalColumnExpr("b")
                ),
                LogicalMathExprModulo(
                    LogicalColumnExpr("a"), LogicalColumnExpr("b")
                ),
            ]
        )
        .get_logical_plan()
    )

    batches = list(ctx.execute(plan))
    assert len(batches) == 1

    batch = batches[0]
    assert batch.num_columns() == 7
    for i in range(1000):
        assert batch.get_column(2)[i] == i + (i + 1)
        assert batch.get_column(3)[i] == i - (i + 1)
        assert batch.get_column(4)[i] == i * (i + 1)
        assert batch.get_column(5)[i] == int(i / (i + 1))
        assert batch.get_column(6)[i] == i % (i + 1)
