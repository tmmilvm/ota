import csv
from random import randint

import pytest

from ota.execution_context import ExecutionContext
from ota.logical.expr.impls import LogicalColumnExpr
from ota.schema import DataType, Schema


@pytest.fixture
def test_csv_file(tmp_path):
    test_csv_path = tmp_path / "test.csv"
    field_names = ["userId", "accountBalance"]

    with open(test_csv_path, "w", newline="") as csv_file:
        csv_writer = csv.DictWriter(csv_file, fieldnames=field_names)
        csv_writer.writeheader()
        for i in range(1000):
            csv_writer.writerow(
                {"userId": i, "accountBalance": randint(50, 1000)}
            )

    return test_csv_path


def test_e2e(test_csv_file):
    schema = Schema({"userId": DataType.Int, "accountBalance": DataType.Int})

    ctx = ExecutionContext()
    plan = (
        ctx.csv(test_csv_file, schema)
        .project([LogicalColumnExpr("accountBalance")])
        .get_logical_plan()
    )

    batches = list(ctx.execute(plan))
    assert len(batches) == 1
    assert batches[0].num_columns() == 1
