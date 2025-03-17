import pathlib

from ota.execution_context import ExecutionContext
from ota.logical.expr.impls import LogicalColumnExpr
from ota.schema import DataType, Schema

if __name__ == "__main__":
    schema = Schema({"a": DataType.Int, "b": DataType.Int})

    ctx = ExecutionContext()
    plan = (
        ctx.csv(pathlib.Path("../test.csv"), schema)
        .project([LogicalColumnExpr("b")])
        .get_logical_plan()
    )
    print(plan.format())

    for batch in ctx.execute(plan):
        print(f"Produced batch of size {batch.num_rows()}")
        print(batch.to_csv())
