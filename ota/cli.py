from pathlib import Path

from ota.execution_context import ExecutionContext
from ota.logical.expr.impls import LogicalColumnExpr
from ota.schema import DataType, Schema


def main():
    ctx = ExecutionContext()
    schema = None
    logical_plan_builder = None
    prompt = ">  "

    while True:
        line = input(prompt).strip()

        if line.startswith("schema"):
            _, fields = line.split(maxsplit=1)
            schema_fields = {}
            for field in fields.split(","):
                name, type_str = field.split(":", maxsplit=1)
                type_ = {"int": DataType.Int}[type_str]
                schema_fields[name] = type_
                schema = Schema(schema_fields)

        elif line.startswith("load"):
            fname = line.split()[1]
            logical_plan_builder = ctx.csv(Path(fname), schema)

        elif line.startswith("select"):
            column_names = line.split(maxsplit=1)[1]
            column_exprs = []
            for column_name in column_names.split(","):
                column_exprs.append(LogicalColumnExpr(column_name.strip(" ;")))
            logical_plan_builder = logical_plan_builder.project(column_exprs)

        if ";" in line:
            plan = logical_plan_builder.get_logical_plan()
            for batch in ctx.execute(plan):
                print(batch.to_csv())
            break

        prompt = ".. "
