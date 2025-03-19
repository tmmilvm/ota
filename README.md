# ota

A simple query engine, created to explore how these kinds of systems are implemented.

## Currently implemented features

- Data types:
  - Int
  - Bool
- Operations:
  - Scan
  - Projection
  - Selection (filtering)
  - Aggregation
- Expressions:
  - Math expressions: addition, subtraction, multiplication, division, modulo
  - Boolean expressions: equal, not equal, greater-than(-or-equal), less-than(-or-equal), and, or
  - Aggregation expressions: sum, minimum, maximum, average, count
  - Integer literal

## Usage example

Let's assume we have a file `test.csv` with the following contents:

```csv
a,b,c
1,2,5
1,4,6
2,6,9
3,8,10
2,10,8
1,2,5
1,4,4
3,6,1
3,8,4
3,10,5
1,2,6
2,4,4
```

Selecting rows where `a > 1`, grouping by `a` and `b`, summing column `c` for the groups and returning the columns `a` and `SUM` can be done as follows:

```python
from pathlib import Path
from ota.execution_context import ExecutionContext
from ota.logical.expr.impls import (
    LogicalAggregateExprSum as Sum,
    LogicalBooleanExprGt as Gt,
    LogicalColumnExpr as Column,
    LogicalLiteralIntExpr as Int,
)
from ota.schema import DataType, Schema

ctx = ExecutionContext()
schema = Schema({"a": DataType.Int, "b": DataType.Int, "c": DataType.Int})
plan = (
    ctx.csv(Path("../test.csv"), schema)
    .select(Gt(Column("a"), Int(1)))
    .aggregate([Column("a"), Column("b")], [Sum(Column("c"))])
    .project([Column("a"), Column("SUM")])
    .get_logical_plan()
)
for batch in ctx.execute(plan):
    print(batch.to_csv())

```

The output:

```
2,9
3,14
2,8
3,1
3,5
2,4
```
