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

### Aggregation

Grouping by columns `a` and `b` and summing column `c` for these groupings can be performed like this:

```python
from pathlib import Path
from ota.execution_context import ExecutionContext
from ota.logical.expr.impls import (
    LogicalAggregateExprSum as Sum,
)
from ota.logical.expr.impls import (
    LogicalColumnExpr as Column,
)
from ota.schema import DataType, Schema

ctx = ExecutionContext()
schema = Schema({"a": DataType.Int, "b": DataType.Int, "c": DataType.Int})
plan = (
    ctx.csv(Path("test.csv"), schema)
    .aggregate([Column("a"), Column("b")], [Sum(Column("c"))])
    .get_logical_plan()
)
for batch in ctx.execute(plan):
    print(batch.to_csv())

```

The output:

```
1,2,16
1,4,10
2,6,9
3,8,14
2,10,8
3,6,1
3,10,5
2,4,4
```
