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
a,b
1,2
1,4
2,6
3,8
2,10
1,2
1,4
3,6
3,8
3,10
1,2
2,4
```

### Aggregation

Aggregation can be performed like this:

```python
from pathlib import Path
from ota.execution_context import ExecutionContext
from ota.logical.expr.impls import (
    LogicalAggregateExprCount,
    LogicalColumnExpr,
)
from ota.schema import DataType, Schema

ctx = ExecutionContext()
schema = Schema({"a": DataType.Int, "b": DataType.Int})
plan = (
    ctx.csv(Path("test.csv"), schema)
    .aggregate(
        [LogicalColumnExpr("a")],
        [LogicalAggregateExprCount(LogicalColumnExpr("b"))],
    )
    .get_logical_plan()
)
for batch in ctx.execute(plan):
    print(batch.to_csv())
```

The output:

```
1,5
2,3
3,4
```
