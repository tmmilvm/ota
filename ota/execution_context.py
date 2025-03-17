from pathlib import Path
from typing import Generator

from ota.data_loader import CsvLoader
from ota.logical.plan.base import LogicalPlan
from ota.logical.plan.builder import LogicalPlanBuilder
from ota.logical.plan.impls import LogicalScan
from ota.query_planner import create_physical_plan
from ota.row_batch import RowBatch
from ota.schema import Schema


class ExecutionContext:
    def csv(self, path: Path, schema: Schema) -> LogicalPlanBuilder:
        data_loader = CsvLoader(path, schema)
        return LogicalPlanBuilder(LogicalScan(data_loader, []))

    def execute(
        self, logical_plan: LogicalPlan
    ) -> Generator[RowBatch, None, None]:
        physical_plan = create_physical_plan(logical_plan)
        yield from physical_plan.execute()
