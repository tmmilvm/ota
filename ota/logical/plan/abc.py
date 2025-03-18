from abc import ABC, abstractmethod

from ota.schema import Schema


class LogicalPlan(ABC):
    @abstractmethod
    def __str__(self) -> str: ...

    @abstractmethod
    def get_schema(self) -> Schema: ...

    @abstractmethod
    def get_children(self) -> list["LogicalPlan"]: ...

    def format(self) -> str:
        return _format_plan(self, 0)


def _format_plan(plan: LogicalPlan, depth: int) -> str:
    plan_str = "\t" * depth
    plan_str += str(plan)
    for child in plan.get_children():
        plan_str += _format_plan(child, depth + 1)
    return plan_str
