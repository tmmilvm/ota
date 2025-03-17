from abc import ABC, abstractmethod
from typing import Generator

from ota.row_batch import RowBatch
from ota.schema import Schema


class PhysicalPlan(ABC):
    @abstractmethod
    def __str__(self) -> str: ...

    @abstractmethod
    def get_schema(self) -> Schema: ...

    @abstractmethod
    def get_children(self) -> list["PhysicalPlan"]: ...

    @abstractmethod
    def execute(self) -> Generator[RowBatch, None, None]: ...
