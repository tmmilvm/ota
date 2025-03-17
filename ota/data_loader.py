"""Data loaders for various file formats."""

from abc import ABC, abstractmethod
from csv import DictReader
from pathlib import Path
from typing import Generator

from ota.column import Column
from ota.row_batch import RowBatch
from ota.schema import Schema


class DataLoader(ABC):
    @abstractmethod
    def get_schema(self) -> Schema: ...

    @abstractmethod
    def get_source_name(self) -> str: ...

    @abstractmethod
    def load(
        self, projection: list[str]
    ) -> Generator[RowBatch, None, None]: ...


class CsvLoader(DataLoader):
    _path: Path
    _schema: Schema
    _batch_size: int

    def __init__(
        self, path: Path, schema: Schema, batch_size: int = 1_000
    ) -> None:
        self._path = path
        self._schema = schema
        self._batch_size = batch_size

    def get_schema(self) -> Schema:
        return self._schema

    def get_source_name(self) -> str:
        return str(self._path)

    def load(self, projection: list[str]) -> Generator[RowBatch, None, None]:
        if projection:
            schema = self._schema.select(projection)
        else:
            schema = self._schema

        with open(self._path, newline="") as csv_file:
            reader = DictReader(csv_file)
            read_rows = []
            for num_rows_read, row in enumerate(reader, start=1):
                read_rows.append(
                    {
                        column_name: row[column_name]
                        for column_name in schema.get_fields().keys()
                    }
                )
                if num_rows_read % self._batch_size == 0:
                    yield _to_row_batch(read_rows, schema)
                    read_rows.clear()
            if read_rows:
                yield _to_row_batch(read_rows, schema)


def _to_row_batch(read_rows: list[dict], schema: Schema) -> RowBatch:
    columns = []
    for column_name in schema.get_fields().keys():
        data_type = schema.get_data_type(column_name)
        values = [read_row[column_name] for read_row in read_rows]
        columns.append(Column(data_type, values))
    return RowBatch(schema, columns)
