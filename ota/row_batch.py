from ota.column import Column
from ota.schema import Schema


class RowBatch:
    _schema: Schema
    _columns: list[Column]

    def __init__(self, schema: Schema, columns: list[Column]) -> None:
        self._schema = schema
        self._columns = columns

    def get_schema(self) -> Schema:
        return self._schema

    def get_column(self, index) -> Column:
        return self._columns[index]

    def num_columns(self) -> int:
        return len(self._columns)

    def num_rows(self) -> int:
        return self._columns[0].size()

    def to_csv(self) -> str:
        csv_str = ""
        for row_index in range(self.num_rows()):
            for col_index in range(self.num_columns()):
                if col_index:
                    csv_str += ","
                csv_str += str(self._columns[col_index][row_index])
            csv_str += "\n"
        return csv_str
