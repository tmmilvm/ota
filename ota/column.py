"""A column in a RowBatch."""

from typing import Any

from ota.schema import DataType


class Column:
    _data_type: DataType
    _values: list[int | bool]

    def __init__(self, data_type: DataType, values: list[Any]) -> None:
        self._data_type = data_type

        if len(values) == 0 or values[0] is None:
            self._values = values
            return

        read_type = type(values[0])
        # TODO: Sort out this mess...
        if read_type is str:
            if data_type == DataType.Bool:
                self._values = list(
                    map(lambda s: s in ["true", "True"], values)
                )
            elif data_type == DataType.Int:
                self._values = list(map(int, values))
            else:
                raise RuntimeError(
                    f"No conversion from {read_type} to {data_type}"
                )
        elif read_type is int:
            if data_type == DataType.Int:
                self._values = values
            else:
                raise RuntimeError(
                    f"No conversion from {read_type} to {data_type}"
                )
        elif read_type is bool:
            if data_type == DataType.Bool:
                self._values = values
            else:
                raise RuntimeError(
                    f"No conversion from {read_type} to {data_type}"
                )
        else:
            raise RuntimeError(f"No conversion from {read_type} to {data_type}")

    def __getitem__(self, item: int) -> int | bool | None:
        """Returns the element corresponding to the given index.

        Args:
            item: The index.
        Returns:
            An element.
        """
        return self._values[item]

    def __setitem__(self, item: int, value: int | bool | None) -> None:
        self._values[item] = value

    def get_data_type(self) -> DataType:
        """Returns the column's data type.

        Returns:
            The data type.
        """
        return self._data_type

    def size(self) -> int:
        """Returns the number of elements in the column.

        Returns:
            The number of elements.
        """
        return len(self._values)
