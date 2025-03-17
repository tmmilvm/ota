"""A column in a RowBatch."""

from ota.schema import DataType


class Column:
    _data_type: DataType
    _values: list

    def __init__(self, data_type: DataType, values: list) -> None:
        self._data_type = data_type
        self._values = values

    def __getitem__(self, item):
        """Returns the element corresponding to the given index.

        Args:
            item: The index.
        Returns:
            An element.
        """
        return self._values[item]

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
