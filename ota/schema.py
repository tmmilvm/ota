from enum import Enum


class DataType(Enum):
    Int = 1


class Schema:
    # Field = namedtuple("Field", ["name", "data_type"])
    _fields: dict[str, DataType]

    def __init__(self, fields: dict[str, DataType]) -> None:
        self._fields = fields

    def get_fields(self) -> dict[str, DataType]:
        return self._fields

    def get_data_type(self, field_name: str) -> DataType:
        """Returns the data type of a field.

        Args:
            field_name: Field name.
        Returns:
            The data type.
        Raises:
            KeyError: When the field name is not in the schema.
        """
        self._check_field_exists(field_name)
        return self._fields[field_name]

    def select(self, field_names: list[str]) -> "Schema":
        map(self._check_field_exists, field_names)
        return Schema(
            dict(
                (field_name, self._fields[field_name])
                for field_name in field_names
            )
        )

    def _check_field_exists(self, field_name: str) -> None:
        if field_name not in self._fields:
            raise KeyError(f"{field_name} not in schema")
