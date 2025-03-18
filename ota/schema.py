from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class DataType(Enum):
    """Data types."""

    Int = 1
    Bool = 2


@dataclass()
class SchemaField:
    """A name and a data type."""

    name: str
    data_type: DataType


class Schema:
    """A container for pairs of names and data types.

    Attributes:
        _fields: The names and data types.
    """

    _fields: list[SchemaField]

    def __init__(self, fields: dict[str, DataType] | list[SchemaField]) -> None:
        """Creates a schema.

        Args:
            fields: The names and data types for the schema.
        """
        if isinstance(fields, dict):
            self._fields = [
                SchemaField(name, data_type)
                for name, data_type in fields.items()
            ]
        else:
            self._fields = fields

    def get_fields(self) -> list[SchemaField]:
        """Returns the schema fields.

        Returns:
            The schema names and data types.
        """
        return self._fields

    def get_field_names(self) -> list[str]:
        """Returns the schema field names.

        Returns:
            The field names.
        """
        return list(map(lambda field: field.name, self._fields))

    def get_data_type(self, field_name: str) -> DataType:
        """Returns the data type of field.

        Args:
            field_name: Field name.
        Returns:
            The data type.
        Raises:
            KeyError: When the field name is not in the schema.
        """
        return self._get_field(field_name).data_type

    def select(self, field_names: list[str]) -> Schema:
        """Returns a new schema with a subset of the fields.

        Args:
            field_names: Names of the fields.
        Returns:
            A schema containing the selected fields.
        Raises:
            KeyError: When some field name is not in the schema.
        """
        fields = list(map(self._get_field, field_names))
        return Schema(fields)

    def _get_field(self, field_name: str) -> SchemaField:
        for field in self._fields:
            if field.name == field_name:
                return field
        raise KeyError(f"{field_name} not in schema")
