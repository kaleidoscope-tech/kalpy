"""
Module for managing entity fields in Kaleidoscope.

This module provides classes and services for working with entity fields, which are the
schema definitions for data stored in the Kaleidoscope system. It includes:

- DataFieldTypeEnum: An enumeration of all supported field types
- EntityField: A model representing a field definition
- EntityFieldsService: A service class for retrieving and creating entity fields

Entity fields can be of two types:

- Key fields: Used to uniquely identify entities
- Data fields: Used to store additional information about entities

The service provides caching mechanisms to minimize API calls and includes error handling
for all network operations.

Classes:
    DataFieldTypeEnum: An enumeration of all supported field types
    EntityField: A model representing a field definition
    EntityFieldsService: A service class for retrieving and creating entity fields

Example:
    ```python
    # Get all key fields
    key_fields = client.entity_fields.get_key_fields()

    # Create or get a data field
    field = client.entity_fields.get_or_create_data_field(
        field_name="temperature",
        field_type=DataFieldTypeEnum.NUMBER
    )
    ```
"""

import logging
from datetime import datetime
from enum import Enum
from functools import lru_cache
from kalpy._kaleidoscope_model import _KaleidoscopeBaseModel
from kalpy.client import KaleidoscopeClient
from pydantic import TypeAdapter
from typing import List, Optional

_logger = logging.getLogger(__name__)


class DataFieldTypeEnum(str, Enum):
    """Enumeration of data field types supported by the system.

    This enum defines all possible types of data fields that can be used in the application.
    Each field type represents a specific kind of data structure and validation rules.

    Attributes:
        TEXT: Plain text field.
        NUMBER: Numeric field for storing numbers.
        QUALIFIED_NUMBER: Numeric field with additional qualifiers or units.
        SMILES_STRING: Field for storing SMILES (Simplified Molecular Input Line Entry System) notation.
        SELECT: Single selection field from predefined options.
        MULTISELECT: Multiple selection field from predefined options.
        MOLFILE: Field for storing molecular structure files.
        RECORD_REFERENCE: Reference to another record by record_id.
        FILE: Generic file attachment field.
        IMAGE: Image file field.
        DATE: Date field.
        URL: Web URL field.
        BOOLEAN: Boolean (true/false) field.
        EMAIL: Email address field.
        PHONE: Phone number field.
        FORMULA: Field for storing formulas or calculated expressions.
        PEOPLE: Field for referencing people/users.
        VOTES: Field for storing vote counts or voting data.
        XY_ARRAY: Field for storing XY coordinate arrays.
        DNA_OLIGO: Field for storing DNA oligonucleotide sequences.
        RNA_OLIGO: Field for storing RNA oligonucleotide sequences.
        PEPTID: Field for storing peptide sequences.
        PLASMID: Field for storing plasmid information.
        GOOGLE_DRIVE: Field for Google Drive file references.
        S3_FILE: Field for AWS S3 file references.
        SNOWFLAKE_QUERY: Field for Snowflake database query references.
    """

    TEXT = "text"
    NUMBER = "number"
    QUALIFIED_NUMBER = "qualified-number"

    SMILES_STRING = "smiles-string"
    SELECT = "select"
    MULTISELECT = "multiselect"
    MOLFILE = "molfile"
    RECORD_REFERENCE = "record-reference"  # value is a record_id
    FILE = "file"
    IMAGE = "image"
    DATE = "date"
    URL = "URL"
    BOOLEAN = "boolean"
    EMAIL = "email"
    PHONE = "phone"
    FORMULA = "formula"
    PEOPLE = "people"
    VOTES = "votes"
    XY_ARRAY = "xy-array"
    DNA_OLIGO = "dna-oligo"
    RNA_OLIGO = "rna-oligo"
    PEPTID = "peptide"
    PLASMID = "plasmid"
    GOOGLE_DRIVE = "google-drive-file"
    S3_FILE = "s3-file"
    SNOWFLAKE_QUERY = "snowflake-query"


class EntityField(_KaleidoscopeBaseModel):
    """Represents a field within an entity in the Kaleidoscope system.

    This class defines the structure and metadata for individual fields that belong
    to an entity, including type information, key status, and optional references.

    Attributes:
        id (str): The UUID of the field.
        created_at (datetime): Timestamp when the field was created.
        is_key (bool): Indicates whether this field is a key field for the entity.
        field_name (str): The name of the field.
        field_type (DataFieldTypeEnum): The data type of the field.
        ref_slice_id (Optional[str]): Optional reference to a slice ID for relational fields.
    """

    created_at: datetime
    is_key: bool
    field_name: str
    field_type: DataFieldTypeEnum
    ref_slice_id: Optional[str]

    def __str__(self):
        return f"{self.field_name}"


class EntityFieldsService:
    """Service class for managing key fields and data fields in Kaleidoscope.

    Entity fields can be of two types:
    
    - Key fields: Used to uniquely identify entities
    - Data fields: Used to store additional information about entities
    """

    def __init__(self, client: KaleidoscopeClient):
        self._client = client

    @lru_cache
    def get_key_fields(self) -> List[EntityField]:
        """Retrieve the key fields from the client.

        This method caches its values.

        Returns:
            List[EntityField]: A list of EntityField objects representing the key fields.

        Note:
            If an exception occurs during the API request, it logs the error,
            clears the cache, and returns an empty list.
        """
        try:
            resp = self._client._get("/key_fields")
            return TypeAdapter(List[EntityField]).validate_python(resp)
        except Exception as e:
            _logger.error(f"Error fetching key fields: {e}")
            self.get_key_fields.cache_clear()
            return []

    def get_key_field_by_name(self, name: str) -> EntityField | None:
        """Retrieve a key field by its name.

        Args:
            name (str): The name of the key field to retrieve.

        Returns:
            (EntityField | None): The EntityField object with the specified name if found, otherwise None.
        """
        fields = self.get_key_fields()
        return next(
            (f for f in fields if f.field_name == name),
            None,
        )

    def get_or_create_key_field(self, field_name: str) -> EntityField | None:
        """Retrieve an existing key field by name or create a new one if it does not exist.

        Args:
            field_name (str): The name of the key field to retrieve or create.

        Returns:
            (EntityField | None): The validated EntityField object corresponding to the specified key field,
            or None if an error occurs.
        """
        try:
            data = {"field_name": field_name}
            resp = self._client._post("/key_fields/", data)
            return EntityField.model_validate(resp)
        except Exception as e:
            _logger.error(f"Error getting or creating key field: {e}")
            return None

    @lru_cache
    def get_data_fields(self) -> List[EntityField]:
        """Retrieve the list of data fields available in the workspace.

        This method caches its values.

        Returns:
            List[EntityField]: A list of EntityField objects representing the data fields.

        Note:
            If an exception occurs during the API request, it logs the error,
            clears the cache, and returns an empty list.
        """
        try:
            resp = self._client._get("/data_fields")
            return TypeAdapter(List[EntityField]).validate_python(resp)
        except Exception as e:
            _logger.error(f"Error fetching data fields: {e}")
            self.get_data_fields.cache_clear()
            return []

    def get_data_field_by_name(self, name: str) -> EntityField | None:
        """Retrieve a data field object by its name.

        Args:
            name (str): The name of the data field to retrieve.

        Returns:
            (EntityField | None): The data field object with the specified name if found, otherwise None.
        """
        fields = self.get_data_fields()
        return next(
            (f for f in fields if f.field_name == name),
            None,
        )

    def get_or_create_data_field(
        self, field_name: str, field_type: DataFieldTypeEnum
    ) -> EntityField | None:
        """Create a new data field or return the existing one.

        Creates a new data field with the specified name and type, or returns the existing
        field if one with the given name already exists.

        Args:
            field_name (str): The name of the data field to create or retrieve.
            field_type (DataFieldTypeEnum): The type of the data field.

        Returns:
            (EntityField | None): The created or existing data field instance, or None if an error occurs.
        """
        try:
            data: dict = {
                "field_name": field_name,
                "field_type": field_type.value,
                "attrs": {},
            }
            resp = self._client._post("/data_fields/", data)
            return EntityField.model_validate(resp)
        except Exception as e:
            _logger.error(f"Error getting or creating data field: {e}")
            return None
