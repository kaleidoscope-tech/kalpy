"""Records module for managing Kaleidoscope record operations.

This module provides classes and services for interacting with records in the Kaleidoscope system.
It includes functionality for filtering, sorting, managing record values, handling file attachments,
and searching records.

Classes:
    FilterRuleTypeEnum: Enumeration of available filter rule types for record filtering
    ViewFieldFilter: TypedDict for view-based field filter configuration
    ViewFieldSort: TypedDict for view-based field sort configuration
    FieldFilter: TypedDict for entity-based field filter configuration
    FieldSort: TypedDict for entity-based field sort configuration
    RecordValue: Model representing a single value within a record field
    Record: Model representing a complete record with all its fields and values
    RecordsService: Service class providing record-related API operations

The module uses Pydantic models for data validation and serialization, and integrates
with the KaleidoscopeClient for API communication.

Example:
    ```python
        # Get a record by ID
        record = client.records.get_record_by_id("record_uuid")

        # Add a value to a record field
        record.add_value(
            field_id="field_uuid",
            content="Experiment result",
            activity_id="activity_uuid"
        )

        # Get a field value
        value = record.get_value_content(field_id="field_uuid")

        # Update a field
        record.update_field(
            field_id="field_uuid",
            value="Updated value",
            activity_id="activity_uuid"
        )

        # Get activities associated with a record
        activities = record.get_activities()
    ```
"""

import logging
from datetime import datetime
from enum import Enum
import json
from kalpy._kaleidoscope_model import _KaleidoscopeBaseModel
from kalpy.client import KaleidoscopeClient
from pydantic import TypeAdapter
from typing import TYPE_CHECKING, Any, BinaryIO, Dict, List, Optional, TypedDict, Unpack

if TYPE_CHECKING:
    from kalpy.activities import Activity

_logger = logging.getLogger(__name__)


class FilterRuleTypeEnum(str, Enum):
    """Enumeration of filter rule types for record filtering operations.

    This enum defines all available filter rule types that can be applied to record properties.
    Filter rules are categorized into several groups:

    - **Existence checks**: `IS_SET`, `IS_EMPTY`
    - **Equality checks**: `IS_EQUAL`, `IS_NOT_EQUAL`, `IS_ANY_OF_TEXT`
    - **String operations**: `INCLUDES`, `DOES_NOT_INCLUDE`, `STARTS_WITH`, `ENDS_WITH`
    - **Membership checks**: `IS_IN`, `IS_NOT_IN`
    - **Set operations**: `VALUE_IS_SUBSET_OF_PROPS`, `VALUE_IS_SUPERSET_OF_PROPS`,
        `VALUE_HAS_OVERLAP_WITH_PROPS`, `VALUE_HAS_NO_OVERLAP_WITH_PROPS`,
        `VALUE_HAS_SAME_ELEMENTS_AS_PROPS`
    - **Numeric comparisons**: `IS_LESS_THAN`, `IS_LESS_THAN_EQUAL`, `IS_GREATER_THAN`,
        `IS_GREATER_THAN_EQUAL`
    - **Absolute date comparisons**: `IS_BEFORE`, `IS_AFTER`, `IS_BETWEEN`
    - **Relative date comparisons**:
        - Day-based: `IS_BEFORE_RELATIVE_DAY`, `IS_AFTER_RELATIVE_DAY`, `IS_BETWEEN_RELATIVE_DAY`
        - Week-based: `IS_BEFORE_RELATIVE_WEEK`, `IS_AFTER_RELATIVE_WEEK`, `IS_BETWEEN_RELATIVE_WEEK`,
            `IS_LAST_WEEK`, `IS_THIS_WEEK`, `IS_NEXT_WEEK`
        - Month-based: `IS_BEFORE_RELATIVE_MONTH`, `IS_AFTER_RELATIVE_MONTH`, `IS_BETWEEN_RELATIVE_MONTH`,
            `IS_THIS_MONTH`, `IS_NEXT_MONTH`
    - **Update tracking**: `IS_LAST_UPDATED_AFTER`

    Each enum value corresponds to a string representation used in filter configurations.
    """

    IS_SET = "is_set"
    IS_EMPTY = "is_empty"
    IS_EQUAL = "is_equal"
    IS_ANY_OF_TEXT = "is_any_of_text"
    IS_NOT_EQUAL = "is_not_equal"
    INCLUDES = "includes"
    DOES_NOT_INCLUDE = "does_not_include"
    IS_IN = "is_in"
    IS_NOT_IN = "is_not_in"
    VALUE_IS_SUBSET_OF_PROPS = "value_is_subset_of_props"
    VALUE_IS_SUPERSET_OF_PROPS = "value_is_superset_of_props"
    VALUE_HAS_OVERLAP_WITH_PROPS = "value_has_overlap_with_props"
    VALUE_HAS_NO_OVERLAP_WITH_PROPS = "value_has_no_overlap_with_props"
    VALUE_HAS_SAME_ELEMENTS_AS_PROPS = "value_has_same_elements_as_props"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    IS_LESS_THAN = "is_less_than"
    IS_LESS_THAN_EQUAL = "is_less_than_equal"
    IS_GREATER_THAN = "is_greater_than"
    IS_GREATER_THAN_EQUAL = "is_greater_than_equal"
    IS_BEFORE = "is_before"
    IS_AFTER = "is_after"
    IS_BETWEEN = "is_between"
    IS_BEFORE_RELATIVE_DAY = "is_before_relative_day"
    IS_AFTER_RELATIVE_DAY = "is_after_relative_day"
    IS_BETWEEN_RELATIVE_DAY = "is_between_relative_day"
    IS_BEFORE_RELATIVE_WEEK = "is_before_relative_week"
    IS_AFTER_RELATIVE_WEEK = "is_after_relative_week"
    IS_BETWEEN_RELATIVE_WEEK = "is_between_relative_week"
    IS_BEFORE_RELATIVE_MONTH = "is_before_relative_month"
    IS_AFTER_RELATIVE_MONTH = "is_after_relative_month"
    IS_BETWEEN_RELATIVE_MONTH = "is_between_relative_month"
    IS_LAST_WEEK = "is_last_week"
    IS_THIS_WEEK = "is_this_week"
    IS_NEXT_WEEK = "is_next_week"
    IS_THIS_MONTH = "is_this_month"
    IS_NEXT_MONTH = "is_next_month"
    IS_LAST_UPDATED_AFTER = "is_last_updated_after"


class ViewFieldFilter(TypedDict):
    """TypedDict for view-based field filter configuration.

    Attributes:
        key_field_id (Optional[str]): The ID of the key field to filter by.
        view_field_id (Optional[str]): The ID of the view field to filter by.
        filter_type (FilterRuleTypeEnum): The type of filter rule to apply.
        filter_prop (Any): The property value to filter against.
    """

    key_field_id: Optional[str]
    view_field_id: Optional[str]
    filter_type: FilterRuleTypeEnum
    filter_prop: Any


class ViewFieldSort(TypedDict):
    """TypedDict for view-based field sort configuration.

    Attributes:
        key_field_id (Optional[str]): The ID of the key field to sort by.
        view_field_id (Optional[str]): The ID of the view field to sort by.
        descending (bool): Whether to sort in descending order.
    """

    key_field_id: Optional[str]
    view_field_id: Optional[str]
    descending: bool


class FieldFilter(TypedDict):
    """TypedDict for entity-based field filter configuration.

    Attributes:
        field_id (Optional[str]): The ID of the field to filter by.
        filter_type (FilterRuleTypeEnum): The type of filter rule to apply.
        filter_prop (Any): The property value to filter against.
    """

    field_id: Optional[str]
    filter_type: FilterRuleTypeEnum
    filter_prop: Any


class FieldSort(TypedDict):
    """TypedDict for entity-based field sort configuration.

    Attributes:
        field_id (Optional[str]): The ID of the field to sort by.
        descending (bool): Whether to sort in descending order.
    """

    field_id: Optional[str]
    descending: bool


class RecordValue(_KaleidoscopeBaseModel):
    """Represents a single value entry in a record within the Kaleidoscope system.

    A RecordValue stores the actual content of a record along with metadata about when it was
    created and its relationships to parent records and operations.

    Attributes:
        id (str): UUID of the record value
        content (Any): The actual data value stored in this record. Can be of any type.
        created_at (Optional[datetime]): Timestamp indicating when this value was created.
            Defaults to None.
        record_id (Optional[str]): Identifier of the parent record this value belongs to.
            Defaults to None.
        operation_id (Optional[str]): Identifier of the operation that created or modified
            this value. Defaults to None.
    """

    content: Any
    created_at: Optional[datetime] = None  # data value
    record_id: Optional[str] = None  # data value
    operation_id: Optional[str] = None  # data value

    def __str__(self):
        return f"{self.content}"


class Record(_KaleidoscopeBaseModel):
    """Represents a record in the Kaleidoscope system.

    A Record is a core data structure that contains values organized by fields, can be associated
    with experiments, and may have sub-records. Records are identified by a unique ID and belong
    to an entity slice.

    Attributes:
        id (str): UUID of the record.
        created_at (datetime): The timestamp when the record was created.
        entity_slice_id (str): The ID of the entity slice this record belongs to.
        identifier_ids (List[str]): A list of identifier IDs associated with this record.
        record_values (Dict[str, List[RecordValue]]): A dictionary mapping field IDs to lists of record values.
        initial_operation_id (Optional[str]): The ID of the initial operation that created this record, if applicable.
        sub_record_ids (List[str]): A list of IDs for sub-records associated with this record.
    """

    created_at: datetime
    entity_slice_id: str
    identifier_ids: List[str]
    record_identifier: str
    record_values: Dict[str, List[RecordValue]]  # [field_id, values[]]
    initial_operation_id: Optional[str] = None
    sub_record_ids: List[str]

    def __str__(self):
        return f"{self.record_identifier}"

    def get_activities(self) -> List["Activity"]:
        """Retrieves a list of activities associated with this record.

        Returns:
            List[kalpy.activities.Activity]: A list of Activity objects related to this record.

        Note:
            If an exception occurs during the API request, it logs the error and returns an empty list.
        """
        try:
            resp = self._client._get("/records/" + self.id + "/operations")
            return self._client.activities._create_activity_list(resp)
        except Exception as e:
            _logger.error(f"Error fetching activities with this record: {e}")
            return []

    def add_value(
        self, field_id: str, content: Any, activity_id: Optional[str] = None
    ) -> None:
        """Adds a value to a specified field for a given activity.

        Args:
            field_id (str): The identifier of the field to which the value will be added.
            content (Any): The value/content to be saved for the field.
            activity_id (Optional[str]): The identifier of the activity. Defaults to None.
        """
        try:
            self._client._post(
                "/records/" + self.id + "/values",
                {"content": content, "field_id": field_id, "operation_id": activity_id},
            )
            return
        except Exception as e:
            _logger.error(f"Error adding this value: {e}")
            return

    def get_value_content(
        self,
        field_id: str,
        activity_id: Optional[str] = None,
        include_sub_record_values: Optional[bool] = False,
        sub_record_id: Optional[str] = None,
    ) -> Any | None:
        """Retrieves the content of a record value for a specified field.

        Optionally filtered by activity, sub-record, and inclusion of sub-record values.

        Args:
            field_id (str): The ID of the field to retrieve the value for.
            activity_id (Optional[str], optional): The ID of the activity to filter values by. Defaults to None.
            include_sub_record_values (Optional[bool], optional): Whether to include values from sub-records. Defaults to False.
            sub_record_id (Optional[str], optional): The ID of a specific sub-record to filter values by. Defaults to None.

        Returns:
            (Any | None): The content of the most recent matching record value, or None if no value is found.
        """
        values = self.record_values.get(field_id)
        if not values:
            return None

        # include key values in the activity data (record_id = None)
        if activity_id is not None:
            values = [
                value
                for value in values
                if (value.operation_id == activity_id) or value.record_id is None
            ]

        if not include_sub_record_values:
            # key values have None for the record_id
            values = [
                value
                for value in values
                if value.record_id == self.id or value.record_id is None
            ]

        if sub_record_id:
            values = [value for value in values if value.record_id == sub_record_id]

        sorted_values: List[RecordValue] = sorted(
            values,
            key=lambda x: x.created_at if x.created_at else datetime.min,
            reverse=True,
        )
        value = next(iter(sorted_values), None)
        return value.content if value else None

    def get_activity_data(self, activity_id: str) -> dict:
        """Retrieves activity data for a specific activity ID.

        Args:
            activity_id (str): The unique identifier of the activity.

        Returns:
            dict: A dictionary mapping field IDs to their corresponding values for the given activity.
                  Only fields with non-None values are included.
        """
        data = {}
        for field_id in self.record_values.keys():
            result = self.get_value_content(field_id, activity_id)
            if result is not None:
                data[field_id] = result

        return data

    def update_field(
        self, field_id: str, value: Any, activity_id: str | None
    ) -> RecordValue | None:
        """Updates a specific field of the record with the given value.

        Args:
            field_id (str): The ID of the field to update.
            value (Any): The new value to set for the field.
            activity_id (str | None): The ID of the activity associated with the update, or None if not an activity value

        Returns:
            (RecordValue | None): The updated record value if the operation is successful, otherwise None.
        """
        try:
            body = {"field_id": field_id, "content": value, "operation_id": activity_id}

            resp = self._client._post("/records/" + self.id + "/values", body)

            if resp is None or len(resp) == 0:
                return None

            return RecordValue.model_validate(resp.get("resource"))
        except Exception as e:
            _logger.error(f"Error updating the field: {e}")
            return None

    def update_field_file(
        self,
        field_id: str,
        file_name: str,
        file_data: BinaryIO,
        file_type: str,
        activity_id: Optional[str] = None,
    ) -> RecordValue | None:
        """Update a record value with a file.

        Args:
            field_id (str): The ID of the field to update.
            file_name (str): The name of the file to upload.
            file_data (BinaryIO): The binary data of the file.
            file_type (str): The MIME type of the file.
            activity_id (Optional[str], optional): The ID of the activity, if applicable. Defaults to None.

        Returns:
            (RecordValue | None): The updated record value if the operation is successful, otherwise None.
        """
        try:
            body = {
                "field_id": field_id,
            }

            if activity_id:
                body["operation_id"] = activity_id

            resp = self._client._post_file(
                "/records/" + self.id + "/values/file",
                (file_name, file_data, file_type),
                body,
            )

            if resp is None or len(resp) == 0:
                return None

            return RecordValue.model_validate(resp.get("resource"))
        except Exception as e:
            _logger.error(f"Error uploading file to field: {e}")
            return None

    def get_values(self) -> List[RecordValue]:
        """Retrieve all values associated with this record.

        Makes a GET request to fetch the values for the current record using its ID.
        If the request is successful, returns the list of record values. If the response
        is None or an error occurs during the request, returns an empty list.

        Returns:
            List[RecordValue]: A list of RecordValue objects associated with this record.
                               Returns an empty list if no values exist.

        Note:
            If an exception occurs during the API request, it logs the error and returns an empty list.
        """

        try:
            resp = self._client._get("/records/" + self.id + "/values")
            if resp is None:
                return []
            return resp
        except Exception as e:
            _logger.error(f"Error fetching values for this record: {e}")
            return []


class RecordsService:
    """Service class for managing records in Kaleidoscope.

    This service provides methods for creating, retrieving, and searching records,
    as well as managing record values and file uploads. It acts as an interface
    between the KaleidoscopeClient and Record objects.

    Example:
        ```python
        # Get a record by ID
        record = client.records.get_record_by_id("record_uuid")
        ```
    """

    def __init__(self, client: KaleidoscopeClient):
        self._client = client

    def _create_record(self, data: dict) -> Record:
        """Creates a new Record instance from the provided data.

        Validates the input data using the Record model, sets the client for the record,
        and returns the resulting Record object.

        Args:
            data (dict): The data to be validated and used for creating the Record.

        Returns:
            Record: The validated and initialized Record instance.
        """
        record = Record.model_validate(data)
        record._set_client(self._client)

        return record

    def _create_record_list(self, data: list[dict]) -> List[Record]:
        """Converts a dictionary of data into a list of Record objects and sets the client for each record.

        Args:
            data (list[dict]): The input data to be converted into Record objects.

        Returns:
            List[Record]: A list of Record objects with the client set.
        """
        records = TypeAdapter(List[Record]).validate_python(data)
        for record in records:
            record._set_client(self._client)

        return records

    def get_record_by_id(self, record_id: str) -> Record | None:
        """Retrieves a record by its unique identifier.

        Args:
            record_id (str): The unique identifier of the record to retrieve.

        Returns:
            (Record | None): The record object if found, otherwise None.
        """
        try:
            resp = self._client._get("/records/" + record_id)
            if resp is None:
                return None

            return self._create_record(resp)
        except Exception as e:
            _logger.error(f"Error fetching record {id}: {e}")
            return None

    def get_records_by_ids(
        self, record_ids: List[str], batch_size: int = 250
    ) -> List[Record]:
        """Retrieves records corresponding to the provided list of record IDs in batches.

        Args:
            record_ids (List[str]): A list of record IDs to retrieve.
            batch_size (int, optional): The number of record IDs to process per batch. Defaults to 250.

        Returns:
            List[Record]: A list of Record objects corresponding to the provided IDs.

        Note:
            If an exception occurs during the API request, it logs the error and returns an empty list.
        """
        try:
            all_records = []

            for i in range(0, len(record_ids), batch_size):
                batch = record_ids[i : i + batch_size]
                resp = self._client._get(f"/records?record_ids={",".join(batch)}")
                records = self._create_record_list(resp)
                all_records.extend(records)

            return all_records
        except Exception as e:
            _logger.error(f"Error fetching records {record_ids}: {e}")
            return []

    def get_record_by_key_values(self, key_values: dict[str, Any]) -> Record | None:
        """Retrieves a record that matches the specified key-value pairs.

        Args:
            key_values (dict[str, Any]): A dictionary containing key-value pairs that uniquely identify the record.

        Returns:
            (Record | None): The matching Record object if found, otherwise None.
        """
        try:
            resp = self._client._get(
                "/records/identifiers",
                {"records_key_field_to_value": json.dumps([key_values])},
            )
            if resp is None or len(resp) == 0:
                return None

            result = resp[0]
            if result.get("record"):
                return self._create_record(result.get("record"))
            return None
        except Exception as e:
            _logger.error(f"Error fetching records {key_values}: {e}")
            return None

    def get_or_create_record(self, key_values: dict[str, str]) -> Record | None:
        """Retrieves an existing record matching the provided key-value pairs, or creates a new one if none exists.

        Args:
            key_values (dict[str, str]): A dictionary containing key-value pairs to identify or create the record.

        Returns:
            (Record | None): The retrieved or newly created Record object if successful or None, if no record is found or created
        """
        try:
            resp = self._client._post(
                "/records",
                {"key_field_to_value": key_values},
            )
            if resp is None or len(resp) == 0:
                return None

            return self._create_record(resp)
        except Exception as e:
            _logger.error(f"Error getting or creating record {key_values}: {e}")
            return None

    class SearchRecordsQuery(TypedDict):
        """TypedDict for search records query parameters.

        Attributes:
            record_set_id (Optional[str]): The ID of the record set to search within.
            program_id (Optional[str]): The ID of the program associated with the records.
            entity_slice_id (Optional[str]): The ID of the entity slice to filter records.
            operation_id (Optional[str]): The ID of the operation to filter records.
            identifier_ids (Optional[List[str]]): List of identifier IDs to filter records.
            record_set_filters (Optional[List[str]]): List of filters to apply on record sets.
            view_field_filters (Optional[List[ViewFieldFilter]]): List of filters to apply on view fields.
            view_field_sorts (Optional[List[ViewFieldSort]]): List of sorting criteria for view fields.
            entity_field_filters (Optional[List[FieldFilter]]): List of filters to apply on entity fields.
            entity_field_sorts (Optional[List[FieldSort]]): List of sorting criteria for entity fields.
            search_text (Optional[str]): Text string to search for within records.
            limit (Optional[int]): Maximum number of records to return in the search results.
        """

        record_set_id: Optional[str]
        program_id: Optional[str]
        entity_slice_id: Optional[str]
        operation_id: Optional[str]
        identifier_ids: Optional[List[str]]
        record_set_filters: Optional[List[str]]
        view_field_filters: Optional[List[ViewFieldFilter]]
        view_field_sorts: Optional[List[ViewFieldSort]]
        entity_field_filters: Optional[List[FieldFilter]]
        entity_field_sorts: Optional[List[FieldSort]]
        search_text: Optional[str]
        limit: Optional[int]

    def search_records(self, **params: Unpack[SearchRecordsQuery]) -> list[str]:
        """Searches for records using the provided query parameters.

        Args:
            **params (Unpack[SearchRecordsQuery]): Keyword arguments representing search criteria. Non-string values will be JSON-encoded before being sent.

        Returns:
            list[str]: A list of record identifiers matching the search criteria.
            Returns an empty list is response is empty.

        Note:
            If an exception occurs during the API request, it logs the error and returns an empty list.
        """
        try:
            client_params = {
                key: (value if isinstance(value, str) else json.dumps(value))
                for key, value in params.items()
            }
            resp = self._client._get("/records/search", client_params)
            if resp is None:
                return []

            return resp
        except Exception as e:
            _logger.error(f"Error searching records {params}: {e}")
            return []

    def create_record_value_file(
        self,
        record_id: str,
        field_id: str,
        file_name: str,
        file_data: BinaryIO,
        file_type: str,
        activity_id: Optional[str] = None,
    ) -> RecordValue | None:
        """Creates a record value for a file and uploads it to the specified record.

        Args:
            record_id (str): The unique identifier of the record to which the file value will be added.
            field_id (str): The identifier of the field associated with the file value.
            file_name (str): The name of the file to be uploaded.
            file_data (BinaryIO): A binary stream representing the file data.
            file_type (str): The MIME type of the file.
            activity_id (Optional[str], optional): An optional activity identifier.

        Returns:
            (RecordValue | None): The created RecordValue object if successful, otherwise None.
        """
        try:
            body = {
                "field_id": field_id,
            }

            if activity_id:
                body["operation_id"] = activity_id

            resp = self._client._post_file(
                "/records/" + record_id + "/values/file",
                (file_name, file_data, file_type),
                body,
            )

            if resp is None or len(resp) == 0:
                return None

            return RecordValue.model_validate(resp.get("resource"))
        except Exception as e:
            _logger.error(f"Error uploading file to record field: {e}")
            return None
