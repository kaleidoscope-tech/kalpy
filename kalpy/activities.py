"""Activities Module for Kaleidoscope API Client.

This module provides comprehensive functionality for managing activities (tasks, experiments,
projects, stages, milestones, and design cycles) within the Kaleidoscope platform. It includes
models for activities, activity definitions, and properties, as well as service classes for
performing CRUD operations and managing activity workflows.

The module manages:

- Activity creation, updates, and status transitions
- Activity definitions
- Properties
- Records of activities
- User and group assignments
- Labels of activites
- Related programs
- Parent-child activity relationships
- Activity dependencies and scheduling

Classes:
    ActivityStatusEnum: Enumeration of possible activity statuses used across activity workflows.
    Property: Model representing a property (field) attached to entities, with update and file upload helpers.
    ActivityDefinition: Template/definition for activities (templates for programs, users, groups, labels, and properties).
    Activity: Core activity model (task/experiment/project) with cached relations, record accessors, and update helpers.
    ActivitiesService: Service class exposing CRUD and retrieval operations for activities and activity definitions.

Example:
    ```python
    # Create a new activity
    activity = client.activities.create_activity(
        title="Synthesis Experiment",
        activity_type="experiment",
        program_ids=["program-uuid", ...],
        assigned_user_ids=["user-uuid", ...]
    )

    # Update activity status
    activity.update(status=ActivityStatusEnum.IN_PROGRESS)

    # Add records to activity
    activity.add_records(["record-uuid"])

    # Get activity data
    record_data = activity.get_record_data()
    ```

Note:
    This module uses Pydantic for data validation and serialization. All datetime
    objects are timezone-aware and follow ISO 8601 format.
"""

from __future__ import annotations
import logging
from datetime import datetime
from enum import Enum
from functools import cached_property, lru_cache
from kalpy._kaleidoscope_model import _KaleidoscopeBaseModel
from kalpy.client import KaleidoscopeClient
from kalpy.entity_fields import DataFieldTypeEnum
from kalpy.programs import Program
from kalpy.labels import Label
from kalpy.workspace import WorkspaceUser, WorkspaceGroup
from pydantic import TypeAdapter
from typing import Any, BinaryIO, List, Literal, Optional, Union
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from kalpy.records import Record


_logger = logging.getLogger(__name__)


class ActivityStatusEnum(str, Enum):
    """Enumeration of possible activity status values.

    This enum defines all possible states that an activity can be in during its lifecycle,
    including general workflow states, review states, and domain-specific states for
    design, synthesis, testing, and compound selection processes.

    Attributes:
        REQUESTED (str): Activity has been requested but not yet started.
        TODO (str): Activity is queued to be worked on.
        IN_PROGRESS (str): Activity is currently being worked on.
        NEEDS_REVIEW (str): Activity requires review.
        BLOCKED (str): Activity is blocked by dependencies or issues.
        PAUSED (str): Activity has been temporarily paused.
        CANCELLED (str): Activity has been cancelled.
        IN_REVIEW (str): Activity is currently under review.
        LOCKED (str): Activity is locked from modifications.
        TO_REVIEW (str): Activity is ready to be reviewed.
        UPLOAD_COMPLETE (str): Upload process for the activity is complete.
        NEW (str): Newly created activity.
        IN_DESIGN (str): Activity is in the design phase.
        READY_FOR_MAKE (str): Activity is ready for manufacturing/creation.
        IN_SYNTHESIS (str): Activity is in the synthesis phase.
        IN_TEST (str): Activity is in the testing phase.
        IN_ANALYSIS (str): Activity is in the analysis phase.
        PARKED (str): Activity has been parked for later consideration.
        COMPLETE (str): Activity has been completed.
        IDEATION (str): Activity is in the ideation phase.
        TWO_D_SELECTION (str): Activity is in 2D selection phase.
        COMPUTATION (str): Activity is in the computation phase.
        COMPOUND_SELECTION (str): Activity is in the compound selection phase.
        SELECTED (str): Activity or compound has been selected.
        QUEUE_FOR_SYNTHESIS (str): Activity is queued for synthesis.
        DATA_REVIEW (str): Activity is in the data review phase.
        DONE (str): Activity is done.
    """

    REQUESTED = "requested"
    TODO = "to do"
    IN_PROGRESS = "in progress"
    NEEDS_REVIEW = "needs review"
    BLOCKED = "blocked"
    PAUSED = "paused"
    CANCELLED = "cancelled"
    IN_REVIEW = "in review"
    LOCKED = "locked"

    TO_REVIEW = "to review"
    UPLOAD_COMPLETE = "upload complete"

    NEW = "new"
    IN_DESIGN = "in design"
    READY_FOR_MAKE = "ready for make"
    IN_SYNTHESIS = "in synthesis"
    IN_TEST = "in test"
    IN_ANALYSIS = "in analysis"
    PARKED = "parked"
    COMPLETE = "complete"

    IDEATION = "ideation"
    TWO_D_SELECTION = "2D selection"
    COMPUTATION = "computation"
    COMPOUND_SELECTION = "compound selection"
    SELECTED = "selected"
    QUEUE_FOR_SYNTHESIS = "queue for synthesis"
    DATA_REVIEW = "data review"

    DONE = "done"


type ActivityType = Union[
    Literal["task"],
    Literal["experiment"],
    Literal["project"],
    Literal["stage"],
    Literal["milestone"],
    Literal["cycle"],
]
"""Type alias representing the valid types of activities in the system.

This type defines the allowed string values for the `activity_type` field
in Activity and ActivityDefinition models.
"""

ACTIVITY_TYPE_TO_LABEL = {
    "task": "Task",
    "experiment": "Experiment",
    "project": "Project",
    "stage": "Stage",
    "milestone": "Milestone",
    "cycle": "Design cycle",
}
"""Dictionary mapping activity type keys to their human-readable labels.

This mapping is used to convert the internal `activity_type` identifiers
into display-friendly strings for UI and reporting purposes.
"""


class Property(_KaleidoscopeBaseModel):
    """Represents a property in the Kaleidoscope system.

    A Property is a data field associated with an entity that contains a value of a specific type.
    It includes metadata about when and by whom it was created/updated, and provides methods
    to update its content.

    Attributes:
        id (str): UUID of the property.
        property_field_id (str): UUID to the property field that defines this
            property's schema.
        content (Any): The actual value/content stored in this property.
        created_at (datetime): Timestamp when the property was created.
        last_updated_by (str): UUID of the user who last updated this property.
        created_by (str): UUID of the user who created this property.
        property_name (str): Human-readable name of the property.
        field_type (DataFieldTypeEnum): The data type of this property's content.
    """

    property_field_id: str
    content: Any
    created_at: datetime
    last_updated_by: str
    created_by: str
    property_name: str
    field_type: DataFieldTypeEnum

    def __str__(self):
        return f"{self.property_name}:{self.content}"

    def update_property(self, property_value: Any) -> None:
        """Update the property with a new value.

        Args:
            property_value (Any): The new value to set for the property.
        """
        try:
            resp = self._client._put(
                "/properties/" + self.id, {"content": property_value}
            )
            if resp:
                for key, value in resp.items():
                    if hasattr(self, key):
                        setattr(self, key, value)
        except Exception as e:
            _logger.error(f"Error updating property {self.id}: {e}")
            return None

    def update_property_file(
        self,
        file_name: str,
        file_data: BinaryIO,
        file_type: str,
    ) -> dict | None:
        """Update a property by uploading a file.

        Args:
            file_name (str): The name of the file to be updated.
            file_data (BinaryIO): The binary data of the file to be updated.
            file_type (str): The MIME type of the file to be updated.

        Returns:
            (dict or None): A dict of response JSON data (contains reference to the
                uploaded file) if request successful, otherwise None.
        """
        try:
            resp = self._client._post_file(
                "/properties/" + self.id + "/file",
                (file_name, file_data, file_type),
            )
            if resp is None or len(resp) == 0:
                return None

            return resp
        except Exception as e:
            _logger.error(f"Error adding file to property {self.id}: {e}")
            return None


class ActivityDefinition(_KaleidoscopeBaseModel):
    """Represents the definition of an activity in the Kaleidoscope system.

    An ActivityDefinition contains a template for the metadata about a task or activity,
    including associated programs, users, groups, labels, and properties.

    Attributes:
        id (str): UUID of the Activity Definition.
        program_ids (List[str]): List of program UUIDs associated with this activity.
        title (str): The title of the activity.
        activity_type (ActivityType): The type/category of the activity.
        status (Optional[ActivityStatusEnum]): The current status of the activity.
            Defaults to None if not specified.
        assigned_user_ids (List[str]): List of user IDs assigned to this activity.
        assigned_group_ids (List[str]): List of group IDs assigned to this activity.
        label_ids (List[str]): List of label identifiers associated with this activity.
        properties (List[Property]): List of properties that define additional
            characteristics of the activity.
        external_id (Optional[str]): The id of the activity definition if it was imported from an external source
    """

    program_ids: List[str]
    title: str
    activity_type: ActivityType
    status: Optional[ActivityStatusEnum] = None
    assigned_user_ids: List[str]
    assigned_group_ids: List[str]
    label_ids: List[str]
    properties: List[Property]
    external_id: Optional[str] = None

    def __str__(self):
        return f"{self.id}:{self.title}"

    @cached_property
    def activities(self) -> "Activity" | None:
        """Get the activities for this activity definition.

        Returns:
            List[Activity]: The activities associated with this
                activity definition.

        Note:
            This is a cached property.
        """
        return [
            a
            for a in self._client.activities.get_activities()
            if a.definition_id == self.id
        ]


class Activity(_KaleidoscopeBaseModel):
    """Represents an activity (e.g. task or experiment) within the Kaleidoscope system.

    An Activity is a unit of work that can be assigned to users or groups, have dependencies,
    and contain associated records and properties. Activities can be organized hierarchically
    with parent-child relationships and linked to programs.

    Attributes:
        id (str): Unique identifier for the model instance.
        created_at (datetime): The timestamp when the activity was created.
        parent_id (Optional[str]): The ID of the parent activity, if this is a child activity.
        child_ids (List[str]): List of child activity IDs.
        definition_id (Optional[str]): The ID of the activity definition template.
        program_ids (List[str]): List of program IDs this activity belongs to.
        activity_type (ActivityType): The type/category of the activity.
        title (str): The title of the activity.
        description (Any): Detailed description of the activity.
        status (ActivityStatusEnum): Current status of the activity.
        assigned_user_ids (List[str]): List of user IDs assigned to this activity.
        assigned_group_ids (List[str]): List of group IDs assigned to this activity.
        due_date (Optional[datetime]): The deadline for completing the activity.
        start_date (Optional[datetime]): The scheduled start date for the activity.
        duration (Optional[int]): Expected duration of the activity.
        completed_at_date (Optional[datetime]): The timestamp when the activity was completed.
        dependencies (List[str]): List of activity IDs that this activity depends on.
        label_ids (List[str]): List of label IDs associated with this activity.
        is_draft (bool): Whether the activity is in draft status.
        properties (List[Property]): List of custom properties associated with the activity.
        external_id (Optional[str]): The id of the activity if it was imported from an external source
        record_ids (List[str]): List of record IDs linked to this activity.
    """

    created_at: datetime
    parent_id: Optional[str] = None
    child_ids: List[str]
    definition_id: Optional[str] = None
    program_ids: List[str]
    activity_type: ActivityType
    title: str
    description: Any
    status: ActivityStatusEnum
    assigned_user_ids: List[str]
    assigned_group_ids: List[str]
    due_date: Optional[datetime] = None
    start_date: Optional[datetime] = None
    duration: Optional[int] = None
    completed_at_date: Optional[datetime] = None
    dependencies: List[str]
    label_ids: List[str]
    is_draft: bool
    properties: List[Property]
    external_id: Optional[str] = None

    # operation fields
    all_record_ids: List[str]

    def __str__(self):
        return f"{self.id}:{self.title}"

    @cached_property
    def activity_definition(self) -> "ActivityDefinition" | None:
        """Get the activity definition for this activity.

        Returns:
            (ActivityDefinition or None): The activity definition associated with this
                activity. If the activity has no definition, returns None.

        Note:
            This is a cached property.
        """
        if self.definition_id:
            return self._client.activities.get_definition_by_id(self.definition_id)
        else:
            return None

    @cached_property
    def assigned_users(self) -> List[WorkspaceUser]:
        """Get the assigned users for this activity.

        Returns:
            List[WorkspaceUser]: The users assigned to this activity.

        Note:
            This is a cached property.
        """
        return self._client.workspace.get_members_by_ids(self.assigned_user_ids)

    @cached_property
    def assigned_groups(self) -> List[WorkspaceGroup]:
        """Get the assigned groups for this activity.

        Returns:
            List[WorkspaceGroup]: The groups assigned to this activity.

        Note:
            This is a cached property.
        """
        return self._client.workspace.get_groups_by_ids(self.assigned_group_ids)

    @cached_property
    def labels(self) -> List[Label]:
        """Get the labels for this activity.

        Returns:
            List[Label]: The labels associated with this activity.

        Note:
            This is a cached property.
        """
        return self._client.labels.get_labels_by_ids(self.label_ids)

    @cached_property
    def programs(self) -> List[Program]:
        """Retrieve the programs associated with this activity.

        Returns:
            List[Program]: A list of Program instances fetched by their IDs.

        Note:
            This is a cached property.
        """
        return self._client.programs.get_programs_by_ids(self.program_ids)

    @cached_property
    def child_activities(self) -> List["Activity"]:
        """Retrieve the child activities associated with this activity.

        Returns:
            List[Activity]: A list of Activity objects representing the child activities.

        Note:
            This is a cached property.
        """
        try:
            resp = self._client._get("/activities/" + self.id + "/activities")
            return self._client.activities._create_activity_list(resp)
        except Exception as e:
            _logger.error(f"Error fetching child activities: {e}")
            return []

    @cached_property
    def records(self) -> List["Record"]:
        """Retrieve the records associated with this activity.

        Returns:
            List[Record]: A list of Record objects corresponding to the activity.

        Note:
            This is a cached property.
        """
        try:
            resp = self._client._get("/operations/" + self.id + "/records")
            return self._client.records._create_record_list(resp)
        except Exception as e:
            _logger.error(f"Error fetching records: {e}")
            return []

    def get_record(self, identifier: str) -> bool:
        """Retrieves the record with the given identifier if it is in the operation

        Returns:
            (Record or None): The record if it is in the operation, otherwise None
        """
        return next(
            (r for r in self.records if r.record_identifier == identifier),
            None,
        )

    def has_record(self, identifier: str) -> bool:
        """Retrieve whether a record with the given identifier is in the operation

        Returns:
            bool: Whether the record is in the operation
        """
        return self.get_record(identifier) != None

    def update(self, **kwargs: Any) -> None:
        """Update the activity with the provided keyword arguments.

        Args:
            **kwargs: Arbitrary keyword arguments representing fields to update
                for the activity.
        """
        try:
            resp = self._client._put("/activities/" + self.id, kwargs)
            if resp:
                for key, value in resp.items():
                    if hasattr(self, key):
                        setattr(self, key, value)
        except Exception as e:
            _logger.error(f"Error updating activity: {e}")
            return None

    def add_records(self, record_ids: List[str]) -> None:
        """Add a list of record IDs to the activity.

        Args:
            record_ids (List[str]): A list of record IDs to be added to the activity.
        """
        try:
            self._client._put(
                "/operations/" + self.id + "/records", {"record_ids": record_ids}
            )
        except Exception as e:
            _logger.error(f"Error adding record: {e}")
            return None

    def get_record_data(self) -> List[dict]:
        """Retrieve data from all this activity's associated records.

        Returns:
            List[dict]: A list containing the activity data for each record,
                obtained by calling get_activity_data with the current activity's ID.
        """
        data = []
        for record in self.records:
            data.append(record.get_activity_data(self.id))
        return data


class ActivitiesService:
    """Service class for managing activities in the Kaleidoscope platform.

    This service provides methods to create, retrieve, and manage activities
    (tasks/experiments) and their definitions within a Kaleidoscope workspace.
    It handles activity lifecycle operations including creation, retrieval by
    ID or associated records, and batch operations.

    Note:
        Some methods use LRU caching to improve performance. Cache is cleared on errors.
    """

    def __init__(self, client: KaleidoscopeClient):
        self._client = client

    def _create_activity(self, data: dict) -> Activity:
        """Convert a dictionary of activity data into a validated Activity object.

        Args:
            data (dict): A dictionary containing the activity information.

        Returns:
            Activity: An activity object created from the provided data, with the
                client set.

        Raises:
            ValidationError: If the data could not be validated as an Activity.
        """
        activity = Activity.model_validate(data)
        activity._set_client(self._client)

        return activity

    def _create_activity_list(self, data: list[dict]) -> List[Activity]:
        """Convert input data into a list of Activity objects.

        Args:
            data (list[dict]): The input data to be converted into Activity objects.

        Returns:
            List[Activity]: A list of Activity objects with clients set.

        Raises:
            ValidationError: If the data could not be validated as a list of
                Activity objects.
        """
        activities = TypeAdapter(List[Activity]).validate_python(data)
        for activity in activities:
            activity._set_client(self._client)

        return activities

    def create_activity(
        self,
        title: str,
        activity_type: ActivityType,
        program_ids: Optional[list[str]] = [],
        activity_definition_id: Optional[str] = None,
        assigned_user_ids: Optional[List[str]] = [],
        start_date: Optional[datetime] = None,
        duration: Optional[int] = None,
    ) -> Activity | None:
        """Create a new activity.

        Args:
            title (str): The title/name of the activity.
            activity_type (ActivityType): The type of activity (e.g. task, experiment, etc.).
            program_ids (Optional[list[str]]): List of program IDs to associate with
                the activity. Defaults to None.
            activity_definition_id (Optional[str]): ID of the activity definition.
                Defaults to None.
            assigned_user_ids (Optional[List[str]]): List of user IDs to assign to
                the activity. Defaults to None.
            start_date (Optional[datetime]): Start date for the activity. Defaults to None.
            duration (Optional[int]): Duration in days for the activity. Defaults to None.

        Returns:
            (Activity or None): The newly created activity instance or None if activity
                creation was not successful.
        """
        try:
            start_date_formatted = start_date.isoformat() if start_date else None
            payload = {
                "program_ids": program_ids,
                "title": title,
                "activity_type": activity_type,
                "definition_id": activity_definition_id,
                "record_ids": [],
                "assigned_user_ids": assigned_user_ids,
                "start_date": start_date_formatted,
                "duration": duration,
            }
            resp = self._client._post("/activities", payload)
            return self._create_activity(resp[0])
        except Exception as e:
            _logger.error(f"Error creating activity {title}: {e}")
            return None

    @lru_cache
    def get_activities(self) -> List[Activity]:
        """Retrieve all activities in the workspace, including experiments.

        Returns:
            List[Activity]: A list of Activity objects representing the activities
                in the workspace.

        Note:
            This method caches its results. If an exception occurs, logs the error,
            clears the cache, and returns an empty list.
        """
        try:
            resp = self._client._get("/activities")
            return self._create_activity_list(resp)
        except Exception as e:
            _logger.error(f"Error fetching activities: {e}")
            self.get_activities.cache_clear()
            return []

    def get_activity_by_id(self, activity_id: str) -> Activity | None:
        """Retrieve an activity by its unique identifier.

        Args:
            activity_id (str): The unique identifier of the activity to retrieve.

        Returns:
            (Activity or None): The Activity object if found, otherwise None.
        """
        try:
            resp = self._client._get("/activities/" + activity_id)
            if resp is None:
                return None

            return self._create_activity(resp)
        except Exception as e:
            _logger.error(f"Error fetching activity {activity_id}: {e}")
            return None

    def get_activity_by_external_id(self, external_id: str) -> Activity | None:
        """Retrieve an activity by its external identifier.

        Args:
            external_id (str): The external identifier of the activity to retrieve.

        Returns:
            (Activity or None): The Activity object if found, otherwise None.
        """
        activities = self.get_definitions()
        return next(
            (a for a in activities if a.external_id == external_id),
            None,
        )

    def get_activities_by_ids(
        self, ids: List[str], batch_size: int = 250
    ) -> List[Activity]:
        """Fetch activities by their IDs in batches.

        Args:
            ids (List[str]): A list of activity ID strings to fetch.
            batch_size (int): The number of IDs to include in each batch request.
                Defaults to 250.

        Returns:
            List[Activity]: A list of Activity objects corresponding to the provided IDs.

        Note:
            If an exception occurs, logs the error and returns an empty list.
        """
        try:
            all_activities = []

            for i in range(0, len(ids), batch_size):
                batch = ids[i : i + batch_size]
                resp = self._client._get(f"/activities?activity_ids={",".join(batch)}")
                activities = self._create_activity_list(resp)
                all_activities.extend(activities)

            return all_activities
        except Exception as e:
            _logger.error(f"Error fetching activities: {e}")
            return []

    @lru_cache
    def get_definitions(self) -> List[ActivityDefinition]:
        """Retrieve all available activity definitions.

        Returns:
            List[ActivityDefinition]: All activity definitions in the workspace.

        Raises:
            ValidationError: If the data could not be validated as an ActivityDefinition.

        Note:
            This method caches its results. If an exception occurs, logs the error,
            clears the cache, and returns an empty list.
        """
        try:
            resp = self._client._get("/activity_definitions")
            return TypeAdapter(List[ActivityDefinition]).validate_python(resp)
        except Exception as e:
            _logger.error(f"Error fetching activity definitions: {e}")
            self.get_definitions.cache_clear()
            return []

    def get_definition_by_name(self, name: str) -> ActivityDefinition | None:
        """Retrieve an activity definition by name.

        Args:
            name (str): The name of the activity definition.

        Returns:
            (ActivityDefinition or None): The activity definition if found, None otherwise.
        """
        definitions = self.get_definitions()
        return next(
            (d for d in definitions if d.title == name),
            None,
        )

    def get_definition_by_id(self, definition_id: str) -> ActivityDefinition | None:
        """Retrieve an activity definition by ID.

        Args:
            definition_id (str): Unique identifier for the activity definition.

        Returns:
            (ActivityDefinition or None): The activity definition if found, None otherwise.
        """
        definitions = self.get_definitions()
        return next(
            (d for d in definitions if d.id == definition_id),
            None,
        )

    def get_activity_definition_by_external_id(
        self, external_id: str
    ) -> ActivityDefinition | None:
        """Retrieve an activity definition by its external identifier.

        Args:
            external_id (str): The external identifier of the activity definition to retrieve.

        Returns:
            (ActivityDefinition or None): The ActivityDefinition object if found, otherwise None.
        """
        definitions = self.get_definitions()
        return next(
            (d for d in definitions if d.external_id == external_id),
            None,
        )

    def get_activities_with_record(self, record_id: str) -> List[Activity]:
        """Retrieve all activities that contain a specific record.

        Args:
            record_id (str): Unique identifier for the record.

        Returns:
            List[Activity]: Activities that include the specified record.

        Note:
            If an exception occurs, logs the error and returns an empty list.
        """
        try:
            resp = self._client._get("/records/" + record_id + "/operations")
            return self._create_activity_list(resp)
        except Exception as e:
            _logger.error(f"Error fetching activity with record {record_id}: {e}")
            return []
