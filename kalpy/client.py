"""Kaleidoscope API Client Module.

This module provides the main client class for interacting with the Kaleidoscope API,
along with a base model class for Kaleidoscope entities.

The KaleidoscopeClient provides access to various service endpoints including:

- activities: Manage activities
- imports: Import data into Kaleidoscope
- programs: Manage programs
- entity_types: Manage entity types
- records: Manage records
- fields: Manage fields
- experiments: Manage experiments
- record_views: Manage record views
- exports: Export data from Kaleidoscope

Attributes:
    PROD_API_URL (str): The production URL for the Kaleidoscope API.
    VALID_CONTENT_TYPES (list): List of acceptable content types for file downloads.
    TIMEOUT_MAXIMUM (int): Maximum timeout for API requests in seconds.

Example:
    ```python
        # instantiate client object
        client = KaleidoscopeClient(
            client_id="your_client_id",
            client_secret="your_client_secret"
        )

        # retrieve activities
        programs = client.activities.get_activities()
    ```
"""

from datetime import datetime, timedelta
import json
from json import JSONDecodeError
import requests
import urllib
from typing import Any, BinaryIO, Dict, Optional

PROD_API_URL = "https://api.kaleidoscope.bio"
"""The production URL for the Kaleidoscope API.

This is the default url used for the `KaleidoscopeClient`, in the event
no url is provided in the `KaleidoscopeClient`'s initialization"""

VALID_CONTENT_TYPES = [
    "text/csv",
    "chemical/x-mdl-sdfile",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
]
"""List of acceptable content types for file downloads.

Any file retrieved by the `KaleidoscopeClient` must be of one the above types"""

TIMEOUT_MAXIMUM = 10
"""Maximum timeout for API requests in seconds."""


class TokenResponse:
    access_token: str
    refresh_token: str
    expires_in: int


class KaleidoscopeClient:
    """A client for interacting with the Kaleidoscope API.

    This client provides a high-level interface to various Kaleidoscope services including
    imports, programs, entity types, records, fields, tasks, experiments, record views, and exports.
    It handles authentication using API key credentials and provides methods for making HTTP requests
    (GET, POST, PUT) to the API endpoints.

    Attributes:
        activities (ActivitiesService): Service for managing activities.
        dashboards (DashboardsService): Service for managing dashboards.
        workspace (WorkspaceService): Service for workspace-related operations.
        programs (ProgramsService): Service for managing programs.
        labels (LabelsService): Service for managing labels.
        entity_types (EntityTypesService): Service for managing entity types.
        entity_fields (EntityFieldsService): Service for managing entity fields.
        records (RecordsService): Service for managing records.
        record_views (RecordViewsService): Service for managing record views.
        imports (ImportsService): Service for managing data imports.
        exports (ExportsService): Service for managing data exports.
        property_fields (PropertyFieldsService): Service for managing property fields.

    Example:
        ```python
        client = KaleidoscopeClient(
            client_id="your_api_client_id",
            client_secret="your_api_client_secret"
        }
        # Use the client to interact with various services
        programs = client.activities.get_activities()
        ```
    """

    _client_id: str
    _client_secret: str

    _refresh_token: str
    _access_token: str
    _refresh_before: datetime

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        url: Optional[str] = None,
    ):
        """Initialize the Kaleidoscope API client.

        Sets up the client with API credentials and optional API URL, and initializes
        service interfaces for interacting with different API endpoints.

        Args:
            client_id (str): The API client ID for authentication.
            client_secret (str): The API client secret for authentication.
            url (Optional[str]): The base URL for the API. Defaults to the production
                API URL if not provided.
        """
        from kalpy.activities import ActivitiesService
        from kalpy.dashboards import DashboardsService
        from kalpy.entity_fields import EntityFieldsService
        from kalpy.entity_types import EntityTypesService
        from kalpy.exports import ExportsService
        from kalpy.imports import ImportsService
        from kalpy.labels import LabelsService
        from kalpy.programs import ProgramsService
        from kalpy.property_fields import PropertyFieldsService
        from kalpy.record_views import RecordViewsService
        from kalpy.records import RecordsService
        from kalpy.workspace import WorkspaceService

        self._api_url = url if url else PROD_API_URL

        self.activities = ActivitiesService(self)
        self.dashboards = DashboardsService(self)
        self.entity_fields = EntityFieldsService(self)
        self.entity_types = EntityTypesService(self)
        self.exports = ExportsService(self)
        self.imports = ImportsService(self)
        self.labels = LabelsService(self)
        self.property_fields = PropertyFieldsService(self)
        self.programs = ProgramsService(self)
        self.record_views = RecordViewsService(self)
        self.records = RecordsService(self)
        self.workspace = WorkspaceService(self)

        self._client_id = client_id
        self._client_secret = client_secret
        self._get_auth_token()

    def _update_tokens(self, data: TokenResponse):
        self._access_token = data.get("access_token")
        self._refresh_token = data.get("refresh_token")
        self._last_refreshed_at = datetime.now() + timedelta(
            seconds=data.get("expires_in") - (60 * 10)  # add a 10 minute buffer
        )

    def _get_auth_token(self):
        auth_resp = requests.post(
            self._api_url + "/auth/oauth/token",
            data={
                "grant_type": "client_credentials",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
            },
        )
        if auth_resp.status_code >= 400:
            print(
                f"Could not connect to server with client_id {self._client_id}: ",
                auth_resp.content,
            )
            return None

        self._update_tokens(auth_resp.json())

    def _refresh_auth_token(self):
        if self._refresh_token is None:
            return self._get_auth_token()

        auth_resp = requests.post(
            self._api_url + "/auth/oauth/token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": self._refresh_token,
            },
        )
        if auth_resp.status_code >= 400:
            print(
                f"Could not refresh access token: ",
                auth_resp.content,
            )
            return None

        self._update_tokens(auth_resp.json())

    def _get_headers(self):
        if datetime.now() > self._last_refreshed_at:
            self._refresh_auth_token()

        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self._access_token}",
        }

    def _post(self, url: str, payload: dict) -> Any:
        """Send a POST request to the specified URL with the given payload.

        Args:
            url (str): The endpoint URL (relative to the API base URL) to send the
                POST request to.
            payload (dict): The data to be sent in the body of the POST request.
                Should be serializable to JSON.

        Returns:
            Any: The JSON response from the server if the request is successful
            and the response is valid JSON. Returns None if the request fails
            or the response cannot be decoded.
        """

        resp = requests.post(
            self._api_url + url,
            data=json.dumps(payload),
            headers=self._get_headers(),
            timeout=TIMEOUT_MAXIMUM,
        )
        if resp.status_code >= 400:
            print(f"POST {url} received {resp.status_code}: ", resp.content)
            return None
        try:
            return resp.json()
        except JSONDecodeError:
            return None

    def _post_file(
        self, url: str, file_data: tuple[str, BinaryIO, str], body: Any = None
    ) -> Any:
        """Send a POST request with a file and optional JSON body.

        Args:
            url (str): The endpoint URL (relative to the API base URL).
            file_data (tuple[str, BinaryIO, str]): A tuple containing the file name,
                file object, and MIME type.
            body (Any): Optional data to be sent as JSON in the
                form data. Defaults to None.

        Returns:
            Any: The JSON response from the server if the request is successful.
            Returns None if the request fails or the response cannot be decoded.
        """
        files = {"file": file_data}

        form_data = {}
        if body:
            form_data["body"] = json.dumps(body)

        resp = requests.post(
            self._api_url + url,
            files=files,
            data=form_data,
            headers=self._get_headers(),
            timeout=TIMEOUT_MAXIMUM,
        )
        if resp.status_code >= 400:
            print(f"POST {url} received {resp.status_code}: ", resp.content)
            return None
        try:
            return resp.json()
        except JSONDecodeError:
            return None

    def _put(self, url: str, payload: dict) -> Any:
        """Send a PUT request to the specified URL with the provided payload.

        Args:
            url (str): The endpoint URL (relative to the base API URL).
            payload (dict): The data to be sent in the PUT request body.

        Returns:
            Any: The JSON response from the server if the request is successful.
            Returns None if the request fails or the response cannot be decoded.
        """

        resp = requests.put(
            self._api_url + url,
            data=json.dumps(payload),
            headers=self._get_headers(),
            timeout=TIMEOUT_MAXIMUM,
        )
        if resp.status_code >= 400:
            print(f"PUT {url} received {resp.status_code}: ", resp.content)
            return None
        try:
            return resp.json()
        except JSONDecodeError:
            return None

    def _get(self, url: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Send a GET request to the specified API endpoint with optional query parameters.

        Args:
            url (str): The API endpoint path to append to the base URL.
            params (Optional[Dict[str, Any]]): Dictionary of query parameters to
                include in the request. Defaults to None.

        Returns:
            Any: The JSON response from the server if the request is successful.
            Returns None if the request fails or the response cannot be decoded.
        """
        url = self._api_url + url
        if params:
            url += "?" + urllib.parse.urlencode(params)

        resp = requests.get(url, headers=self._get_headers(), timeout=TIMEOUT_MAXIMUM)
        if resp.status_code >= 400:
            print(f"GET {url} received {resp.status_code}", resp.content)
            return None
        try:
            return resp.json()
        except JSONDecodeError:
            return None

    def _get_file(
        self, url: str, download_path: str, params: Optional[Dict[str, Any]] = None
    ) -> str | None:
        """Download a file from the specified URL and save it to the given path.

        Args:
            url (str): The endpoint URL (relative to the API base URL) to download
                the file from.
            download_path (str): The local file path where the downloaded file
                will be saved.
            params (Optional[Dict[str, Any]]): Dictionary of query parameters to
                include in the request. Defaults to None.

        Returns:
            (str | None): The path to the downloaded file if successful. Returns None
            if the request fails or the response does not contain valid file data.

        Note:
            Only responses with valid content types (as defined in VALID_CONTENT_TYPES)
            are saved.
        """
        url = self._api_url + url
        if params:
            url += "?" + urllib.parse.urlencode(params)

        resp = requests.get(
            url, headers=self._get_headers(), stream=True, timeout=TIMEOUT_MAXIMUM
        )
        if resp.status_code >= 400:
            print(f"GET {url} received {resp.status_code}", resp.content)
            return None

        content_type = resp.headers.get("Content-Type", "")
        if content_type not in VALID_CONTENT_TYPES:
            print(
                f"Invalid Content-Type: {content_type}. Response does not contain valid file data."
            )
            return None

        with open(download_path, "wb") as f_download:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    f_download.write(chunk)

        return download_path

    def _delete(self, url: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Send a DELETE request to the specified API endpoint with optional query parameters.

        Args:
            url (str): The API endpoint path to append to the base URL.
            params (Optional[Dict[str, Any]]): Dictionary of query parameters to
                include in the request. Defaults to None.

        Returns:
            Any: The JSON response from the server if the request is successful.
            Returns None if the request fails or the response cannot be decoded.
        """
        url = self._api_url + url
        if params:
            url += "?" + urllib.parse.urlencode(params)

        resp = requests.delete(
            url, headers=self._get_headers(), timeout=TIMEOUT_MAXIMUM
        )
        if resp.status_code >= 400:
            print(f"DELETE {url} received {resp.status_code}", resp.content)
            return None
        try:
            return resp.json()
        except JSONDecodeError:
            return None
