#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from dataclasses import InitVar, dataclass
from datetime import datetime
from http import HTTPStatus
from typing import Any, Mapping, MutableMapping, Optional, Union

import dpath.util
import requests
from requests import HTTPError

from airbyte_cdk.sources.declarative.auth.declarative_authenticator import NoAuth
from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.declarative.transformations import RecordTransformation
from airbyte_cdk.sources.declarative.types import Config, Record, StreamSlice, StreamState


@dataclass
class AuthenticatorFacebookPageAccessToken(NoAuth):
    config: Config
    page_id: Union[InterpolatedString, str]
    access_token: Union[InterpolatedString, str]

    def __post_init__(self, parameters: Mapping[str, Any]):
        self._page_id = InterpolatedString.create(self.page_id, parameters=parameters).eval(self.config)
        self._access_token = InterpolatedString.create(self.access_token, parameters=parameters).eval(self.config)

    def __call__(self, request: requests.PreparedRequest) -> requests.PreparedRequest:
        """Attach the page access token to params to authenticate on the HTTP request"""
        page_access_token = self.generate_page_access_token()
        request.prepare_url(url=request.url, params={"access_token": page_access_token})
        return request

    def generate_page_access_token(self) -> str:
        # We are expecting to receive User access token from config. To access
        # Pages API we need to generate Page access token. Page access tokens
        # can be generated from another Page access token (with the same page ID)
        # so if user manually set Page access token instead of User access
        # token it would be no problem unless it has wrong page ID.
        # https://developers.facebook.com/docs/pages/access-tokens#get-a-page-access-token
        try:
            r = requests.get(
                f"https://graph.facebook.com/{self._page_id}", params={"fields": "access_token", "access_token": self._access_token}
            )
            if r.status_code != HTTPStatus.OK:
                raise HTTPError(r.text)
            return r.json().get("access_token")
        except Exception as e:
            raise Exception(f"Error while generating page access token: {e}") from e


@dataclass
class CustomFieldTransformation(RecordTransformation):
    """
    Transform all 'date-time' fields to rfc3339 format.

    The original implementation dynamically discovered which fields need this
    by loading the stream's JSON schema file from disk via JsonFileSchemaLoader
    - that only works when this connector runs as its real installed package,
    with schemas/*.json alongside it. The Connector Builder's custom-components
    sandbox has no such files (schemas are now embedded inline in the manifest
    instead), so this uses a static per-stream field list - the actual set of
    date-time fields in Facebook's schema doesn't change at runtime, so there's
    nothing lost by hardcoding it. Derived from the connector's own schemas/*.json.
    """

    config: Config
    parameters: InitVar[Mapping[str, Any]]

    DATE_TIME_PATHS_BY_STREAM = {
        "page": ["leadgen_tos_acceptance_time"],
        "post": ["backdated_time", "created_time", "updated_time"],
        "post_insights": ["values/*/end_time"],
        "page_insights": ["values/*/end_time"],
        "ig_media": ["timestamp"],
    }

    def __post_init__(self, parameters: Mapping[str, Any]):
        self.name = parameters.get("name")

    @staticmethod
    def _to_rfc3339(value: str) -> str:
        # stdlib-only replacement for pendulum.parse(...).to_rfc3339_string() -
        # pendulum isn't available in the Connector Builder's custom-components
        # sandbox. datetime.fromisoformat handles Facebook's date-time formats
        # directly (ISO 8601 with Z, +0000, or +00:00 suffixes - verified).
        return datetime.fromisoformat(str(value)).isoformat()

    def _date_time_to_rfc3339(self, record: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
        """
        Transform 'date-time' items to RFC3339 format
        """
        date_time_paths = self.DATE_TIME_PATHS_BY_STREAM.get(self.name, [])
        for path in date_time_paths:
            if "*" not in path:
                if field_value := dpath.util.get(record, path, default=None):
                    dpath.util.set(record, path, self._to_rfc3339(field_value))
            else:
                if field_values := dpath.util.values(record, path):
                    for i, date_time_value in enumerate(field_values):
                        dpath.util.set(record, path.replace("*", str(i)), self._to_rfc3339(date_time_value))
        return record

    def transform(
        self,
        record: Record,
        config: Optional[Config] = None,
        stream_state: Optional[StreamState] = None,
        stream_slice: Optional[StreamSlice] = None,
    ) -> Record:
        return self._date_time_to_rfc3339(record)
