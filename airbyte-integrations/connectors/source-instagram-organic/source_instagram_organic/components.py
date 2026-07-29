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
        # config['page_id'] is the Facebook Page linked to the Instagram Business
        # Account we actually want data from - the page access token generated
        # here is valid against the linked IG account's Graph API endpoints too,
        # so there's no separate Instagram auth step.
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

    Static per-stream field list instead of a schema-file lookup, because the
    Connector Builder's custom-components sandbox has no schema files on disk
    (schemas are embedded inline in the manifest instead).
    """

    config: Config
    parameters: InitVar[Mapping[str, Any]]

    DATE_TIME_PATHS_BY_STREAM = {
        "ig_media": ["timestamp"],
    }

    def __post_init__(self, parameters: Mapping[str, Any]):
        self.name = parameters.get("name")

    @staticmethod
    def _to_rfc3339(value: str) -> str:
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
