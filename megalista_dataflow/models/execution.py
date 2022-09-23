# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from enum import Enum
from typing import Dict, List, Union, Any
import logging
from apache_beam.typehints.decorators import with_output_types


class DestinationType(Enum):
    (
        CM_OFFLINE_CONVERSION,
        ADS_OFFLINE_CONVERSION,
        ADS_OFFLINE_CONVERSION_CALLS,
        ADS_SSD_UPLOAD,
        ADS_ENHANCED_CONVERSION,
        ADS_ENHANCED_CONVERSION_LEADS,
        ADS_CUSTOMER_MATCH_CONTACT_INFO_UPLOAD,
        ADS_CUSTOMER_MATCH_MOBILE_DEVICE_ID_UPLOAD,
        ADS_CUSTOMER_MATCH_USER_ID_UPLOAD,
        GA_USER_LIST_UPLOAD,
        APPSFLYER_S2S_EVENTS,
        GA_MEASUREMENT_PROTOCOL,
        GA_DATA_IMPORT,
        GA_4_MEASUREMENT_PROTOCOL,
        DV_CUSTOMER_MATCH_CONTACT_INFO_UPLOAD,
        DV_CUSTOMER_MATCH_DEVICE_ID_UPLOAD,
        UPLOADED_UUID,  # schema verification purposes
        UPLOADED_GCLID_TIME  # schema verification purposes
    ) = range(18)

    def __eq__(self, other):
        if other is None:
            return False
        return self.name == other.name


class SourceType(Enum):
    BIG_QUERY, FILE = range(2)


class TransactionalType(Enum):
    """
        Distinct types to handle data uploading deduplication.
        NOT_TRANSACTION: don't handle.
        UUID: Expect a 'uuid' field in the source table as a unique identifier to each row.
        GCLID_DATE_TIME: Expect 'gclid' and 'time' fields in the source table as unique identifiers to each row.
    """
    (
        NOT_TRANSACTIONAL,
        UUID,
        GCLID_TIME,
    ) = range(3)


class AccountConfig:
    def __init__(
        self,
        google_ads_account_id: str,
        mcc: bool,
        google_analytics_account_id: str,
        campaign_manager_profile_id: str,
        app_id: str,
    ):
        self._google_ads_account_id = google_ads_account_id
        self._mcc = mcc
        self._google_analytics_account_id = google_analytics_account_id
        self._campaign_manager_profile_id = campaign_manager_profile_id
        self._app_id = app_id

    @property
    def google_ads_account_id(self) -> str:
        return self._google_ads_account_id

    @property
    def mcc(self) -> bool:
        return self._mcc

    @property
    def google_analytics_account_id(self) -> str:
        return self._google_analytics_account_id

    @property
    def campaign_manager_profile_id(self) -> str:
        return self._campaign_manager_profile_id

    @property
    def app_id(self) -> str:
        return self._app_id

    def to_dict(self):
        return {
            'google_ads_account_id': self.google_ads_account_id,
            'mcc': self.mcc,
            'google_analytics_account_id': self.google_analytics_account_id,
            'campaign_manager_profile_id': self.campaign_manager_profile_id,
            'app_id': self.app_id,
        }

    @staticmethod
    def from_dict(dict_account_config):
        return AccountConfig(
            dict_account_config['google_ads_account_id'],
            dict_account_config['mcc'],
            dict_account_config['google_analytics_account_id'],
            dict_account_config['campaign_manager_profile_id'],
            dict_account_config['app_id'],
        )

    def __str__(self) -> str:
        return (
            f"\n[Account Config]\n\t"
            f"Google Ads Customer Id: {self.google_ads_account_id}\n\t"
            f"Google Ads MCC: {self._mcc}\n\t"
            f"Google Analytics Account Id: {self.google_analytics_account_id}\n\t"
            f"Campaign Manager Account Id: {self.campaign_manager_profile_id}\n\t"
            f"Play Store App Id: {self.app_id}"
        )

    def __eq__(self, other):
        return (
            self.google_ads_account_id == other.google_ads_account_id
            and self.google_analytics_account_id == other.google_analytics_account_id
            and self.campaign_manager_profile_id == other.campaign_manager_profile_id
            and self.app_id == other.app_id
        )

    def __hash__(self):
        return hash(
            (
                self.google_ads_account_id,
                self.google_analytics_account_id,
                self.campaign_manager_profile_id,
                self.app_id,
            )
        )


class Source:
    def __init__(
        self, source_name: str, source_type: SourceType, source_metadata: List[str]
    ):
        self._source_name = source_name
        self._source_type = source_type
        self._source_metadata = source_metadata

    @property
    def source_name(self) -> str:
        return self._source_name

    @property
    def source_type(self) -> SourceType:
        return self._source_type

    @property
    def source_metadata(self) -> List[str]:
        return self._source_metadata

    def to_dict(self):
        return {
            'source_name': self.source_name,
            'source_type': self.source_type.name,
            'source_metadata': self.source_metadata,
        }

    @staticmethod
    def from_dict(dict_source):
        return Source(
            dict_source['source_name'],
            SourceType[dict_source['source_type']],
            dict_source['source_metadata']
        )

    def __eq__(self, other):
        return (
            self.source_name == other.source_name
            and self.source_type == other.source_type
            and self.source_metadata == other.source_metadata
        )

    def __hash__(self):
        return hash(
            (
                self.source_name,
                self.source_type,
                self.source_metadata[0],
                self.source_metadata[1],
            )
        )


class Destination:
    def __init__(
        self,
        destination_name: str,
        destination_type: DestinationType,
        destination_metadata: List[str],
    ):
        self._destination_name = destination_name
        self._destination_type = destination_type
        self._destination_metadata = destination_metadata

    @property
    def destination_name(self) -> str:
        return self._destination_name

    @property
    def destination_type(self) -> DestinationType:
        return self._destination_type

    @property
    def destination_metadata(self) -> List[str]:
        return self._destination_metadata

    def to_dict(self):
        return {
            'destination_name': self.destination_name,
            'destination_type': self.destination_type.name,
            'destination_metadata': self.destination_metadata,
        }

    @staticmethod
    def from_dict(dict_destination):
        return Destination(
            dict_destination['destination_name'],
            DestinationType[dict_destination['destination_type']],
            dict_destination['destination_metadata'],
        )

    def __eq__(self, other) -> bool:
        return bool(
            self.destination_name == other.destination_name
            and self.destination_metadata[0] == other.destination_metadata[0]
        )

    def __hash__(self) -> int:
        return hash(
            (
                self.destination_name,
                self.destination_type.name,
                self.destination_metadata[0],
            )
        )


class Execution:
    def __init__(
        self, account_config: AccountConfig, source: Source, destination: Destination
    ):
        self._account_config = account_config
        self._source = source
        self._destination = destination

    @property
    def source(self) -> Source:
        return self._source

    @property
    def destination(self) -> Destination:
        return self._destination

    @property
    def account_config(self) -> AccountConfig:
        return self._account_config

    def to_dict(self):
        return {
            'account_config': self.account_config.to_dict(),
            'source': self.source.to_dict(),
            'destination': self.destination.to_dict()
        }

    @staticmethod
    def from_dict(dict_execution):
        return Execution(
            AccountConfig.from_dict(dict_execution['account_config']),
            Source.from_dict(dict_execution['source']),
            Destination.from_dict(dict_execution['destination']),
        )

    def __str__(self):
        return f"Origin name: {self.source.source_name}. Action: {self.destination.destination_type}. Destination name: {self.destination.destination_name}"

    def __eq__(self, other):
        if other is None:
            return False
        return (
            self.source == other.source
            and self.destination == other.destination
            and self.account_config == other.account_config
        )

    def __hash__(self):
        return hash((self.source, self.destination, self.account_config))


class ExecutionsGroupedBySource:
    def __init__(
        self,
        source_name: str,
        executions: List[Execution]
    ):
        self._source_name = source_name
        self._executions = executions

    @property
    def executions(self) -> List[Execution]:
        return self._executions

    @property
    def source_name(self) -> str:
        return self._source_name

    @property
    def source(self) -> Source:
        return self._executions[0].source

    @property
    def destinations(self) -> List[Destination]:
        return list([exec.destination for exec in self._executions])

    def __getitem__(self, i):
        return self._executions[i]

    def to_dict(self):
        executions_json = [exec.to_dict() for exec in self.executions]
        return {
            'source_name': self._source_name,
            'executions': executions_json
        }

    @staticmethod
    def from_dict(dict_executions):
        executions = list([Execution.from_dict(exec_json)
                          for exec_json in dict_executions['executions']])

        return ExecutionsGroupedBySource(
            dict_executions['source_name'],
            executions
        )

    def __str__(self):
        return f"Source: {self._source_name}. Executions: {','.join(['(' + str(exec) + ')' for exec in self._executions])}"

    def __eq__(self, other):
        if other is None:
            return False
        return self.source_name == other.source_name

    def __hash__(self):
        return hash(("ExecutionsGroupedBySource", self.source_name))


class DataRow(Dict[str, Any]):
    pass


class DataRowsGroupedBySource:
    def __init__(
        self,
        executions: ExecutionsGroupedBySource,
        rows: List[DataRow]
    ):
        self._executions = executions
        self._rows = rows

    @property
    def rows(self) -> List[DataRow]:
        return self._rows

    @property
    def executions(self) -> ExecutionsGroupedBySource:
        return self._executions

    @property
    def source(self) -> Source:
        return self._executions.source

    @property
    def source_name(self) -> str:
        return self._executions.source.source_name

    @property
    def destinations(self) -> List[Destination]:
        return self._executions.destinations

    def to_dict(self):
        return {
            'rows': self._rows,
            'executions': self._executions.to_dict()
        }

    @staticmethod
    def from_dict(dict_rows):
        return DataRowsGroupedBySource(
            ExecutionsGroupedBySource.from_dict(dict_rows.executions),
            dict_rows.rows
        )

    def __str__(self):
        return f"Source: {self.source_name}. Executions: {self._executions}. Rows: {self.rows}"

    def __eq__(self, other):
        if other is None:
            return False
        return self.source_name == other.source_name

    def __hash__(self):
        return hash(("DataRowsGroupedBySource", self.source_name))


class Batch:
    def __init__(
        self,
        execution: Execution,
        elements: List[Dict[str, Union[str, Dict[str, str]]]],
        iteration: int = 1,
    ):
        self._execution = execution
        self._elements = elements
        self._iteration = iteration

    @property
    def execution(self) -> Execution:
        return self._execution

    @property
    def elements(self) -> List[Dict[str, Union[str, Dict[str, str]]]]:
        return self._elements

    @property
    def iteration(self) -> int:
        return self._iteration

    def __str__(self):
        return f"Execution: {self._execution}. Elements: {self._elements}"

    def __eq__(self, other):
        if other is None:
            return False
        return self.execution == other.execution and self.elements == other.elements

    def __hash__(self):
        return hash(("Batch", self.execution))


class BatchesGroupedBySource:
    def __init__(
        self,
        source_name: str,
        batches: List[Batch]
    ):
        self._source_name = source_name
        self._batches = batches

    @property
    def batches(self) -> List[Batch]:
        return self._batches

    @property
    def source_name(self) -> str:
        return self._source_name

    def __getitem__(self, i):
        return self._batches[i]

    def __str__(self):
        return f"Source: {self._source_name}. Batches: {self._batches}"

    def __eq__(self, other):
        if other is None:
            return False
        return self.source_name == other.source_name

    def __hash__(self):
        return hash(("ExecutionsGroupedBySource", self.source_name))
