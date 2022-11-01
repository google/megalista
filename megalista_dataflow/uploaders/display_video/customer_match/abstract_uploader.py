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

import logging
from typing import Dict, Any, List, Optional, Tuple
from apache_beam.options.value_provider import StaticValueProvider
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build


from error.error_handling import ErrorHandler
from models.execution import AccountConfig, Destination, Execution
from models.execution import Batch
from models.execution import DestinationType
from models.oauth_credentials import OAuthCredentials
from uploaders.display_video.utils import Utils
from uploaders.display_video import DV_API_VERSION
from uploaders.uploaders import MegalistaUploader

_DEFAULT_LOGGER: str = 'megalista.DisplayVideoCustomerMatchAbstractUploader'


class DisplayVideoCustomerMatchAbstractUploaderDoFn(MegalistaUploader):

    def __init__(self, oauth_credentials: OAuthCredentials, developer_token: StaticValueProvider,
                 error_handler: ErrorHandler):
        super().__init__(error_handler)
        self.oauth_credentials = oauth_credentials
        self.developer_token = developer_token
        self.active = True
        self._user_list_id_cache: Dict[str, Dict[str, Any]] = dict()

    def _get_dv_service(self):
        credentials = Credentials(
            token=self.oauth_credentials.get_access_token(),
            refresh_token=self.oauth_credentials.get_refresh_token(),
            client_id=self.oauth_credentials.get_client_id(),
            client_secret=self.oauth_credentials.get_client_secret(),
            token_uri='https://accounts.google.com/o/oauth2/token',
            scopes=[
                'https://www.googleapis.com/auth/display-video'
            ]
        )

        return build(
            'displayvideo',
            DV_API_VERSION,      
            credentials=credentials
        )

    def _get_dv_audience_service(self):
        return self._get_dv_service().firstAndThirdPartyAudiences()

    def start_bundle(self):
        pass

    def finish_bundle(self):
        super().finish_bundle()

    def _create_list_if_it_does_not_exist(self,
                                          advertiser_id: str,
                                          list_name: str,
                                          list_definition: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        was_audience_created = False
        if self._user_list_id_cache.get(list_name) is None:
            was_audience_created, self._user_list_id_cache[list_name] = \
                 self._do_create_list_if_it_does_not_exist(
                    advertiser_id, list_name, list_definition)

        return was_audience_created, self._user_list_id_cache[list_name]

    def _do_create_list_if_it_does_not_exist(self,
                                             advertiser_id: str,
                                             list_name: str,
                                             list_definition: Dict[str, Any]
                                             ) -> Tuple[bool, Dict[str, Any]]:

        was_audience_created = False
        found_audience = self._get_advertiser_audience_by_display_name(
            advertiser_id, list_name)

        if found_audience is None:
            # Create list
            logging.getLogger(_DEFAULT_LOGGER).info(
                '%s list does not exist, creating...', list_name)
            
            # Marks the newly created audience    
            was_audience_created = True
            
            found_audience = self._get_dv_audience_service().create(
                advertiserId=advertiser_id,
                body=list_definition
            ).execute()

            logging.getLogger(_DEFAULT_LOGGER).info(
                'List %s created with resource name: %s', list_name, found_audience['displayName'])
        else:
            logging.getLogger(_DEFAULT_LOGGER).info(
                'List found with name: %s [%s]', found_audience['displayName'], found_audience['firstAndThirdPartyAudienceId'] )

        return was_audience_created, found_audience

    def _get_advertiser_audience_by_display_name(self, advertiser_id: str, display_name: str) -> Optional[Dict[str, Any]]:
        """
            Gets the advertise's audience by the displayName
        """
        response = self._get_dv_audience_service().list(
            advertiserId=advertiser_id,
            pageSize=1,
            filter=f'displayName : "{display_name}"'
        ).execute()

        if response and response['firstAndThirdPartyAudiences']:
            return dict(response['firstAndThirdPartyAudiences'][0])
        else: 
            return None

    def _assert_execution_is_valid(self, execution) -> None:
        destination = execution.destination.destination_metadata

        # The number of parameters vary by upload. This test could be parameterized
        if not destination[0]:
            raise ValueError('Missing destination information. Received {}'.format(
                str(destination)))
        
        elif not destination[1]:
            raise ValueError('Missing list_name information. Received {}'.format(
                str(destination)))        

    def _get_advertiser_id(self, account_config: AccountConfig, destination: Destination) -> str:
        """
          Gets the advertiser id that the audience should be sent to
        """
        return destination.destination_metadata[0]


    def _get_list_name(self, account_config: AccountConfig, destination: Destination) -> str:
        """
          Gets the advertiser id that the audience should be sent to
        """
        return destination.destination_metadata[1]

    def get_filtered_rows(self, rows: List[Any], keys: List[str]) -> List[Dict[str, Any]]:
        return [{key: row.get(key) for key in keys if key in row} for row in rows]

    @Utils.safe_process(logger=logging.getLogger(_DEFAULT_LOGGER))
    def process(self, batch: Batch, **kwargs) -> List[Execution]:
        if not self.active:
            logging.getLogger(_DEFAULT_LOGGER).info(
                'Skipping upload to DV, parameters not configured.')
            return []

        execution = batch.execution

        self._assert_execution_is_valid(execution)

        # Gets advertiser_id from Metadata 0
        advertiser_id = self._get_advertiser_id(
            execution.account_config, execution.destination) 

        # Gets audience name from Metadata 1
        list_name = self._get_list_name(
            execution.account_config, execution.destination)  

        # Customer Match element's list (Contact Info/Device)
        rows = self.get_filtered_rows(batch.elements, self.get_row_keys())

        # Payload for creating a new list in case there isn't one
        list_definition = self.get_list_definition(
            execution.account_config,
            execution.destination.destination_metadata,
            rows
        )

        # Checks if the audience already exists and returns, or creates a new one
        # If the audience was just created, skips the update for this batch
        was_audience_created, audience = self._create_list_if_it_does_not_exist(
            advertiser_id,
            list_name,
            list_definition
        )
        
        if not was_audience_created:
            # Payload for updating an existing list
            updated_list_definition = self.get_update_list_definition(
                execution.account_config,
                execution.destination.destination_metadata,
                rows
            )

            # Updates found/created audience by renewing the customer's membership list
            self._get_dv_audience_service().editCustomerMatchMembers(
                firstAndThirdPartyAudienceId=audience['firstAndThirdPartyAudienceId'],
                body=updated_list_definition
            ).execute()

        return [execution]
        
    def get_list_definition(self, account_config: AccountConfig,
                            destination_metadata: List[str], list_to_add: List[Dict[str, Any]]) -> Dict[str, Any]:
        pass

    def get_update_list_definition(self, account_config: AccountConfig,
                                  destination_metadata: List[str], list_to_add: List[Dict[str, Any]]) -> Dict[str, Any]:
        pass

    def get_row_keys(self) -> List[str]:
        pass

    def get_action_type(self) -> DestinationType:
        pass
