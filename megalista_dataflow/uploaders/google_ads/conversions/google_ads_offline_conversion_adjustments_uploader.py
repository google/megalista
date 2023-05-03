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

from typing import Any, Dict, List, Optional
from apache_beam.options.value_provider import ValueProvider

from error.error_handling import ErrorHandler
from models.execution import Batch, Execution, AccountConfig, Destination, DestinationType
from models.oauth_credentials import OAuthCredentials
from uploaders import utils
from utils.utils import Utils
from uploaders.google_ads import ADS_API_VERSION
from uploaders.uploaders import MegalistaUploader

_DEFAULT_LOGGER: str = 'megalista.GoogleAdsOfflineConversionAdjustmentsUploader'


class GoogleAdsOfflineAdjustmentUploaderDoFn(MegalistaUploader):
    def __init__(
        self,
        oauth_credentials: OAuthCredentials,
        developer_token: ValueProvider,
        error_handler: ErrorHandler,
    ):
        super().__init__(error_handler)
        self.oauth_credentials = oauth_credentials
        self.developer_token = developer_token

    def _get_ads_service(self, customer_id: str):
        return utils.get_ads_service(
            'GoogleAdsService',
            ADS_API_VERSION,
            self.oauth_credentials,
            self.developer_token.get(),
            customer_id,
        )

    def _get_oca_service(self, customer_id):
        return utils.get_ads_service(
            'ConversionAdjustmentUploadService',
            ADS_API_VERSION,
            self.oauth_credentials,
            self.developer_token.get(),
            customer_id,
        )

    def start_bundle(self):
        pass

    def _get_customer_id(
        self, account_config: AccountConfig, destination: Destination
    ) -> str:
        """
        If the customer_id is present on the destination, returns it, otherwise defaults to the account_config info.
        """
        if (
            len(destination.destination_metadata) >= 2
            and len(destination.destination_metadata[1]) > 0
        ):
            return Utils.filter_text_only_numbers(destination.destination_metadata[1])
        return account_config.google_ads_account_id

    def _get_login_customer_id(
        self, account_config: AccountConfig, destination: Destination
    ) -> str:
        """
        If the customer_id in account_config is a mcc, then login with the mcc account id, otherwise use the customer id.
        """
        if account_config._mcc:
            return account_config.google_ads_account_id

        return self._get_customer_id(account_config, destination)

    @staticmethod
    def _assert_valid_destination(execution: Execution):
        destination = execution.destination.destination_metadata
        # Metadada 1 (conversion name) and Metadata 3 (adjustment type) are both required.
        if len(destination) <= 2:
            raise ValueError(
                'Missing destination information. Found {} Metadata. Metadata 1 and Metadata 3 are mandatory'.format(len(destination))
            )

        if not destination[0]:
            raise ValueError(
                'Missing Metadata 1. Received {}'.format(str(destination))
            )
        
        if not destination[2]:
            raise ValueError(
                'Missing Metadata 3. Received {}'.format(str(destination))
            )

    @utils.safe_process(logger=logging.getLogger('megalista.GoogleAdsOfflineUploader'))
    def process(self, batch: Batch, **kwargs):
        execution = batch.execution
        self._assert_valid_destination(execution)

        customer_id = self._get_customer_id(
            execution.account_config, execution.destination
        )

        # Retrieves the login-customer-id if mcc enabled
        login_customer_id = self._get_login_customer_id(
            execution.account_config, execution.destination
        )
        # Initiates OCA service
        oca_service = self._get_oca_service(login_customer_id)
        # Initiates ADS service
        ads_service = self._get_ads_service(login_customer_id)

        resource_name = self._get_resource_name(
            ads_service, customer_id, execution.destination.destination_metadata[0]
        )

        response = self._do_upload(
            oca_service, execution, resource_name, customer_id, batch.elements
        )

        batch_with_successful_elements = self._get_new_batch_with_successfully_uploaded_elements(batch, response)
        
        if len(batch_with_successful_elements.elements) > 0:
            return [batch_with_successful_elements]
    
    def _do_upload(
        self, oca_service, execution, conversion_resource_name, customer_id, rows
    ):
        logging.getLogger(_DEFAULT_LOGGER).info(
            f'Uploading {len(rows)} offline conversions adjustments on {conversion_resource_name} to Google Ads.'
        )
        
        conversion_adjustments = self.populate_adjustments(rows, conversion_resource_name, execution.destination.destination_metadata[2])
        
        upload_data = {
            'customer_id': customer_id,
            'partial_failure': True,
            'validate_only': False,
            'conversion_adjustments': conversion_adjustments,
        }

        response = oca_service.upload_conversion_adjustments(request=upload_data)
        
        error_message = utils.print_partial_error_messages(
            _DEFAULT_LOGGER, 'uploading offline conversion adjustments', response
        )
        if error_message:
            self._add_error(execution, error_message)

        return response

    def _get_resource_name(self, ads_service, customer_id: str, name: str):
        query = f"SELECT conversion_action.resource_name FROM conversion_action WHERE conversion_action.name = '{name}'"
        response_query = ads_service.search_stream(customer_id=customer_id, query=query)
        for batch in response_query:
            for row in batch.results:
                return row.conversion_action.resource_name
        raise Exception(
            f'Conversion "{name}" could not be found on account {customer_id}'
        )

    def _get_new_batch_with_successfully_uploaded_elements(self, batch: Batch, response):
        pass
    
    def populate_adjustments(self, rows:List[Dict[str, Any]], conversion_resource_name: str, adjustment_type: str) -> Dict[str, Any]:
        pass
