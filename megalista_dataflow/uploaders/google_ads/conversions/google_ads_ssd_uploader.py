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

import apache_beam as beam
import logging
import datetime

from models.execution import Batch, Execution
from uploaders import utils
from uploaders.google_ads import ADS_API_VERSION


class GoogleAdsSSDUploaderDoFn(beam.DoFn):

    def __init__(self, oauth_credentials, developer_token):
        super().__init__()
        self.oauth_credentials = oauth_credentials
        self.developer_token = developer_token
        self.active = developer_token is not None

    def _get_offline_user_data_job_service(self, customer_id):
        return utils.get_ads_service('OfflineUserDataJobService', ADS_API_VERSION,
                                         self.oauth_credentials,
                                         self.developer_token.get(),
                                         customer_id)

    @staticmethod
    def _assert_conversion_metadata_is_present(execution: Execution):
        metadata = execution.destination.destination_metadata
        if len(metadata) != 2:
            raise ValueError(
                f'Missing destination information. Received {len(metadata)} entry(ies)')

    @utils.safe_process(
        logger=logging.getLogger('megalista.GoogleAdsSSDUploader'))
    def process(self, batch: Batch, **kwargs):
        execution = batch.execution
        self._assert_conversion_metadata_is_present(execution)
        customer_id = execution.account_config.google_ads_account_id.replace('-', '')

        offline_user_data_job_service = self._get_offline_user_data_job_service(
            customer_id)
        conversion_action_resource_name = self._get_resource_name(customer_id,
                                                                  execution.destination.destination_metadata[0])
        self._do_upload(offline_user_data_job_service,
                        customer_id,
                        conversion_action_resource_name,
                        execution.destination.destination_metadata[1], batch.elements)

    @staticmethod
    def _do_upload(offline_user_data_job_service, customer_id, conversion_action_resource_name, ssd_external_upload_id, rows):
        # Upload is divided into 3 parts:
        # 1. Create Job
        # 2. Create operations (data insertion)
        # 3. Run the Job

        # 1. Create Job
        # TODO(caiotomazelli): Remove ssd_external_upload_id parameter
        unique_external_id = int(datetime.datetime.now().timestamp()*10e3)
        job_creation_payload = {
            'type_': 'STORE_SALES_UPLOAD_FIRST_PARTY',
            'external_id': unique_external_id,
            'store_sales_metadata': {
                'loyalty_fraction': 1.0,
                'transaction_upload_fraction': 1.0
            }
        }

        job_resource_name = offline_user_data_job_service.create_offline_user_data_job(customer_id = customer_id, job = job_creation_payload).resource_name

        # 2. Crete operations (data insertion)
        data_insertion_payload = {
            'resource_name': job_resource_name,
            'enable_partial_failure': False,
            'operations': [{
                'create': {
                    'user_identifiers': [{
                        'hashed_email': conversion['hashedEmail']
                    }],
                    'transaction_attribute': {
                        'conversion_action': conversion_action_resource_name,
                        'currency_code': 'BRL',
                        'transaction_amount_micros': conversion['amount'],
                        'transaction_date_time': utils.format_date(conversion['time'])
                    }
                }
            } for conversion in rows]
        }

        data_insertion_response = offline_user_data_job_service.add_offline_user_data_job_operations(request = data_insertion_payload)

        # 3. Run the Job
        offline_user_data_job_service.run_offline_user_data_job(resource_name = job_resource_name)

    def _get_ads_service(self, customer_id: str):
      return utils.get_ads_service('GoogleAdsService', ADS_API_VERSION,
                                       self.oauth_credentials,
                                       self.developer_token.get(),
                                       customer_id)

    def _get_resource_name(self, customer_id: str, name: str):
        resource_name = None
        service = self._get_ads_service(customer_id)
        query = f"SELECT conversion_action.resource_name FROM conversion_action WHERE conversion_action.name = '{name}'"
        response_query = service.search_stream(customer_id=customer_id, query=query)
        for batch in response_query:
          for row in batch.results:
            resource_name = row.conversion_action.resource_name
        return resource_name
