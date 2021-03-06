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

from uploaders import utils
from models.execution import DestinationType, Batch, Execution


class GoogleAdsSSDUploaderDoFn(beam.DoFn):

    def __init__(self, oauth_credentials, developer_token):
        super().__init__()
        self.oauth_credentials = oauth_credentials
        self.developer_token = developer_token
        self.active = developer_token is not None

    def _get_ssd_service(self, customer_id):
        return utils.get_ads_service('OfflineDataUploadService', 'v201809',
                                         self.oauth_credentials,
                                         self.developer_token.get(), customer_id)

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

        ssd_service = self._get_ssd_service(
            execution.account_config._google_ads_account_id)
        self._do_upload(ssd_service,
                        execution.destination.destination_metadata[0],
                        execution.destination.destination_metadata[1], batch.elements)

    @staticmethod
    def _do_upload(ssd_service, conversion_name, ssd_external_upload_id, rows):
        upload_data = [{
            'StoreSalesTransaction': {
                'userIdentifiers': [{
                    'userIdentifierType': 'HASHED_EMAIL',
                    'value': conversion['hashedEmail']
                }],
                'transactionTime': utils.format_date(conversion['time']),
                'transactionAmount': {
                    'currencyCode': 'BRL',
                    'money': {
                        'microAmount': conversion['amount']
                    }
                },
                'conversionName': conversion_name
            }
        } for conversion in rows]

        offline_data_upload = {
            'externalUploadId': ssd_external_upload_id,
            'offlineDataList': upload_data,
            'uploadType': 'STORE_SALES_UPLOAD_FIRST_PARTY',
            'uploadMetadata': {
                'StoreSalesUploadCommonMetadata': {
                    'xsi_type': 'FirstPartyUploadMetadata',
                    'loyaltyRate': 1.0,
                    'transactionUploadRate': 1.0,
                }
            }
        }

        add_conversions_operation = {
            'operand': offline_data_upload,
            'operator': 'ADD'
        }
        ssd_service.mutate([add_conversions_operation])
