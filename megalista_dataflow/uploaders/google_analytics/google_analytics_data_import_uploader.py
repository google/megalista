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
#

import logging
from typing import List, Dict

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload

from error.error_handling import ErrorHandler
from models.execution import Batch, Union
from uploaders import utils
from uploaders.uploaders import MegalistaUploader


class GoogleAnalyticsDataImportUploaderDoFn(MegalistaUploader):
    """
      This uploader uploads csv files to Google Analytics Data Import.
      The csv headers are the dict received keys.
      Only one Execution can ben handled at a time, meaning that only one data
      import can be handled at a time.

      """

    def __init__(self, oauth_credentials, error_handler: ErrorHandler):
        super().__init__(error_handler)
        self.oauth_credentials = oauth_credentials

    def _get_analytics_service(self):
        credentials = Credentials(
            token=self.oauth_credentials.get_access_token(),
            refresh_token=self.oauth_credentials.get_refresh_token(),
            client_id=self.oauth_credentials.get_client_id(),
            client_secret=self.oauth_credentials.get_client_secret(),
            token_uri='https://accounts.google.com/o/oauth2/token',
            scopes=[
                'https://www.googleapis.com/auth/analytics.edit',
                'https://www.googleapis.com/auth/adwords'
            ])

        service = build('analytics', 'v3', credentials=credentials)
        return service

    def start_bundle(self):
        pass

    @staticmethod
    def _assert_all_list_names_are_present(any_execution):
        destination = any_execution.destination.destination_metadata
        if len(destination) < 2:
            raise ValueError('Missing destination information. Found '
                             f'{len(destination)}')

        if not destination[0] or not destination[1]:
            raise ValueError('Missing destination information. Received {}'.format(
                str(destination)))

    @utils.safe_process(
        logger=logging.getLogger('megalista.GoogleAnalyticsDataImportUploader'))
    def process(self, batch: Batch, **kwargs):
        execution = batch.execution
        self._assert_all_list_names_are_present(execution)

        ga_account_id = execution.account_config.google_analytics_account_id

        # Reads all metadata parameters
        metadata = execution.destination.destination_metadata

        web_property_id = metadata[0]
        data_import_name = metadata[1]

        self._do_upload_data(execution, web_property_id, data_import_name,
                             ga_account_id, batch.elements)

        return [execution]

    def _do_upload_data(self, execution, web_property_id, data_import_name, ga_account_id,
                        rows: List[Dict[str, Union[str, Dict[str, str]]]]):
        analytics = self._get_analytics_service()
        data_sources = analytics.management().customDataSources().list(
            accountId=ga_account_id,
            webPropertyId=web_property_id).execute()['items']

        data_source_results = list(
            filter(lambda x: x['name'] == data_import_name, data_sources))

        if data_source_results:

            data_source_id = data_source_results[0]['id']

            try:
                self._call_upload_api(analytics, data_import_name, ga_account_id,
                                      data_source_id, rows, web_property_id)
            except Exception as e:
                error_message = f'Error while uploading GA Data: {e}'
                logging.getLogger('megalista.GoogleAnalyticsDataImportUploader').error(error_message)
                self._add_error(execution, error_message)
        else:
            error_message = f'{data_import_name} - data import not found, please configure it in Google Analytics'
            logging.getLogger('megalista.GoogleAnalyticsDataImportUploader').error(error_message)
            self._add_error(execution, error_message)

    @staticmethod
    def prepare_csv(rows):
        """
                Transform a input into this format:
                        sample = [{'col1': 'val1a', 'col2': 'val2a', 'col3': 'val3a'},
                          {'col1': 'val1b', 'col2': 'val2b', 'col3': 'val3b'},
                          {'col1': 'val1c', 'col2': 'val2c', 'col3': 'val3c'}]
                into a csv:
                        col1,col2,col3
                        val1a,val2a,val3a
                        val1b,val2b,val3b
                        val1c,val2c,val3c
            """
        column_names = ['ga:' + columnName for columnName in rows[0].keys()]
        header = ','.join(column_names)
        body = '\n'.join([
            ','.join(
                ['' if element is None else element
                 for element in row.values()])
            for row in rows
        ])
        return '\n'.join([header, body])

    def _call_upload_api(self, analytics, data_import_name, ga_account_id,
                         data_source_id, rows, web_property_id):
        logging.getLogger('megalista.GoogleAnalyticsDataImportUploader').info(
            'Adding data to %s - %s' % (data_import_name, data_source_id))
        csv = self.prepare_csv(rows)

        media = MediaInMemoryUpload(
            bytes(csv, 'UTF-8'),
            mimetype='application/octet-stream',
            resumable=True)

        analytics.management().uploads().uploadData(
            accountId=ga_account_id,
            webPropertyId=web_property_id,
            customDataSourceId=data_source_id,
            media_body=media).execute()
