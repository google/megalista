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

from config import logging

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from error.error_handling import ErrorHandler
from models.execution import Batch
from uploaders import utils
from uploaders.uploaders import MegalistaUploader

LOGGER_NAME = 'megalista.GoogleAnalyticsDataImportUploader'

class GoogleAnalyticsDataImportEraser(MegalistaUploader):
    """
    Clean up every file in a Custom Data Import.

    If you are changing this code, be very careful, since this class deletes ALL FILES within a Data Import.
    Make sure you're not deleting files from the wrong Data Import.
    Also, make sure that all unit tests pass and write new ones as you feel appropriated.
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
            scopes=["https://www.googleapis.com/auth/analytics.edit", 'https://www.googleapis.com/auth/adwords'])

        return build('analytics', 'v3', credentials=credentials)

    @staticmethod
    def _assert_all_list_names_are_present(any_execution):
        destination = any_execution.destination.destination_metadata
        if len(destination) < 2:
            raise ValueError('Missing destination information. Found {}'.format(len(destination)))

        if not destination[0] or not destination[1]:
            raise ValueError('Missing destination information. Received {}'.format(str(destination)))

    @utils.safe_process(
        logger=logging.get_logger(LOGGER_NAME))
    def process(self, batch: Batch, **kwargs):
        execution = batch.execution
        self._assert_all_list_names_are_present(execution)

        ga_account_id = execution.account_config.google_analytics_account_id

        # Reads all metadata parameters
        metadata = execution.destination.destination_metadata

        web_property_id = metadata[0]
        data_import_name = metadata[1]

        analytics = self._get_analytics_service()
        data_sources = analytics.management().customDataSources().list(
            accountId=ga_account_id, webPropertyId=web_property_id).execute()['items']
        data_source_results = list(
            filter(lambda data_source: data_source['name'] == data_import_name, data_sources))

        if len(data_source_results) == 1:
            data_source_id = data_source_results[0]['id']
            try:
                self._call_delete_api(analytics, data_import_name, ga_account_id, data_source_id, web_property_id)
                return [batch]
            except Exception as e:
                error_message = f'Error while delete GA Data Import files: {e}'
                logging.get_logger(LOGGER_NAME).error(error_message)
                self._add_error(execution, error_message)
        else:
            error_message = f"{data_import_name} - data import not found, please configure it in Google Analytics"
            logging.get_logger(LOGGER_NAME).error(error_message)
            self._add_error(execution, error_message)

    @staticmethod
    def _call_delete_api(analytics, data_import_name, ga_account_id, data_source_id, web_property_id):
        logging.get_logger(LOGGER_NAME).info(
            "Listing files from %s - %s" % (data_import_name, data_source_id))

        uploads = analytics.management().uploads().list(
            accountId=ga_account_id,
            webPropertyId=web_property_id,
            customDataSourceId=data_source_id
        ).execute()

        file_ids = [upload.get('id') for upload in uploads.get('items', [])]
        if len(file_ids) == 0:
            logging.get_logger(LOGGER_NAME).error(
                "Data Source %s had no files to delete" % data_import_name)

        else:
            logging.get_logger(LOGGER_NAME).info(
                "File Ids: %s" % file_ids)

            logging.get_logger(LOGGER_NAME).info(
                "Deleting %s files from %s - %s" % (len(file_ids), data_import_name, data_source_id))
            analytics.management().uploads().deleteUploadData(
                accountId=ga_account_id,
                webPropertyId=web_property_id,
                customDataSourceId=data_source_id,
                body={
                    'customDataImportUids': file_ids
                }
            ).execute()
