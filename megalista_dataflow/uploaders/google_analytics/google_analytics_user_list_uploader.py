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
from googleapiclient.http import MediaInMemoryUpload

from error.error_handling import ErrorHandler
from models.execution import Batch
from uploaders import utils
from uploaders.uploaders import MegalistaUploader


class GoogleAnalyticsUserListUploaderDoFn(MegalistaUploader):

    def __init__(self,oauth_credentials, error_handler: ErrorHandler):
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

        service = build('analytics', 'v3', credentials=credentials)
        return service

    def _create_list_if_doesnt_exist(self, analytics, web_property_id, view_ids, list_name, list_definition,
                                     ga_account_id, ads_customer_id, mcc):
        lists = analytics.management().remarketingAudience().list(
            accountId=ga_account_id, webPropertyId=web_property_id).execute()['items']
        results = list(
            filter(lambda x: x['name'] == list_name, lists))
        if len(results) == 0:
            logging.get_logger().info('%s list does not exist, creating...' % list_name)

            response = analytics.management().remarketingAudience().insert(
                accountId=ga_account_id,
                webPropertyId=web_property_id,
                body={
                    'name': list_name,
                    'linkedViews': view_ids,
                    'linkedAdAccounts': [{
                        'type': 'MCC_LINKS' if mcc else 'ADWORDS_LINKS',
                        'linkedAccountId': ads_customer_id
                    }],
                    **list_definition
                }).execute()
            _id = response['id']
            logging.get_logger().info('%s created with id: %s' % (list_name, id))
        else:
            _id = results[0]['id']
            logging.get_logger().info('%s found with id: %s' % (list_name, id))
        return _id

    def _create_list(self, web_property_id, view_id, user_id_list_name, buyer_custom_dim, ga_account_id,
                     ads_customer_id,
                     mcc):
        analytics = self._get_analytics_service()
        view_ids = [view_id]
        self._create_list_if_doesnt_exist(analytics, web_property_id, view_ids, user_id_list_name, {
            'audienceType': 'SIMPLE',
            'audienceDefinition': {
                'includeConditions': {
                    'kind': 'analytics#includeConditions',
                    'isSmartList': False,
                    'segment': 'users::condition::%s==buyer' % buyer_custom_dim,
                    'membershipDurationDays': 365
                }
            }
        }, ga_account_id, ads_customer_id, mcc)

    @staticmethod
    def _assert_all_list_names_are_present(any_execution):
        destination = any_execution.destination.destination_metadata
        if len(destination) < 6:
            raise ValueError('Missing destination information. Found {}'.format(len(destination)))

        if not destination[0] \
                or not destination[1] \
                or not destination[2] \
                or not destination[4] \
                or not destination[5]:
            raise ValueError('Missing destination information. Received {}'.format(str(destination)))

    @utils.safe_process(logger=logging.get_logger("megalista.GoogleAnalyticsUserListUploader"))
    def process(self, batch: Batch, **kwargs):
        execution = batch.execution
        self._assert_all_list_names_are_present(execution)

        ads_customer_id = execution.account_config.google_ads_account_id
        mcc = execution.account_config.mcc
        ga_account_id = execution.account_config.google_analytics_account_id

        # Reads all metadata parameters
        metadata = execution.destination.destination_metadata

        web_property_id = metadata[0]
        view_id = metadata[1]
        data_import_name = metadata[2]
        user_id_list_name = metadata[3]
        user_id_custom_dim = metadata[4]
        buyer_custom_dim = metadata[5]

        # Optional parameter
        custom_dim_field = metadata[6] if len(metadata) > 6 else None

        self._do_upload_data(execution, web_property_id, view_id, data_import_name, user_id_list_name, user_id_custom_dim,
                             buyer_custom_dim, custom_dim_field, ga_account_id, ads_customer_id, mcc,
                             batch.elements)

        return [execution]

    def _do_upload_data(self, execution, web_property_id, view_id, data_import_name, user_id_list_name, user_id_custom_dim,
                        buyer_custom_dim, custom_dim_field, ga_account_id, ads_customer_id, mcc, rows):

        if user_id_list_name:
            self._create_list(web_property_id, view_id, user_id_list_name, buyer_custom_dim, ga_account_id,
                              ads_customer_id, mcc)

        analytics = self._get_analytics_service()
        data_sources = analytics.management().customDataSources().list(
            accountId=ga_account_id, webPropertyId=web_property_id).execute()['items']
        results = list(
            filter(lambda x: x['name'] == data_import_name, data_sources))

        if len(results) == 1:

            _id = results[0]['id']

            logging.get_logger().info("Adding data to %s - %s" % (data_import_name, _id))
            body = '\n'.join([
                '%s,%s' % (user_id_custom_dim, buyer_custom_dim),
                *['%s,%s' % (row['user_id'], row[custom_dim_field] if custom_dim_field else 'buyer') for row in rows]
            ])

            try:
                media = MediaInMemoryUpload(bytes(body, 'UTF-8'),
                                            mimetype='application/octet-stream',
                                            resumable=True)
                analytics.management().uploads().uploadData(
                    accountId=ga_account_id,
                    webPropertyId=web_property_id,
                    customDataSourceId=_id,
                    media_body=media).execute()
            except Exception as e:
                error_message = f'Error while uploading GA Data: {e}'
                logging.get_logger().error(error_message, execution=execution)
                self._add_error(execution, error_message)
                execution.unsuccessful_records = execution.unsuccessful_records + len(rows)
            else:
                execution.successful_records = execution.successful_records + len(rows)

        else:
            error_message = f"{data_import_name} - data import not found, please configure it in Google Analytics"
            logging.get_logger().error(error_message, execution=execution)
            self._add_error(execution, error_message)
            execution.unsuccessful_records = execution.unsuccessful_records + len(rows)

