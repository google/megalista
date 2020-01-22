# Copyright 2019 Google LLC
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
import pytz
import datetime
from utils.oauth_credentials import OAuthCredentials
from apache_beam.options.value_provider import StaticValueProvider

timezone = pytz.timezone('America/Sao_Paulo')


class GoogleAdsOfflineUploaderDoFn(beam.DoFn):
    def __init__(self, oauth_credentials, developer_token, customer_id, conversion_name):
        self.oauth_credentials = oauth_credentials
        self.developer_token = developer_token
        self.customer_id = customer_id
        self.active = True
        self.conversion_name = conversion_name
        if self.developer_token is None or self.customer_id is None:
            self.active = False

    def _get_oc_service(self):
        from googleads import adwords
        from googleads import oauth2
        oauth2_client = oauth2.GoogleRefreshTokenClient(
            self.oauth_credentials.get_client_id(), self.oauth_credentials.get_client_secret(), self.oauth_credentials.get_refresh_token())
        client = adwords.AdWordsClient(
            self.developer_token.get(), oauth2_client, 'MegaList Dataflow', client_customer_id=self.customer_id.get())
        return client.GetService('OfflineConversionFeedService', version='v201809')

    def _format_date(self, date):
        pdate = datetime.datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%f")
        return '%s %s' % (datetime.datetime.strftime(pdate, '%Y%m%d %H%M%S'), timezone.zone)

    def start_bundle(self):
        pass

    def process(self, elements_batch):
        print("here")
        if self.active == False:
            return
        oc_service = self._get_oc_service()

        upload_data = [
            {
                'operator': 'ADD',
                'operand': {
                    'conversionName': self.conversion_name.get(),
                    'conversionTime': self._format_date(conversion['time']),
                    'conversionValue': conversion['amount'],
                    'googleClickId': conversion['gclid']
                }
            } for conversion in elements_batch]

        oc_service.mutate(upload_data)

        return elements_batch
