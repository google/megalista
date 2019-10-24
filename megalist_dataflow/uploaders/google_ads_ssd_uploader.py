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
from megalist_dataflow.utils.oauth_credentials import OAuthCredentials
from apache_beam.options.value_provider import StaticValueProvider


class GoogleAdsSSDUploaderDoFn(beam.DoFn):

    def __init__(self, oauth_credentials, developer_token, customer_id):
        self.oauth_credentials = oauth_credentials
        self.developer_token = developer_token
        self.customer_id = customer_id
        self.active = True
        if self.developer_token is None or self.customer_id is None:
            self.active = False

    def _get_ssd_service(self):
        from googleads import adwords
        from googleads import oauth2
        oauth2_client = oauth2.GoogleRefreshTokenClient(
            self.oauth_credentials.get_client_id(), self.oauth_credentials.get_client_secret(), self.oauth_credentials.get_refresh_token())
        client = adwords.AdWordsClient(
            self.developer_token.get(), oauth2_client, 'MegaList Dataflow', client_customer_id=self.customer_id.get())
        return client.GetService(
            'OfflineDataUploadService', 'v201809')

    def start_bundle(self):
        pass

    def process(self, element):
        if self.active == False:
            return
        ssd_service = self._get_ssd_service()
        print(dir(ssd_service))

        # mutate_members_operation = {
        #     'operand': {
        #         'userListId': self.user_list_id,
        #         'membersList': element
        #     },
        #     'operator': 'ADD'
        # }
        # ssd_service.mutateMembers([mutate_members_operation])

        return element


print('yay')
client_id = StaticValueProvider(str, "601628591949-qm671o3eeam3eacevk3rp2v3e64tt9ud.apps.googleusercontent.com")
client_secret = StaticValueProvider(str, "JC9ZnhqDx-sHgvJyTvQ28j_D")
access_token = StaticValueProvider(str, "ya29.Il-pB1PI4Njzfe3KDuNlAfoW8shJUl-0XzRopJRoG68leBgJW6Z-RnJuS_aB0cSXHvQ0Kjerp4Tc9fhIaQBptvBr-DHNwA2rb0X-XkCE7NSv4_GyraDugcL6xlzZTMqwWQ")
refresh_token = StaticValueProvider(str, "1//0hNdWArOM2BCrCgYIARAAGBESNwF-L9Irf5ABkXW4h0wYA1EjAqrGie3qFF8xk979vRXdcNF23tPdTUIeKvSTSHqZ972sTTPCsjg")
developer_token = StaticValueProvider(str, "z27QEhvZe2mjjYFmwwfskg")
customer_id = StaticValueProvider(str, "872-438-5642")
credentials = OAuthCredentials(client_id, client_secret, access_token, refresh_token)
uploader = GoogleAdsSSDUploaderDoFn(credentials, developer_token, customer_id)
uploader.process({'a':'b'})