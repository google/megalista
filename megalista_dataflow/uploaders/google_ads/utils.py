# Copyright 2022 Google LLC
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
from typing import Optional

from ..utils import Utils as BaseUtils

class Utils(BaseUtils):
    
    @staticmethod
    def get_ads_client(oauth_credentials, developer_token, customer_id):
        from google.ads.googleads.client import GoogleAdsClient
        from google.ads.googleads import oauth2

        oauth2_client = oauth2.get_installed_app_credentials(
            oauth_credentials.get_client_id(), oauth_credentials.get_client_secret(),
            oauth_credentials.get_refresh_token())

        return GoogleAdsClient(
            oauth2_client, developer_token,
            login_customer_id=customer_id)

    @staticmethod
    def get_ads_service(service_name, version, oauth_credentials, developer_token,
                        customer_id):
        return Utils.get_ads_client(oauth_credentials, developer_token, customer_id).get_service(service_name, version=version)

    @staticmethod
    def print_partial_error_messages(logger_name, action, response) -> Optional[str]:
        """
        Print partials errors received in the response.
        @param logger_name: logger name to be used
        @param action: action to be part of the message
        @param response: response body of the API call
        @return: the error_message returned, if there was one, None otherwise.
        """
        error_message = None

        partial_failure = getattr(response, 'partial_failure_error', None)
        if partial_failure is not None and partial_failure.message != '':
            error_message = f'Error on {action}: {partial_failure.message}.'
            logging.getLogger(logger_name).error(error_message)
        results = getattr(response, 'results', [])
        for result in results:
            gclid = getattr(result, 'gclid', None)
            caller_id = getattr(result, 'caller_id', None)
            if gclid is not None:
                message = f'gclid {result.gclid} uploaded.'
            elif caller_id is not None:
                message = f'caller_id {result.caller_id} uploaded.'
            else:
                message = f'item {result} uploaded.'

            logging.getLogger(logger_name).debug(message)

        return error_message
