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

from apache_beam.options.value_provider import ValueProvider

from error.error_handling import ErrorHandler
from models.execution import Batch, Execution, AccountConfig, Destination
from models.oauth_credentials import OAuthCredentials
from uploaders import utils
from utils.utils import Utils
from uploaders.google_ads import ADS_API_VERSION
from uploaders.uploaders import MegalistaUploader

# Petlove
from datetime import datetime, timedelta


_DEFAULT_LOGGER: str = 'megalista.GoogleAdsECLeadsUploader'


class GoogleAdsECLeadsUploaderDoFn(MegalistaUploader):

    def __init__(self, oauth_credentials: OAuthCredentials, developer_token: ValueProvider, error_handler: ErrorHandler):
        super().__init__(error_handler)
        self.oauth_credentials = oauth_credentials
        self.developer_token = developer_token

    def _get_ads_service(self, customer_id: str):
        return utils.get_ads_service('GoogleAdsService', ADS_API_VERSION,
                                     self.oauth_credentials,
                                     self.developer_token.get(),
                                     customer_id)

    def _get_oc_service(self, customer_id):
        return utils.get_ads_service('ConversionUploadService', ADS_API_VERSION,
                                     self.oauth_credentials,
                                     self.developer_token.get(),
                                     customer_id)

    def start_bundle(self):
        pass

    def _get_customer_id(self, account_config: AccountConfig, destination: Destination) -> str:
        """
            If the customer_id is present on the destination, returns it, otherwise defaults to the account_config info.
        """
        if len(destination.destination_metadata) >= 2 and len(destination.destination_metadata[1]) > 0:
            return Utils.filter_text_only_numbers(destination.destination_metadata[1])
        return account_config.google_ads_account_id

    def _get_login_customer_id(self, account_config: AccountConfig, destination: Destination) -> str:
        """
            If the customer_id in account_config is a mcc, then login with the mcc account id, otherwise use the customer id.
        """
        if account_config._mcc:
            return account_config.google_ads_account_id

        return self._get_customer_id(account_config, destination)

    @staticmethod
    def _assert_conversion_name_is_present(execution: Execution):
        destination = execution.destination.destination_metadata
        if len(destination) == 0:
            raise ValueError('Missing destination information. Found {}'.format(
                len(destination)))

        if not destination[0]:
            raise ValueError('Missing destination information. Received {}'.format(
                str(destination)))

    # Petlove
    def _get_start_date(self, destination:Destination):
        if len(destination.destination_metadata) <= 3:
            today = datetime.now()
            unix_today = str(datetime(today.year, today.month, today.day, 23, 59, 59)) + ".000"
            return datetime.strptime(unix_today, '%Y-%m-%d %H:%M:%S.%f') - timedelta(days=1)
        
        try:
            return datetime.strptime(destination.destination_metadata[2], '%Y-%m-%d %H:%M:%S.%f')
        
        except Exception as err:
            raise ValueError(f'Wrong format start date information. Exception: {err}')

    # Petlove
    def _get_stop_date(self, destination:Destination):
        if len(destination.destination_metadata) <= 4:
            today = datetime.now()
            unix_today = str(datetime(today.year, today.month, today.day, 23, 59, 59)) + ".000"
            return datetime.strptime(unix_today, '%Y-%m-%d %H:%M:%S.%f') - timedelta(days=1)
        
        try:
            return datetime.strptime(destination.destination_metadata[3], '%Y-%m-%d %H:%M:%S.%f')
        
        except Exception as err:
            raise ValueError(f'Wrong format start date information. Exception: {err}')


    @utils.safe_process(
        logger=logging.getLogger('megalista.GoogleAdsECLeadsUploader'))  # Changed
    def process(self, batch: Batch, **kwargs):
        execution = batch.execution
        self._assert_conversion_name_is_present(execution)
        
        # logging.getLogger(_DEFAULT_LOGGER).info(f'[PETLOVE] execution.account_config: {execution.account_config}')
        # logging.getLogger(_DEFAULT_LOGGER).info(f'[PETLOVE] execution.destination: {execution.destination}')
        

        customer_id = self._get_customer_id(
            execution.account_config, execution.destination)
        # Retrieves the login-customer-id if mcc enabled
        login_customer_id = self._get_login_customer_id(
            execution.account_config, execution.destination)
        # Initiates OCI service
        oc_service = self._get_oc_service(login_customer_id)
        # Initiates ADS service
        ads_service = self._get_ads_service(login_customer_id)
        
        # Asserts Ads is set up for EC for Leads
        self._assert_ec_for_leads_enabled(
            ads_service, customer_id)

        logging.getLogger(_DEFAULT_LOGGER).info(f'[PETLOVE] execution.destination.destination_metadata: {execution.destination.destination_metadata}')

        resource_name = self._get_resource_name(ads_service,
                                                customer_id, execution.destination.destination_metadata[0])

        # Petlove
        start_date = self._get_start_date(execution.destination)
        stop_date = self._get_stop_date(execution.destination)


        # response = self._do_upload(oc_service,
        #                            execution,
        #                            resource_name,
        #                            customer_id,
        #                            batch.elements)
        
        
        response = self._do_upload(oc_service,
                                   execution,
                                   resource_name,
                                   customer_id,
                                   batch.elements, start_date, stop_date)
        
        
        successful_users = [
            user for user in response.results if user.ListFields()]
        logging.getLogger(_DEFAULT_LOGGER).info(
            f'Sucessfully uploaded {len(successful_users)} conversions')

        # all uploaded results do not need to be sent again
        return [batch]

    # Petlove
    def _do_upload(self, oc_service, execution, conversion_resource_name, customer_id, rows, start_date, stop_date):
        logging.getLogger(_DEFAULT_LOGGER).info(
            f'Uploading {len(rows)} offline conversions on {conversion_resource_name} to Google Ads.')
        
        # Petlove
        #logging.getLogger(_DEFAULT_LOGGER).info(f'[PETLOVE] total rows: {len(rows)}')
        
        conversions = []
        
        for conversion in rows:
            
            # Petlove
            # logging.getLogger(_DEFAULT_LOGGER).info(f'[PETLOVE] conversion time: {conversion["time"]}')
            # logging.getLogger(_DEFAULT_LOGGER).info(f'[PETLOVE] start_date: {start_date}, date: {datetime.strptime(conversion["time"], "%Y-%m-%dT%H:%M:%S.%f")}, stop_date: {stop_date}')
        
            if start_date <= datetime.strptime(conversion['time'], '%Y-%m-%dT%H:%M:%S.%f') <= stop_date:
                # logging.getLogger(_DEFAULT_LOGGER).info(f'[PETLOVE] The conversion is in time range. Conversion date: {datetime.strptime(conversion["time"], "%Y-%m-%dT%H:%M:%S.%f")}, start_date: {start_date}, stop_date: {stop_date}')
                conversion_data = {
                    'conversion_action': conversion_resource_name,
                    'conversion_date_time': utils.format_date(conversion['time']),
                    'conversion_value': float(str(conversion['amount'])),
                    'user_identifiers': [{k: v} for (k, v) in conversion.items() if k in ('hashed_email', 'hashed_phone_number')]
                }
                
                # logging.getLogger(_DEFAULT_LOGGER).info(f'[PETLOVE] conversion_data: {conversion_data}')
                
                conversions.append(conversion_data)
                
            # else:
                # logging.getLogger(_DEFAULT_LOGGER).info(f'[PETLOVE] The conversion is NOT in time range. Conversion date: {datetime.strptime(conversion["time"], "%Y-%m-%dT%H:%M:%S.%f")}, start_date: {start_date}, stop_date: {stop_date}')
        
        logging.getLogger(_DEFAULT_LOGGER).info(f'[PETLOVE] total conversions: {len(conversions)}')

        upload_data = {
            'customer_id': customer_id,
            'partial_failure': True,
            'validate_only': False,
            'conversions': conversions,
        }

        # logging.getLogger(_DEFAULT_LOGGER).info(f'[PETLOVE] upload_data: {upload_data}')

        response = oc_service.upload_click_conversions(
            request=upload_data)

        error_message = utils.print_partial_error_messages(
            _DEFAULT_LOGGER, 'uploading enhanced conversions for leads', response)
        if error_message:
            self._add_error(execution, error_message)

        return response

    def _get_resource_name(self, ads_service, customer_id: str, name: str):
        query = f"SELECT conversion_action.resource_name FROM conversion_action WHERE conversion_action.name = '{name}'"
        response_query = ads_service.search_stream(
            customer_id=customer_id, query=query)
        for batch in response_query:
            for row in batch.results:
                return row.conversion_action.resource_name
        raise Exception(
            f'Conversion "{name}" could not be found on account {customer_id}')

    def _assert_ec_for_leads_enabled(self, ads_service, customer_id: str):
        query = f"SELECT customer.id, customer.conversion_tracking_setting.accepted_customer_data_terms, customer.conversion_tracking_setting.enhanced_conversions_for_leads_enabled FROM customer"
        response_query = ads_service.search_stream(
            customer_id=customer_id, query=query)
        for batch in response_query:
            for row in batch.results:
                if (row.customer.conversion_tracking_setting.accepted_customer_data_terms == True
                        and row.customer.conversion_tracking_setting.enhanced_conversions_for_leads_enabled == True):
                    return
                else:
                    raise Exception(
                        f'Google Ads for account {customer_id} has not been set up for Enhanced Conversions for Leads, check https://developers.google.com/google-ads/api/docs/conversions/upload-identifiers'
                    )

    def _get_results_dataframe(self, response):
        successful_users = [
            user for user in response.results if user.ListFields()]
        logging.getLogger(_DEFAULT_LOGGER).info(
            f'Sucessfully uploaded {len(successful_users)} conversions')

        sucess_df = pd.DataFrame(columns=['conversion_action',
                                          'conversion_date_time',
                                          'hashed_email',
                                          'hashed_phone_number'])
