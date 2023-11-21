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

import datetime
import logging
from typing import Optional

from error.error_handling import ErrorHandler
from models.execution import AccountConfig, Batch, Destination, Execution
from uploaders import utils
from utils.utils import Utils
from uploaders.google_ads import ADS_API_VERSION
from uploaders.uploaders import MegalistaUploader


class GoogleAdsSSIUploaderDoFn(MegalistaUploader):
    def __init__(self, oauth_credentials, developer_token, error_handler: ErrorHandler):
        super().__init__(error_handler)
        self.oauth_credentials = oauth_credentials
        self.developer_token = developer_token
        self.active = developer_token is not None

    def _get_offline_user_data_job_service(self, customer_id):
        return utils.get_ads_service(
            "OfflineUserDataJobService",
            ADS_API_VERSION,
            self.oauth_credentials,
            self.developer_token.get(),
            customer_id,
        )

    @staticmethod
    def _assert_conversion_metadata_is_present(execution: Execution):
        metadata = execution.destination.destination_metadata
        if len(metadata) < 3:
            raise ValueError(
                f"Missing destination information. Received {len(metadata)} entry(ies)"
            )

    @utils.safe_process(logger=logging.getLogger("megalista.GoogleAdsSSIUploader"))
    def process(self, batch: Batch, **kwargs):
        execution = batch.execution
        self._assert_conversion_metadata_is_present(execution)

        custom_key = self._get_custom_key(execution.destination)
        customer_id = self._get_customer_id(
            execution.account_config, execution.destination
        )
        login_customer_id = self._get_login_customer_id(
            execution.account_config, execution.destination
        )
        user_data_consent = self._get_user_data_consent(
            execution.account_config, execution.destination
        )
        ad_personalization = self._get_ad_personalization_consent(
            execution.account_config, execution.destination
        )

        offline_user_data_job_service = self._get_offline_user_data_job_service(
            login_customer_id
        )

        ads_service = self._get_ads_service(login_customer_id)

        conversion_action_resource_name = self._get_resource_name(
            ads_service, customer_id, execution.destination.destination_metadata[0]
        )

        self._do_upload(
            execution,
            offline_user_data_job_service,
            customer_id,
            custom_key,
            conversion_action_resource_name,
            user_data_consent,
            ad_personalization,
            batch.elements,
        )

        return [execution]

    def _do_upload(
        self,
        execution,
        offline_user_data_job_service,
        customer_id,
        custom_key,
        conversion_action_resource_name,
        user_data_consent,
        ad_personalization,
        rows,
    ):
        logger = logging.getLogger("megalista.GoogleAdsSSIUploader")

        # Upload is divided into 3 parts:
        # 1. Creates Job
        # 2. Creates operations (data insertion)
        # 3. Runs the Job

        # 1. Creates Job
        job_creation_payload = {
            "type_": "STORE_SALES_UPLOAD_FIRST_PARTY",
            "external_id": int(datetime.datetime.now().timestamp() * 10e3),
            "store_sales_metadata": {
                "loyalty_fraction": 1.0,
                "transaction_upload_fraction": 1.0,
                **({"custom_key": custom_key} if custom_key else {}),
            },
        }

        job_resource_name = offline_user_data_job_service.create_offline_user_data_job(
            customer_id=customer_id, job=job_creation_payload
        ).resource_name

        # Sets consent info if any
        consent = {}
        if user_data_consent:
            consent["ad_user_data"] = user_data_consent
        if ad_personalization:
            consent["ad_personalization"] = ad_personalization

        # 2. Creates operations (data insertion)
        data_insertion_payload = {
            "resource_name": job_resource_name,
            "enable_partial_failure": False,
            "operations": [
                {
                    "create": {
                        "user_identifiers": [
                            {k: v}
                            for (k, v) in conversion.items()
                            if k not in ("amount", "time", "currency_code", "custom_value")
                        ],
                        "transaction_attribute": {
                            "conversion_action": conversion_action_resource_name,
                            "currency_code": conversion["currency_code"],
                            "transaction_amount_micros": int(conversion["amount"]),
                            "transaction_date_time": utils.format_date(
                                conversion["time"]
                            ),
                            **(
                                {"custom_value": conversion["custom_value"]}
                                if "custom_value" in conversion and custom_key
                                else {}
                            ),
                        },
                        **({"consent": consent} if consent else {}),
                    }
                }
                for conversion in rows
            ],
        }

        data_insertion_response = (
            offline_user_data_job_service.add_offline_user_data_job_operations(
                request=data_insertion_payload
            )
        )

        error_message = utils.print_partial_error_messages(
            logger, "uploading ssi", data_insertion_response
        )
        if error_message:
            self._add_error(execution, error_message)

        # 3. Runs the Job
        offline_user_data_job_service.run_offline_user_data_job(
            resource_name=job_resource_name
        )

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

    def _get_custom_key(self, destination: Destination) -> Optional[str]:
        """
        If the custom_key is present on the destination, return it, otherwise returns none.
        """
        if (
            len(destination.destination_metadata) >= 4
            and destination.destination_metadata[3] is not None
        ):
            return destination.destination_metadata[3]
        return None

    def _get_user_data_consent(
        self, account_config: AccountConfig, destination: Destination
    ) -> Optional[str]:
        """
        Specifies whether user consent was obtained for the data you are uploading.
        https://www.google.com/about/company/user-consent-policy
        """
        if (
            len(destination.destination_metadata) >= 5
            and len(destination.destination_metadata[4]) > 0
        ):
            return destination.destination_metadata[4]
        return None

    def _get_ad_personalization_consent(
        self, account_config: AccountConfig, destination: Destination
    ) -> Optional[str]:
        """
        Specifies whether user consent was obtained for the data you are uploading.
        https://www.google.com/about/company/user-consent-policy
        """
        if (
            len(destination.destination_metadata) >= 6
            and len(destination.destination_metadata[5]) > 0
        ):
            return destination.destination_metadata[5]
        return None

    def _get_login_customer_id(
        self, account_config: AccountConfig, destination: Destination
    ) -> str:
        """
        If the customer_id in account_config is a mcc, then login with the mcc account id, otherwise use the customer id.
        """
        if account_config._mcc:
            return account_config.google_ads_account_id

        return self._get_customer_id(account_config, destination)

    def _get_ads_service(self, customer_id: str):
        return utils.get_ads_service(
            "GoogleAdsService",
            ADS_API_VERSION,
            self.oauth_credentials,
            self.developer_token.get(),
            customer_id,
        )

    def _get_resource_name(self, ads_service, customer_id: str, name: str):
        query = f"SELECT conversion_action.resource_name FROM conversion_action WHERE conversion_action.name = '{name}'"
        response_query = ads_service.search_stream(customer_id=customer_id, query=query)
        for batch in response_query:
            for row in batch.results:
                return row.conversion_action.resource_name
        raise Exception(
            f'Conversion "{name}" could not be found on account {customer_id}'
        )
