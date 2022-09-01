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

from models.execution import Batch
from mappers.abstract_list_pii_hashing_mapper import ListPIIHashingMapper


class DVUserListPIIHashingMapper(ListPIIHashingMapper):
    def __init__(self):
        self.logger = logging.get_logger("megalista.DVUserListPIIHashingMapper")

    def _hash_user(self, user, hasher):
        hashable_keys = self._get_default_hasheable_keys()
        processed_user = {}
        # include non PII keys as is (these should not be hashed)
        for k, v in user.items():
            if k not in hashable_keys:
                processed_user[k] = v

        try:
            if self._is_data_present(user, "email"):
                processed_email = self.normalize_email(user["email"])
                processed_user["hashedEmails"] = hasher.hash_field(
                    processed_email)
        except Exception:
            self.logger.error(f"Error hashing email for user: {str(user)}")

        try:
            if (
                self._is_data_present(user, "mailing_address_first_name")
                and self._is_data_present(user, "mailing_address_last_name")
                and self._is_data_present(user, "mailing_address_country")
                and self._is_data_present(user, "mailing_address_zip")
            ):
                processed_user["hashedFirstName"] = hasher.hash_field(
                    user["mailing_address_first_name"])
                processed_user["hashedLastName"] = hasher.hash_field(
                    user["mailing_address_last_name"])
                processed_user["countryCode"] = user["mailing_address_country"]
                processed_user["zipCodes"] = user["mailing_address_zip"]

        except Exception:
            self.logger.error(f"Error hashing address for user: {str(user)}")

        try:
            if self._is_data_present(user, "phone"):
                processed_user["hashedPhoneNumbers"] = hasher.hash_field(
                    user["phone"])
        except Exception:
            self.logger.error(f"Error hashing phone for user: {str(user)}")

        if self._is_data_present(user, "mobile_device_id"):
            processed_user["mobileDeviceIds"] = user["mobile_device_id"]

        return processed_user
