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

from models.execution import Batch


class FieldHasher:
    def __init__(self, should_hash_fields):
        self.should_hash_fields = should_hash_fields

    def hash_field(self, field):
        import hashlib

        if self.should_hash_fields:
            return hashlib.sha256(field.strip().lower().encode("utf-8")).hexdigest()

        return field


def _is_data_present(dict, key):
    return key in dict and dict[key] is not None and dict[key] != ""


class AdsUserListPIIHashingMapper:
    def __init__(self):
        self.logger = logging.getLogger("megalista.AdsUserListPIIHashingMapper")

    def _hash_user(self, user, hasher):
        hashable_keys = ("email",
                         "mailing_address_first_name",
                         "mailing_address_last_name",
                         "mailing_address_country",
                         "mailing_address_zip",
                         "phone",
                         "mobile_device_id",
                         "user_id")
        hashed = {}
        # include non PII keys
        for k,v in user.items():
          if k not in hashable_keys:
            hashed[k] = v

        try:
            if _is_data_present(user, "email"):
                hashed["hashed_email"] = hasher.hash_field(user["email"])
        except:
            self.logger.error(f"Error hashing email for user: {str(user)}")

        try:
            if (
                _is_data_present(user, "mailing_address_first_name")
                and _is_data_present(user, "mailing_address_last_name")
                and _is_data_present(user, "mailing_address_country")
                and _is_data_present(user, "mailing_address_zip")
            ):
                hashed["address_info"] = {
                    "hashed_first_name": hasher.hash_field(
                        user["mailing_address_first_name"]
                    ),
                    "hashed_last_name": hasher.hash_field(
                        user["mailing_address_last_name"]
                    ),
                    "country_code": user["mailing_address_country"],
                    "postal_code": user["mailing_address_zip"],
                }
        except:
            self.logger.error(f"Error hashing address for user: {str(user)}")

        try:
            if _is_data_present(user, "phone"):
                hashed["hashed_phone_number"] = hasher.hash_field(user["phone"])
        except:
            self.logger.error(f"Error hashing phone for user: {str(user)}")

        if _is_data_present(user, "mobile_device_id"):
            hashed["mobile_id"] = user["mobile_device_id"]

        try:
            if _is_data_present(user, "user_id"):
                hashed["third_party_user_id"] = hasher.hash_field(user["user_id"])
        except:
            self.logger.error(f"Error hashing user_id for user: {str(user)}")

        return hashed

    def _get_should_hash_fields(self, metadata_list):

        if len(metadata_list) < 3:
            return True

        should_hash_fields = metadata_list[2]

        if not should_hash_fields:
            return True

        return should_hash_fields.lower() != "false"

    def hash_users(self, batch: Batch):

        should_hash_fields = self._get_should_hash_fields(
            batch.execution.destination.destination_metadata
        )
        self.logger.debug(f"Should hash fields is {str(should_hash_fields)}")

        hashed_elements = [
            self._hash_user(element, FieldHasher(should_hash_fields))
            for element in batch.elements
        ]

        return Batch(
            batch.execution, [element for element in hashed_elements if element]
        )
