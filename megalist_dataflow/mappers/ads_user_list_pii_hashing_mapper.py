# Copyright 2020 Google LLC
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


class FieldHasher:

    def __init__(self, should_hash_fields):
        self.should_hash_fields = should_hash_fields

    def hash_field(self, field):
        import hashlib

        if self.should_hash_fields:
            return hashlib.sha256(field.strip().lower().encode('utf-8')).hexdigest()

        return field


class AdsUserListPIIHashingMapper:

    def _hash_user(self, user, hasher):

        hashed = dict()

        try:
            if 'email' in user:
                hashed['hashedEmail'] = hasher.hash_field(user['email'])
        except:
            print("Error hashing email for user: %s" % user)

        try:
            if 'mailing_address_first_name' in user and 'mailing_address_last_name' in user:
                hashed['addressInfo'] = {
                    'hashedFirstName': hasher.hash_field(user['mailing_address_first_name']),
                    'hashedLastName': hasher.hash_field(user['mailing_address_last_name']),
                    'countryCode': user['mailing_address_country'],
                    'zipCode': user['mailing_address_zip']
                }
        except:
            print("Error hashing address for user: %s" % user)

        try:
            if 'phone' in user:
                hashed['hashedPhoneNumber'] = hasher.hash_field(user['phone'])
        except:
            print("Error hashing phone for user: %s" % user)

        try:
            if 'mobile_device_id' in user:
                hashed['mobileId'] = user['mobile_device_id']
        except:
            print("Error hashing mobile_device_id for user: %s" % user)

        try:
            if 'user_id' in user:
                hashed['userId'] = user['user_id']
        except:
            print("Error hashing user_id for user: %s" % user)

        return hashed

    def _get_should_hash_fields(self, metadata_list):

        if len(metadata_list) < 3:
            return True

        should_hash_fields = metadata_list[2]

        if not should_hash_fields:
            return True

        return should_hash_fields.lower() != 'false'

    def hash_users(self, dicts):

        should_hash_fields = self._get_should_hash_fields(dicts[0]['execution'].destination.destination_metadata)
        print('Should hash fields is %s' % should_hash_fields)

        return [{'execution': dict['execution'], 'row': self._hash_user(dict['row'], FieldHasher(should_hash_fields))}
                for dict in dicts]
