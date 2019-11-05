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


class PIIHashingMapper():
    def _hash_field(self, s):
        import hashlib
        return hashlib.sha256(s.strip().lower().encode('utf-8')).hexdigest()

    def _hash_user(self, user):
        hashed = dict()
        try:
            if 'email' in user:
                hashed['hashedEmail'] = self._hash_field(user['email'])
        except:
            print("Error hashing email for user: %s" % user)

        try:
            if 'mailing_address_first_name' in user and 'mailing_address_last_name' in user:
                hashed['addressInfo'] = {
                    'hashedFirstName': self._hash_field(user['mailing_address_first_name']),
                    'hashedLastName': self._hash_field(user['mailing_address_last_name']),
                    'countryCode': user['mailing_address_country'],
                    'zipCode': user['mailing_address_zip']
                }
        except:
            print("Error hashing address for user: %s" % user)

        try:
            if 'phone'in user:
                hashed['hashedPhoneNumber'] = self._hash_field(user['phone'])
        except:
            print("Error hashing phone for user: %s" % user)

        try:
            if 'mobile_device_id' in user:
                hashed['mobileId'] = user['mobile_device_id']
        except:
            print("Error hashing mobile_device_id for user: %s" % user)
        
        return hashed

    def hash_users(self, users):
        return [self._hash_user(user) for user in users]
