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

from megalist_dataflow.mappers.ads_user_list_pii_hashing_mapper import AdsUserListPIIHashingMapper


def test_pii_hashing():
    hasher = AdsUserListPIIHashingMapper()
    users = [{
        "email": "john@doe.com",
        "mailing_address_first_name": "John",
        "mailing_address_last_name": "Doe",
        "mailing_address_zip": "12345",
        "mailing_address_country": "US"
    },
        {
        "email": "jane@doe.com",
        "mailing_address_first_name": "Jane",
        "mailing_address_last_name": "Doe",
        "mailing_address_zip": "12345",
        "mailing_address_country": "US"
    }]
    hashed = hasher.hash_users(users)
    assert len(hashed) == 2
    assert hashed[0] == {
        'hashedEmail': 'd709f370e52b57b4eb75f04e2b3422c4d41a05148cad8f81776d94a048fb70af',
        'addressInfo': {
            'countryCode': 'US',
            'hashedFirstName': '96d9632f363564cc3032521409cf22a852f2032eec099ed5967c0d000cec607a',
            'hashedLastName': '799ef92a11af918e3fb741df42934f3b568ed2d93ac1df74f1b8d41a27932a6f',
            'zipCode': '12345'
        }}
    assert hashed[1] == {
    'hashedEmail': '7c815580ad3844bcb627c74d24eaf700e1a711d9c23e9beb62ab8d28e8cb7954',
    'addressInfo': {
        'countryCode': 'US',
        'hashedFirstName': '81f8f6dde88365f3928796ec7aa53f72820b06db8664f5fe76a7eb13e24546a2',
        'hashedLastName': '799ef92a11af918e3fb741df42934f3b568ed2d93ac1df74f1b8d41a27932a6f',
        'zipCode': '12345'
    }}
