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

from mappers.ads_user_list_pii_hashing_mapper import AdsUserListPIIHashingMapper

from models.execution import Batch


def test_get_should_hash_fields():

    hasher = AdsUserListPIIHashingMapper()

    # True
    assert hasher._get_should_hash_fields(['ListName', 'Operator', 'True'])
    assert hasher._get_should_hash_fields(['ListName', 'Operator'])
    assert hasher._get_should_hash_fields(['ListName', 'Operator', None])
    assert hasher._get_should_hash_fields(['ListName', 'Operator', ''])
    assert hasher._get_should_hash_fields(['ListName', 'Operator', 'anything'])

    # False
    assert not hasher._get_should_hash_fields(['ListName', 'Operator', 'false'])
    assert not hasher._get_should_hash_fields(['ListName', 'Operator', 'FALSE'])
    assert not hasher._get_should_hash_fields(['ListName', 'Operator', 'False'])


def test_pii_hashing(mocker):

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

    # Execution mock
    execution = mocker.MagicMock()
    execution.destination.destination_metadata = ['Audience', 'ADD']

    batch = Batch(execution, [users[0], users[1]])

    # Call
    hasher = AdsUserListPIIHashingMapper()
    hashed = hasher.hash_users(batch).elements

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


def test_avoid_pii_hashing(mocker):
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

    # Mock the execution
    execution = mocker.MagicMock()
    execution.destination.destination_metadata = ['Audience', 'ADD', 'False']

    batch = Batch(execution, [users[0], users[1]])

    # Call
    hasher = AdsUserListPIIHashingMapper()
    hashed = hasher.hash_users(batch).elements

    assert len(hashed) == 2

    assert hashed[0] == {
        'hashedEmail': 'john@doe.com',
        'addressInfo': {
            'countryCode': 'US',
            'hashedFirstName': 'John',
            'hashedLastName': 'Doe',
            'zipCode': '12345'
        }}

    assert hashed[1] == {
        'hashedEmail': 'jane@doe.com',
        'addressInfo': {
            'countryCode': 'US',
            'hashedFirstName': 'Jane',
            'hashedLastName': 'Doe',
            'zipCode': '12345'
        }}
