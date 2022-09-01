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

PHONE_1 = "+551199999999"

def test_get_should_hash_fields():

    hasher = AdsUserListPIIHashingMapper()

    # True
    assert hasher._get_should_hash_fields(['ListName', 'Operator', 'True'])
    assert hasher._get_should_hash_fields(['ListName', 'Operator'])
    assert hasher._get_should_hash_fields(['ListName', 'Operator', None])
    assert hasher._get_should_hash_fields(['ListName', 'Operator', ''])
    assert hasher._get_should_hash_fields(['ListName', 'Operator', 'anything'])

    # False
    assert not hasher._get_should_hash_fields(
        ['ListName', 'Operator', 'false'])
    assert not hasher._get_should_hash_fields(
        ['ListName', 'Operator', 'FALSE'])
    assert not hasher._get_should_hash_fields(
        ['ListName', 'Operator', 'False'])


def test_pii_hashing(mocker):

    users = [{
        "email": "john@doe.com",
        "phone": PHONE_1,
        "mailing_address_first_name": "John ",
        "mailing_address_last_name": "Doe",
        "mailing_address_zip": "12345",
        "mailing_address_country": "US"
    },
        {
            "email": "jane@doe.com",
            "phone": "+551199999910",
            "mailing_address_first_name": "Jane",
            "mailing_address_last_name": " Doe",
            "mailing_address_zip": "12345",
            "mailing_address_country": "US"
    },
        {
            "email": "only@email.com",
            "phone": None,
            "mailing_address_first_name": "",
            "mailing_address_last_name": "",
            "mailing_address_zip": "",
            "mailing_address_country": ""
    },
        {
            "email": "",
            "phone": "+551199999910",
            "mailing_address_first_name": "",
            "mailing_address_last_name": "",
            "mailing_address_zip": "",
            "mailing_address_country": ""
    },
        {
            "phone": "+551199999911",
            "mailing_address_first_name": "Incomplete",
            "mailing_address_last_name": "Register",
            "mailing_address_zip": None,
    },
        {
            "phone": "",
            "mailing_address_first_name": "Incomplete",
            "mailing_address_last_name": None,
            "mailing_address_zip": None,
    },
        {
            "email": "ca.us@gmail.com",
            "phone": PHONE_1,
    },
        {
            "email": "us.ca@doe.com",
            "phone": PHONE_1,
    }
    ]

    # Execution mock
    execution = mocker.MagicMock()
    execution.destination.destination_metadata = ['Audience', 'ADD']

    batch = Batch(execution, users)

    # Call
    hasher = AdsUserListPIIHashingMapper()
    hashed = hasher.hash_users(batch).elements

    # only valid entries to hash
    assert len(hashed) == 7

    assert hashed[0] == {
        'hashed_email': 'd709f370e52b57b4eb75f04e2b3422c4d41a05148cad8f81776d94a048fb70af',
        'hashed_phone_number': 'a58d4dce9db87c65ebb6137f91edb9bbe7f274f5b0d07eea82f756ea70532b9c',
        'address_info': {
            'country_code': 'US',
            'hashed_first_name': '96d9632f363564cc3032521409cf22a852f2032eec099ed5967c0d000cec607a',
            'hashed_last_name': '799ef92a11af918e3fb741df42934f3b568ed2d93ac1df74f1b8d41a27932a6f',
            'postal_code': '12345'
        }}

    assert hashed[1] == {
        'hashed_email': '7c815580ad3844bcb627c74d24eaf700e1a711d9c23e9beb62ab8d28e8cb7954',
        'hashed_phone_number': 'd9303375de7036858c05f5836dd6db59d7f66899d3c8f85fbf09a8b60c79b236',
        'address_info': {
            'country_code': 'US',
            'hashed_first_name': '81f8f6dde88365f3928796ec7aa53f72820b06db8664f5fe76a7eb13e24546a2',
            'hashed_last_name': '799ef92a11af918e3fb741df42934f3b568ed2d93ac1df74f1b8d41a27932a6f',
            'postal_code': '12345'
        }}

    assert hashed[2] == {
        'hashed_email': '785af30a27e429e1a2dc2f5e589d59f268239db551c3af29821eb0b3f05d40af'}

    assert hashed[3] == {
        'hashed_phone_number': 'd9303375de7036858c05f5836dd6db59d7f66899d3c8f85fbf09a8b60c79b236'}

    assert hashed[4] == {
        'hashed_phone_number': 'd8d1da09dd3584315610e314b781d0b964a260e6311879930aa2ff678a897753'}

    # Check if function normalize_email removed '.' from ca.us@gmail.com
    assert hashed[5] == {
        'hashed_email': '93d8aed730ac1b81df54d22efa758fc707f9f2763b59769d1f36c9ce9ff160b0',
        'hashed_phone_number': 'a58d4dce9db87c65ebb6137f91edb9bbe7f274f5b0d07eea82f756ea70532b9c'}
    # Check if function normalize_email ignores '.' for non gmail ou googlemail
    assert hashed[6] == {
        'hashed_email': '5de5320a299a39f8c370f6940b481ce30a46ac835d11632d99220ab0a0993dbf',
        'hashed_phone_number': 'a58d4dce9db87c65ebb6137f91edb9bbe7f274f5b0d07eea82f756ea70532b9c'}


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
        'hashed_email': 'john@doe.com',
        'address_info': {
            'country_code': 'US',
            'hashed_first_name': 'John',
            'hashed_last_name': 'Doe',
            'postal_code': '12345'
        }}

    assert hashed[1] == {
        'hashed_email': 'jane@doe.com',
        'address_info': {
            'country_code': 'US',
            'hashed_first_name': 'Jane',
            'hashed_last_name': 'Doe',
            'postal_code': '12345'
        }}
