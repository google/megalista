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

import data_sources.data_schemas as DataSchemas
from models.execution import DestinationType
from io import StringIO
import pandas as pd

def test_update_data_types_not_string(mocker):
    csv = "uuid,gclid,mobileDeviceId,\
encryptedUserId,matchId,value,quantity,\
customVariables.type,customVariables.value\n\
123,123,123,123,123,123,123,U1,123\n\
123,123,123,123,123,123,123,U2,456"
    file = StringIO(csv)
    df = pd.read_csv(file, dtype='string')

    destination_type = DestinationType.CM_OFFLINE_CONVERSION

    df = DataSchemas.update_data_types_not_string(df, destination_type)

    assert df.dtypes['uuid'] == 'string'
    assert df.loc[0, 'uuid'] == '123'
    assert df.dtypes['value'] == 'int64'
    assert df.loc[0, 'value'] == 123
    assert df.dtypes['quantity'] == 'int64'
    assert df.loc[0, 'quantity'] == 123

def test_update_data_types_not_string_incomplete(mocker):
    csv = "uuid\n\
123\n\
123"
    file = StringIO(csv)
    df = pd.read_csv(file, dtype='string')

    destination_type = DestinationType.CM_OFFLINE_CONVERSION

    df = DataSchemas.update_data_types_not_string(df, destination_type)

    assert df.dtypes['uuid'] == 'string'
    assert df.loc[0, 'uuid'] == '123'
    assert 'value' not in df.columns
    assert 'quantity' not in df.columns


def test_process_by_destination_type(mocker):
    csv = "uuid,gclid,mobileDeviceId,\
encryptedUserId,matchId,value,quantity,\
customVariables.type,customVariables.value\n\
123,123,123,123,123,123,123,U1,123\n\
123,123,123,123,123,123,123,U2,456"
    file = StringIO(csv)
    df = pd.read_csv(file, dtype='string')

    destination_type = DestinationType.CM_OFFLINE_CONVERSION

    df = DataSchemas.process_by_destination_type(df, destination_type)

    assert df.loc[0, 'customVariables'] == [{ "type": "U1", "value": "123" },{ "type": "U2", "value": "456" }]
    
