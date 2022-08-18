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


from configparser import MissingSectionHeaderError
from typing import List, Dict, Any
from models.execution import Destination, DestinationType, Execution, Batch
import logging
import functools
import pandas as pd
import ast
import re

_dtypes: Dict[str, Dict[str, Any]] = {
    'CM_OFFLINE_CONVERSION': {
        'columns' : [
            {'name': 'uuid', 'required': True, 'data_type': 'string'},
            {'name': 'gclid', 'required': False, 'data_type': 'string'},
            {'name': 'mobileDeviceId', 'required': False, 'data_type': 'string'},
            {'name': 'encryptedUserId', 'required': False, 'data_type': 'string'},
            {'name': 'matchId', 'required': False, 'data_type': 'string'},
            {'name': 'value', 'required': False, 'data_type': 'int'},
            {'name': 'quantity', 'required': False, 'data_type': 'int'},
            {'name': 'timestamp', 'required': False, 'data_type': 'string'},
            {'name': 'customVariables.type', 'required': False, 'data_type': 'string'},
            {'name': 'customVariables.value', 'required': False, 'data_type': 'string'}
        ],
        'groups': [
            ['gclid', 'mobileDeviceId', 'encryptedUserId', 'matchId']
        ]
    },
    'ADS_OFFLINE_CONVERSION': {
        'columns': [
            {'name': 'gclid', 'required': True, 'data_type': 'string'},
            {'name': 'time', 'required': True, 'data_type': 'string'},
            {'name': 'amount', 'required': True, 'data_type': 'string'}
        ],
        'groups': []
    },
    'ADS_OFFLINE_CONVERSION_CALLS': {
        'columns': [
            {'name': 'caller_id', 'required': True, 'data_type': 'string'},
            {'name': 'call_time', 'required': True, 'data_type': 'string'},
            {'name': 'time', 'required': True, 'data_type': 'string'},
            {'name': 'amount', 'required': True, 'data_type': 'string'} 
        ],
        'groups': []
    },
    'ADS_SSD_UPLOAD': {
        'columns': [
            {'name': 'email', 'required': False, 'data_type': 'string'},
            {'name': 'phone', 'required': False, 'data_type': 'string'},
            {'name': 'mailing_address_first_name', 'required': False, 'data_type': 'string'},
            {'name': 'mailing_address_last_name', 'required': False, 'data_type': 'string'},
            {'name': 'mailing_address_country_name', 'required': False, 'data_type': 'string'},
            {'name': 'mailing_address_zip_name', 'required': False, 'data_type': 'string'},
            {'name': 'time', 'required': True, 'data_type': 'string'},
            {'name': 'amount', 'required': True, 'data_type': 'string'}
        ],
        'groups': [
            ['email', 'phone', 'mailing_address_first_name']
        ]
    },
    'ADS_ENHANCED_CONVERSION': {
        'columns': [],
        'groups': []
    },
    'ADS_CUSTOMER_MATCH_CONTACT_INFO_UPLOAD': {
        'columns': [
            {'name': 'email', 'required': False, 'data_type': 'string'},
            {'name': 'phone', 'required': False, 'data_type': 'string'},
            {'name': 'mailing_address_first_name', 'required': False, 'data_type': 'string'},
            {'name': 'mailing_address_last_name', 'required': False, 'data_type': 'string'},
            {'name': 'mailing_address_country_name', 'required': False, 'data_type': 'string'},
            {'name': 'mailing_address_zip_name', 'required': False, 'data_type': 'string'}  
        ],
        'groups': []
    },
    'ADS_CUSTOMER_MATCH_MOBILE_DEVICE_ID_UPLOAD': {
        'columns': [
            {'name': 'mobile_device_id', 'required': True, 'data_type': 'string'}  
        ],
        'groups': []
    },
    'ADS_CUSTOMER_MATCH_USER_ID_UPLOAD': {
        'columns': [
            {'name': 'user_id', 'required': True, 'data_type': 'string'}  
        ],
        'groups': []
    },
    'GA_USER_LIST_UPLOAD': {
        'columns': [],
        'groups': []
    },
    'APPSFLYER_S2S_EVENTS': {
        'columns': [
            {'name': 'uuid', 'required': True, 'data_type': 'string'},
            {'name': 'appsflyer_id', 'required': True, 'data_type': 'string'},
            {'name': 'customer_user_id', 'required': False, 'data_type': 'string'},
            {'name': 'ip', 'required': False, 'data_type': 'string'},
            {'name': 'device_ids_idfa', 'required': False, 'data_type': 'string'},
            {'name': 'device_ids_advertising_id', 'required': False, 'data_type': 'string'},
            {'name': 'device_ids_amazon_aid', 'required': False, 'data_type': 'string'},
            {'name': 'device_ids_oaid', 'required': False, 'data_type': 'string'},
            {'name': 'device_ids_imei', 'required': False, 'data_type': 'string'},
            {'name': 'event_eventName', 'required': True, 'data_type': 'string'},
            {'name': 'event_eventCurrency', 'required': False, 'data_type': 'string'},
            {'name': 'event_eventTime', 'required': False, 'data_type': 'string'},
            {'name': 'event_eventValue', 'required': False, 'data_type': 'string'},
        ],
        'groups': []
    },
    'GA_MEASUREMENT_PROTOCOL': {
        'columns': [
            {'name': 'uuid', 'required': True, 'data_type': 'string'},
            {'name': 'client_id', 'required': False, 'data_type': 'string'},
            {'name': 'user_id', 'required': False, 'data_type': 'string'},
            {'name': 'event_category', 'required': True, 'data_type': 'string'},
            {'name': 'event_action', 'required': True, 'data_type': 'string'},
            {'name': 'event_label', 'required': False, 'data_type': 'string'},
            {'name': 'event_value', 'required': False, 'data_type': 'string'},
            {'name': 'cm\\d+', 'required': False, 'data_type': 'string'},
            {'name': 'cd\\d+', 'required': False, 'data_type': 'string'},
            {'name': 'campaign_source', 'required': False, 'data_type': 'string'},
            {'name': 'campaign_medium', 'required': False, 'data_type': 'string'},
        ],
        'groups': [
            ['client_id', 'user_id']
        ]
    },
    'GA_DATA_IMPORT': {
        'columns': [
            {'name': 'cd\\d+', 'required': True, 'data_type': 'string'},
            {'name': 'cd\\d+', 'required': True, 'data_type': 'string'},
            {'name': 'cd\\d+', 'required': False, 'data_type': 'string'},
        ],
        'groups': []
    },
    'GA_4_MEASUREMENT_PROTOCOL': {
        'columns': [
            {'name': 'uuid', 'required': True, 'data_type': 'string'},
            {'name': 'app_instance_id', 'required': False, 'data_type': 'string'},
            {'name': 'client_id', 'required': False, 'data_type': 'string'},
            {'name': 'name', 'required': False, 'data_type': 'string'},
            {'name': 'user_id', 'required': False, 'data_type': 'string'},
            {'name': 'parameter_\\d+', 'required': False, 'data_type': 'string'},
            {'name': 'user_property_\\d+', 'required': False, 'data_type': 'string'},  
        ],
        'groups': [
            ['app_instance_id', 'client_id']
        ]
    },
    'DV_CUSTOMER_MATCH_CONTACT_INFO_UPLOAD': {
        'columns': [
            {'name': 'email', 'required': False, 'data_type': 'string'},
            {'name': 'phone', 'required': False, 'data_type': 'string'},
            {'name': 'mailing_address_first_name', 'required': False, 'data_type': 'string'},
            {'name': 'mailing_address_last_name', 'required': False, 'data_type': 'string'},
            {'name': 'mailing_address_country_name', 'required': False, 'data_type': 'string'},
            {'name': 'mailing_address_zip_name', 'required': False, 'data_type': 'string'}  
        ],
        'groups': []
    },
    'DV_CUSTOMER_MATCH_DEVICE_ID_UPLOAD': {
        'columns': [
            {'name': 'mobile_device_id', 'required': True, 'data_type': 'string'}  
        ],
        'groups': []
    },
    'UPLOADED_GCLID_TIME': {
        'columns': [
            {'name': 'timestamp', 'required': True, 'data_type': 'string'},
            {'name': 'gclid', 'required': True, 'data_type': 'string'}, 
            {'name': 'time', 'required': True, 'data_type': 'string'}  
        ],
        'groups': []
    },
    'UPLOADED_UUID': {
        'columns': [
            {'name': 'timestamp', 'required': True, 'data_type': 'string'},
            {'name': 'uuid', 'required': True, 'data_type': 'string'}  
        ],
        'groups': []
    }
}

# checks if every column marked as required exists in dataframe columns
def _validate_required_columns(data_cols: List[str], destination_type: DestinationType) -> List[str]:
    data_type = _dtypes[destination_type.name]
    data_type_cols = data_type['columns']
    data_type_colnames = [col['name'] for col in data_type_cols if col['required'] == True]

    missing_cols = []
    for data_type_col in data_type_colnames:
        found = False
        for col in data_cols:
            if re.match(f'^{data_type_col}$', col) is not None:
                found = True
                break
        if not found:
            missing_cols.append(data_type_col)
    
    return missing_cols

# checks if every column group is verified
def _validade_group_columns(data_cols: List[str], destination_type: DestinationType) -> List[List[str]]:
    data_type = _dtypes[destination_type.name]
    data_type_groups = data_type['groups']

    missing_groups = []
    for group in data_type_groups:
        group_validated = False
        for data_type_col in group:
            found = False
            for col in data_cols:
                if re.match(f'^{data_type_col}$', col) is not None:
                    found = True
                    break
            if found:
                group_validated = True
        if not group_validated:
            missing_groups.append(group)

    return missing_groups


# Validade data columns
def validate_data_columns(data_cols: List[str], destination_type: DestinationType) -> bool:
    # checks if every column marked as required exists in dataframe columns
    missing_cols = _validate_required_columns(data_cols, destination_type)

    # checks if every column group is verified
    missing_groups = _validade_group_columns(data_cols, destination_type)

    return len(missing_cols) == 0 and len(missing_groups) == 0

def get_error_message(data_cols: List[str], destination_type: DestinationType) -> str:
    message = []
    missing_cols = _validate_required_columns(data_cols, destination_type)
    if len(missing_cols) > 0:
        message.append(f'Required: [{",".join(missing_cols)}]')
    missing_groups = _validade_group_columns(data_cols, destination_type)
    if len(missing_groups) > 0:
        for group in missing_groups:
            message.append(f'One of [{",".join(group)}]')

    return f'Some columns were missing: {"; ".join(message)}.'

# Get columns (names) that will be considered
def get_cols_names(data_cols: list, destination_type: DestinationType) -> list:
    data_type = _dtypes[destination_type.name]
    data_type_cols = [col['name'] for col in data_type['columns']]
    data_type_optional_cols = [col for col in [group for group in data_type['groups']]]
    
    data_type_cols = data_type_cols + data_type_optional_cols
    filtered_cols = []
    for col in data_cols:
        found = False
        for data_type_col in data_type_cols:
            if re.match(f'^{data_type_col}$', col) is not None:
                filtered_cols.append(col)
    
    return filtered_cols

# Parse columns that aren't string
def update_data_types_not_string(df: pd.DataFrame, destination_type: DestinationType) -> pd.DataFrame:
    # temp_dtypes_to_change = _dtypes_not_string[destination_type.name]
    data_type = _dtypes[destination_type.name]
    cols_to_change = [col['name'] for col in filter(lambda col: col['data_type'] != 'string', data_type['columns'])]
    dtypes_to_change = {}
    for key in cols_to_change:
        if key in df.columns:
            dtypes_to_change[key] = list(filter(lambda col: col['name'] == key, data_type['columns']))[0]['data_type']

    return df.astype(dtypes_to_change)

# Destination_type-specific data treatment
def process_by_destination_type(df: pd.DataFrame, destination_type: DestinationType) -> pd.DataFrame:
    if destination_type == DestinationType.CM_OFFLINE_CONVERSION:
        return _join_custom_variables(df)
    else:
        return df

# Data treatment - CM_OFFLINE_CONVERSION
def _join_custom_variables(df) -> pd.DataFrame:
    df['customVariables'] = '{ "type": "' + df['customVariables.type'] + '", "value": "' + df['customVariables.value'] + '"}'
    df.drop(['customVariables.type', 'customVariables.value'], axis=1, inplace=True)
    df['customVariables'] = df.groupby('uuid')['customVariables'].transform(lambda x: '[' + ', '.join(x) + ']')
    df = df.drop_duplicates()
    df = df.reset_index()
    df = df.drop(['index'], axis=1)
    df['customVariables'] = df['customVariables'].transform(lambda x: ast.literal_eval(x))
    return df

# Get the data type from _dtypes  dict
def get_data_type(data_type_name: str) -> Dict[str, Any]:
    return _dtypes[data_type_name]