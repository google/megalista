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


from models.execution import DestinationType, Execution, Batch

import pandas as pd
import ast

class DataSchemas:
    # data types that aren't  string
    _dtypes_not_string = {
        'CM_OFFLINE_CONVERSION': {'value': 'int', 'quantity': 'int'},
        'ADS_OFFLINE_CONVERSION': {},
        'ADS_SSD_UPLOAD': {'amount': 'int'},
        'ADS_ENHANCED_CONVERSION': {},
        'ADS_CUSTOMER_MATCH_CONTACT_INFO_UPLOAD': {},
        'ADS_CUSTOMER_MATCH_MOBILE_DEVICE_ID_UPLOAD': {},
        'ADS_CUSTOMER_MATCH_USER_ID_UPLOAD': {},
        'GA_USER_LIST_UPLOAD': {},
        'APPSFLYER_S2S_EVENTS': {'event_eventTime': 'datetime'},
        'GA_MEASUREMENT_PROTOCOL': {},
        'GA_DATA_IMPORT': {},
        'GA_4_MEASUREMENT_PROTOCOL': {},
    }

    # Parse columns that aren't string
    def update_data_types_not_string(df: pd.DataFrame, destination_type: DestinationType) -> pd.DataFrame:
        temp_dtypes_to_change = DataSchemas._dtypes_not_string[destination_type.name]
        dtypes_to_change = {}
        for key in temp_dtypes_to_change:
            if key in df.columns:
                dtypes_to_change[key] = temp_dtypes_to_change[key]

        df = df.astype(dtypes_to_change)
        return df

    # Destination_type-specific data treatment
    def process_by_destination_type(df: pd.DataFrame, destination_type: DestinationType) -> pd.DataFrame:
        if destination_type == DestinationType.CM_OFFLINE_CONVERSION:
            df = DataSchemas._join_custom_variables(df)

        return df

    # Data treatment - CM_OFFLINE_CONVERSION
    def _join_custom_variables(df) -> pd.DataFrame:
        # df['customVariables'] = '{ "' + df['customVariables.type'] + '": "' + df['customVariables.value'] + '" }'
        df['customVariables'] = '{ "type": "' + df['customVariables.type'] + '", "value": "' + df['customVariables.value'] + '"}'
        df.drop(['customVariables.type', 'customVariables.value'], axis=1, inplace=True)
        df['customVariables'] = df.groupby('uuid')['customVariables'].transform(lambda x: '[' + ', '.join(x) + ']')
        df = df.drop_duplicates()
        df = df.reset_index()
        df = df.drop(['index'], axis=1)
        df['customVariables'] = df['customVariables'].transform(lambda x: ast.literal_eval(x))
        return df