#!/bin/bash
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

if [ $# != 2 ]; then
    echo "Usage: $0 client_id client_secret"
    exit 1
fi

pip3 install --user -q -r megalist_dataflow/requirements.txt 
python3 cloud_config/generate_megalist_token.py --client_id $1 --client_secret $2
