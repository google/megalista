#!/bin/bash
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

echo "Environment variables handling"

ENV_VARS=`env | grep ^MEGALISTA_.*`
params=""

for var in $ENV_VARS;
do
    IFS='='
    read key value <<< "$var"
    key=`echo "${key}" | tr [:upper:] [:lower:]`
    if [[ $key == megalista_* ]]
    then
        params="${params} --${key:10} ${value}"
    fi
    IFS=' '
done

# Checks if the service-account file is filled 
if [ ! -z "$(cat service-account-file.json | jq -r '.project_id')" ]; then
   export GOOGLE_APPLICATION_CREDENTIALS=/app/service-account-file.json
   echo "Service Account configuration being used"
fi

echo "Running Megalista"
python megalista_dataflow/main.py \
    --runner DirectRunner \
    ${params}