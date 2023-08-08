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


if [ $# != 7 ]; then
    echo "Usage: $0 gcp_project_id bucket_name region service_account_email image_repository_name image_name image_version"
    exit 1
fi

echo "Move to megalista_dataflow folder"
cd megalista_dataflow
echo "Configuration GCP project in gcloud"
gcloud config set project "$1"
echo "Upgrading pip to latest version"
python3 -m pip install --upgrade pip
echo "Build Dataflow metadata"
python3 -m pip install --user -q -r requirements.txt
echo $4
echo "Update commit info inside code"
sed -i "s/MEGALISTA_VERSION\s*=.*/MEGALISTA_VERSION = '$(git rev-parse HEAD)'/" ./config/version.py
python -m main --runner=DataflowRunner --project "$1" --region "$3" --template_location "gs://$2/templates/megalista" --temp_location "gs://$2/tmp/" --num_workers 1 --service_account_email "$4" --experiments=use_runner_v2 --sdk_container_image="$3-docker.pkg.dev/$1/$5/dataflow/$6:$7" --sdk_location=container
echo "Copy megalista_medata to bucket $2"
gsutil cp megalista_metadata "gs://$2/templates/megalista_metadata"
echo "Cleanup"
sed -i "s/MEGALISTA_VERSION\s*=.*/MEGALISTA_VERSION = '\[megalista_version\]'/" ./config/version.py
cd ..
echo "Finished"
