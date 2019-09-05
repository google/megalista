#!/bin/bash
# Copyright 2019 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
cd megalist_dataflow
python3 -m main --runner DataflowRunner --project vuru-beacons-test --gcp_project_id vuru-beacons-test --temp_location gs://megalist-data/tmp/  --setup_file ./setup.py --template_location gs://megalist-data/templates/megalist
gsutil cp megalist_metadata gs://megalist-data/templates/megalist_metadata
cd ..
cd cloud_functions
cd is_new_buyer
gcloud functions deploy is_new_buyer --runtime python37 --trigger-http
cd ..
cd ..
cd examples
gsutil cp generate_megalist_token.py gs://megalist-data/generate_megalist_token.py
cd ..