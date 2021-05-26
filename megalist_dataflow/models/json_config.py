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

import json
from google.cloud import storage


class JsonConfig:

  def parse_json_from_url(self, url):
    url = url.replace("https://", "")
    url_components = url.split("/", 2)
    bucket_name, file_path = url_components[1], url_components[2]
    bucket = storage.Client().get_bucket(bucket_name)
    blob = bucket.blob(file_path)
    data = json.loads(blob.download_as_string())
    return data

  def get_value(self, config_json, key):
    if key not in config_json or not config_json[key]:
      return None
    return config_json[key]
