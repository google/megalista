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
from data_sources.file.file_provider import FileProvider
from models.options import DataflowOptions
from models.execution import SourceType

class JsonConfig:
  def __init__(self, dataflow_options: DataflowOptions):
    self._dataflow_options = dataflow_options

  def parse_json_from_url(self, url):
    fileProvider = FileProvider(url, self._dataflow_options, SourceType.FILE, "File Config (JSON)")
    data = json.loads(fileProvider.read().decode('utf-8'))
    return data

  def get_value(self, config_json, key):
    if key not in config_json or not config_json[key]:
      return None
    return config_json[key]
