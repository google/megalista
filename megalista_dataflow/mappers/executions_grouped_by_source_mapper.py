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

import logging

from models.execution import Batch
from mappers.abstract_list_pii_hashing_mapper import ListPIIHashingMapper
from models.execution import ExecutionsGroupedBySource


class ExecutionsGroupedBySourceMapper():
    def __init__(self):
        self.logger = logging.getLogger("megalista.ExecutionsGroupedBySourceMapper")

    def encapsulate(self, element):
        return ExecutionsGroupedBySource(element[0], element[1])