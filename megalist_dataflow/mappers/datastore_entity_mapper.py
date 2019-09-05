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

import math
from apache_beam.io.gcp.datastore.v1new.types import Key, Entity


class DatastoreEntityMapper():
    def __init__(self, project, batch_size_bytes):
        self.project = project
        self.batch_size_bytes = batch_size_bytes

    def _create_entity(self, values, id):
        key = Key(['megalist', 'bloom_filter', 'batch',
                   str(id)], project=self.project)
        entity = Entity(key=key, exclude_from_indexes=['array'])
        entity.set_properties(
            {'array': bytes(values)[id*self.batch_size_bytes:(id+1)*self.batch_size_bytes]})
        return entity

    def batch_entities(self, value):
        batches = math.ceil((len(bytes(value)) / self.batch_size_bytes))
        entities = [self._create_entity(value, i) for i in range(0, batches)]
        return entities
