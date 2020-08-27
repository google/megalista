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

from mappers.datastore_entity_mapper import DatastoreEntityMapper
from apache_beam.io.gcp.datastore.v1new.types import Key


def test_datastore_mapper():
    mapper = DatastoreEntityMapper("my-project", 2)
    batched = mapper.batch_entities("abcd".encode())
    assert len(batched) == 2
    assert batched[0].key == Key(['megalist', 'bloom_filter', 'batch', '0'], project="my-project")
    assert batched[1].key == Key(['megalist', 'bloom_filter', 'batch', '1'], project="my-project")
    assert batched[0].properties['array'] == bytes("ab".encode())
    assert batched[1].properties['array'] == bytes("cd".encode())
