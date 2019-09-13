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

from bloom_filter import BloomFilter
import apache_beam as beam
import array


class BloomFilterReducer(beam.CombineFn):
    def __init__(self, max_elements):
        self.max_elements = max_elements

    def create_accumulator(self):
        return BloomFilter(max_elements=self.max_elements, error_rate=0.1)

    def add_input(self, acc, users):
        for user in users:
            acc.add(user['user_id'])
        return acc

    def merge_accumulators(self, accumulators):
        bloom = BloomFilter(max_elements=self.max_elements, error_rate=0.1)
        for acc in accumulators:
            bloom.union(acc)
        return bloom

    def extract_output(self, acc):
        return acc.backend.array_
