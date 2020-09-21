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
from reducers.bloom_filter_reducer import BloomFilterReducer
from bloom_filter import BloomFilter
from megalist_dataflow.reducers.bloom_filter_reducer import BloomFilterReducer


def test_bloom_filter():
  bloom = BloomFilterReducer(5000)
  acc = bloom.create_accumulator()
  acc = bloom.add_input(acc, [{'user_id': 'teste@teste.com'}])
  acc = bloom.add_input(acc, [{'user_id': 'teste2@teste.com'}])
  acc2 = bloom.create_accumulator()
  acc2 = bloom.add_input(acc, [{'user_id': 'teste3@teste.com'}])
  result = bloom.merge_accumulators([acc, acc2])
  result_array = bloom.extract_output(result)
  bloom_result = BloomFilter(max_elements=5000, error_rate=0.1)
  bloom_result.backend.array_ = result_array
  assert 'teste@teste.com' in bloom_result
  assert 'teste2@teste.com' in bloom_result
  assert 'teste3@teste.com' in bloom_result
  assert 'teste4@teste.com' not in bloom_result
