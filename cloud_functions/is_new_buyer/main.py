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
import array
import json
import math
from google.cloud import datastore
from bloom_filter import BloomFilter

max_elements = 50000000

def create_bloom_filter():
    datastore_client = datastore.Client()
    bloom = BloomFilter(max_elements=max_elements, error_rate=0.1)
    arr = bytes()    
    for i in range(0, math.ceil(len(bytes(bloom.backend.array_)) / 1024000)):
        key = datastore_client.key('megalist', 'bloom_filter', 'batch', str(i))
        result = datastore_client.get(key)
        if result is not None:
            arr += result['array']
    bloom.backend.array_ = array.array('L', arr)
    return bloom

bloom_filter = create_bloom_filter()

def is_new_buyer(request):
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET',
        'Access-Control-Allow-Headers': 'Authorization',
        'Access-Control-Max-Age': '3600',
        'Access-Control-Allow-Credentials': 'true',
        'Content-Type': 'application/json'
    }
    if request.method == 'OPTIONS':
        return ('', 204, headers)

    return (json.dumps({'result': request.args['id'] in bloom_filter}), 200, headers)
