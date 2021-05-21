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

import setuptools

setuptools.setup(
    name='megalist_dataflow',
    version='0.1',
    author='Alvaro Stivi',
    author_email='astivi@google.com',
    url='https://cse.googlesource.com/solutions/megalist',
    install_requires=['google-ads==10.0.0', 'google-api-python-client==1.10.0',
                      'google-cloud-core==1.3.0', 'google-cloud-bigquery==1.26.0',
                      'google-cloud-datastore==1.13.1', 'aiohttp==3.6.2'],
    packages=setuptools.find_packages(),
)
