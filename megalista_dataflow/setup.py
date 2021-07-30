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
    name='megalista_dataflow',
    version='4.2',
    author='Google',
    author_email='megalista-admin@google.com',
    url='https://github.com/google/megalista/',
    install_requires=['google-ads==13.0.0', 'google-api-python-client==2.15.0',
                      'google-cloud-core==1.7.2', 'google-cloud-bigquery==2.23.2',
                      'google-cloud-datastore==1.15.3', 'aiohttp==3.7.4',
                      'google-cloud-storage==1.41.1', 'google-cloud-firestore==2.2.0'],
    packages=setuptools.find_packages(),
)