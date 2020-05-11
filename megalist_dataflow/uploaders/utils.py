# Copyright 2020 Google LLC
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

def extract_rows(elements):
  return [dict['row'] for dict in elements]
 
def safe_process(logger):
  def deco(func):
    def inner(*args, **kwargs):
      elements = args[1]
      if len(elements) == 0:
        logger.warning('Skipping upload, received no elements.')
        return
      logger.info(f'Uploading {len(elements)} rows...')
      try:
        func(*args, *kwargs)
      except Exception as e:
        logger.error(f"Error uploading SSD data for :{extract_rows(elements)}")
        logger.error(f"Exception: {e}")
    return inner
  return deco