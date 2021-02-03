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

from pytz import timezone

MAX_RETRIES = 3


def extract_rows(elements):
  return [element['row'] for element in elements if 'row' in element]


def safe_process(logger):
  def deco(func):
    def inner(*args, **kwargs):
      elements = args[1]
      if not elements:
        logger.warning('Skipping upload, received no elements.')
        return
      logger.info(f'Uploading {len(elements)} rows...')
      try:
        return func(*args, *kwargs)
      except Exception as e:
        logger.error(f'Error uploading data for :{extract_rows(elements)}')
        logger.error(e, exc_info=True)
        logger.exception('Error uploading data.')

    return inner

  return deco


def safe_call_api(function, logger, *args, **kwargs):
  current_retry = 1
  _do_safe_call_api(function, logger, current_retry, *args, **kwargs)


def _do_safe_call_api(function, logger, current_retry, *args, **kwargs):
  try:
    return function(*args, *kwargs)
  except Exception as e:
    if current_retry < MAX_RETRIES:
      logger.exception(f'Fail number {current_retry}. Stack track follows. Trying again.')
      current_retry += 1
      return _do_safe_call_api(function, logger, current_retry, *args, **kwargs)


def convert_datetime_tz(dt, origin_tz, destination_tz):
  datetime_obj = timezone(origin_tz).localize(dt)
  return datetime_obj.astimezone(timezone(destination_tz))
