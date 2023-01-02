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

import datetime
import logging
import pytz
import math
import re

from typing import Optional
from models.execution import Batch
from uploaders.uploaders import MegalistaUploader
    
from config.utils import Utils as BaseUtils

class Utils(BaseUtils):
    _TIMEZONE = pytz.timezone('America/Sao_Paulo')
    _MAX_RETRIES = 3
    
    @staticmethod
    def format_date(date):
        if isinstance(date, datetime.datetime):
            pdate = date
        else:
            pdate = datetime.datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%f')

        pdate = Utils._TIMEZONE.localize(pdate)
        str_timezone = pdate.strftime("%z")
        return f'{datetime.datetime.strftime(pdate, "%Y-%m-%d %H:%M:%S")}{str_timezone[-5:-2]}:{str_timezone[-2:]}'

    @staticmethod
    def get_timestamp_micros(date):
        if isinstance(date, datetime.datetime):
            pdate = date
        else:
            pdate = datetime.datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%f')

        return math.floor(pdate.timestamp() * 10e5)
        
    
    @staticmethod
    def safe_process(logger):
        def deco(func):
            def inner(*args, **kwargs):
                self_ = args[0]
                batch = args[1]
                if not batch:
                    logger.info('Skipping upload, received no elements.')
                    return
                logger.info(f'Uploading {len(batch.elements)} rows...')
                try:
                    return func(*args, **kwargs)
                except BaseException as e:
                    self_._add_error(batch.execution, f'Error uploading data: {e}')
                    logger.error(f'Error uploading data for :{batch.elements}')
                    logger.error(e, exc_info=True)
                    logger.exception('Error uploading data.')

            return inner

        return deco

    @staticmethod
    def safe_call_api(function, logger, *args, **kwargs):
        current_retry = 1
        _do_safe_call_api(function, logger, current_retry, *args, **kwargs)

    @staticmethod
    def _do_safe_call_api(function, logger, current_retry, *args, **kwargs):
        try:
            return function(*args, *kwargs)
        except Exception as e:
            if current_retry < Utils._MAX_RETRIES:
                logger.exception(
                    f'Fail number {current_retry}. Stack track follows. Trying again.')
                current_retry += 1
                return _do_safe_call_api(function, logger, current_retry, *args, **kwargs)

    @staticmethod
    def convert_datetime_tz(dt, origin_tz, destination_tz):
        datetime_obj = pytz.timezone(origin_tz).localize(dt)
        return datetime_obj.astimezone(pytz.timezone(destination_tz))
