# Copyright 2022 Google LLC
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
import re
from typing import Optional, List
from .error_handling import ErrorNotifier

class LoggingHandler(logging.Handler):
    def __init__(self, log_name: str=None, level=logging.INFO, error_notifier: ErrorNotifier=None):
        self.level = level
        self.filters = []
        self.lock = None
        self._error_notifier: error_notifier
        self._has_errors = False
        self._records: List[logging.LogRecord] = []

        logging.getLogger(log_name).addHandler(self)


    def emit(self, record):
        if record.levelno >= logging.ERROR:
            self._has_errors = True
        self._records.append(record)

    @property
    def has_errors(self) -> bool:
        return self._has_errors

    @property
    def all_records(self) -> List[logging.LogRecord]:
        return self._records

    @property
    def error_records(self) -> List[logging.LogRecord]:
        return list(filter(lambda rec: rec.levelno >= logging.ERROR, self._records))

    @staticmethod
    def format_records(records: List[logging.LogRecord]) -> str:
        if records is not None and len(records) > 0:
            return '- ' + '\n- '.join([str(rec.msg) for rec in records])
        else:
            return None