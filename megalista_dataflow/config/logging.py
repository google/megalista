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
import sys
from error.logging_handler import LoggingHandler

class LoggingConfig:
    @staticmethod
    def config_logging(show_lines: bool = False):
        # If there is a FileHandler, the execution is running on Dataflow
        # In this scenario, we shouldn't change the formatter
        logging_handler = LoggingHandler()
        logging.getLogger().addHandler(logging_handler)
        file_handler = LoggingConfig.get_file_handler()
        if file_handler is None:
            log_format = "[%(levelname)s] %(name)s: %(message)s"
            if show_lines:
                log_format += "\n... in %(pathname)s:%(lineno)d"
            formatter = logging.Formatter(log_format)

            stream_handler = LoggingConfig.get_stream_handler()

            if stream_handler is None:
                stream_handler = logging.StreamHandler(stream=sys.stderr)
                logging.getLogger().addHandler(stream_handler)
            stream_handler.setFormatter(formatter)

            logging_handler.setFormatter(formatter)
        
        logging.getLogger().setLevel(logging.ERROR)
        logging.getLogger("megalista").setLevel(logging.INFO)
    
    @staticmethod
    def get_stream_handler():
        return LoggingConfig.get_handler(logging.StreamHandler)

    @staticmethod
    def get_file_handler():
        return LoggingConfig.get_handler(logging.FileHandler)


    @staticmethod
    def get_logging_handler():
        return LoggingConfig.get_handler(LoggingHandler)

    @staticmethod
    def get_handler(type: type):
        result_handler = None
        for handler in logging.getLogger().handlers:
            if isinstance(handler, type):
                result_handler = handler
                break
        
        return result_handler