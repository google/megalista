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
import io
import traceback
from types import FrameType
from typing import Optional, Tuple, List, Any

from models.execution import Execution

class LoggingConfig:
    @staticmethod
    def config_logging(show_lines: bool = False):
        # If there is a FileHandler, the execution is running on Dataflow
        # In this scenario, we shouldn't change the formatter
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
        return None

    @staticmethod
    def get_handler(type: type):
        result_handler = None
        for handler in logging.getLogger().handlers:
            if isinstance(handler, type):
                result_handler = handler
                break
        
        return result_handler

class _LogWrapper:
    def __init__(self, name: Optional[str]):
        self._name = str(name)
        self._logger = logging.getLogger(name)

    def debug(self, msg: str, *args, **kwargs):
        self._log(msg, logging.DEBUG, *args, **kwargs)
    
    def info(self, msg: str, *args, **kwargs):
        self._log(msg, logging.INFO, *args, **kwargs)
        
    def warning(self, msg: str, *args, **kwargs):
        self._log(msg, logging.WARNING, *args, **kwargs)
        
    def error(self, msg: str, *args, **kwargs):
        self._log(msg, logging.ERROR, *args, **kwargs)
    
    def critical(self, msg: str, *args, **kwargs):
        self._log(msg, logging.CRITICAL, *args, **kwargs)

    def exception(self, msg: str, *args, **kwargs):
        self._log(msg, logging.CRITICAL, *args, **kwargs)
    
    def _log(self, msg: str, level: int, *args, **kwargs):
        stacklevel = self._get_stacklevel(**kwargs)
        msg = self._get_msg_execution(msg, **kwargs)
        msg = self._get_msg_context(msg, **kwargs)
        if level >= logging.ERROR:
            _add_error(self._name, msg, stacklevel, level, args)
            if level == logging.ERROR:
                level = logging.WARNING
        keys_to_remove = ['execution', 'context']
        for key in keys_to_remove:
            if key in kwargs:
                del kwargs[key]
        self._logger.log(level, msg, *args, **self._change_stacklevel(**kwargs))
    
    def _change_stacklevel(self, **kwargs):
        stacklevel = self._get_stacklevel(**kwargs)
        return dict(kwargs, stacklevel = stacklevel)
    
    def _get_stacklevel(self, **kwargs):
        dict_kwargs = dict(kwargs)
        stacklevel = 3
        if 'stacklevel' in dict_kwargs:
            stacklevel = 2 + dict_kwargs['stacklevel']
        return stacklevel

    def _get_msg_context(self, msg: str, **kwargs):
        if 'context' in kwargs:
            context = kwargs['context']
            msg = f'[Context: {context}] {msg}'
        return msg

    def _get_msg_execution(self, msg: str, **kwargs):
        if 'execution' in kwargs:
            execution: Execution = kwargs['execution']
            msg = f'[Execution: {execution.source.source_name} -> {execution.destination.destination_name}] {msg}'
        return msg

def getLogger(name: Optional[str] = None):
    return get_logger(name)
    
def get_logger(name: Optional[str] = None):
    return _LogWrapper(name)

_error_list: List[logging.LogRecord] = []

def _add_error(name: str, msg: str, stacklevel: int, level: int, args):
    fn, lno, func, sinfo = _get_stack_trace(stacklevel)    
    _error_list.append(logging.LogRecord(name, level, fn, lno, msg, args, None, func, sinfo))

def _get_stack_trace(stacklevel: int, stack_info: bool = True):
    # from python logging module
    f: Optional[FrameType] = sys._getframe(3)
    if f is not None:
        f = f.f_back
    orig_f = f
    while f and stacklevel > 1:
        f = f.f_back
        stacklevel -= 1
    if not f:
        f = orig_f
    rv: Tuple[str, int, str, Optional[str]]= ("(unknown file)", 0, "(unknown function)", None)
    if f is not None and hasattr(f, "f_code"):
        co = f.f_code
        sinfo = None
        if stack_info:
            sio = io.StringIO()
            sio.write('Stack (most recent call last):\n')
            traceback.print_stack(f, file=sio)
            sinfo = sio.getvalue()
            if sinfo[-1] == '\n':
                sinfo = sinfo[:-1]
            sio.close()
        rv = (co.co_filename, f.f_lineno, co.co_name, sinfo)
    return rv

def has_errors() -> bool:
    return len(_error_list) > 0

def error_list() -> List[logging.LogRecord]:
    return _error_list

def get_formatted_error_list() -> Optional[str]:
    records = _error_list
    if records is not None and len(records) > 0:
        message = ''
        for i in range(len(records)):
            rec = records[i]
            message += f'{i+1}. {rec.msg}\n... in {rec.pathname}:{rec.lineno}\n'
        return message
    else:
        return None

def null_filter(el: Any) -> Any:
    get_logger('megalista.LOG').info(f'Logging: {el}')
    return el