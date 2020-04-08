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

from enum import Enum
from typing import Iterable

OK_STATUS = 'OK'


class Action(Enum):
  CM_OFFLINE_CONVERSION, \
  ADS_OFFLINE_CONVERSION, \
  ADS_SSD_UPLOAD, \
  ADS_USER_LIST_UPLOAD, \
  ADS_USER_LIST_REMOVE, \
  GA_USER_LIST_UPLOAD = range(6)


class SourceType(Enum):
  BIG_QUERY, \
  CSV = range(2)
  # TODO: CSV not yet implemented


class Execution:

  def __init__(
      self,
      source_name,  # type: str,
      source_type,  # type: SourceType
      source_metadata,  # type: str,
      destination_name,  # type str
      action,  # type: Action
      destination_metadata,  # type: Iterable[str]
  ):
    self._source_name = source_name
    self._source_type = source_type
    self._source_metadata = source_metadata
    self._destination_name = destination_name
    self._action = action
    self._destination_metadata = destination_metadata

  @property
  def source_name(self):
    return self._source_name

  @property
  def source_type(self):
    return self._source_type

  @property
  def source_metadata(self):
    return self._source_metadata

  @property
  def destination_name(self):
    return self._destination_name

  @property
  def action(self):
    return self._action

  @property
  def destination_metadata(self):
    return self._destination_metadata

  def __str__(self):
    return 'Origin name: {}. Action: {}. Destination name: {}'.format(self.source_name, self.action,
                                                                      self.destination_name)

  def __eq__(self, other):
    return self.source_name == other.source_name \
           and self.source_type == other.source_type \
           and self.source_metadata == other.source_metadata \
           and self.destination_name == other.destination_name \
           and self.action == other.action \
           and self.destination_metadata == other.destination_metadata

  def __hash__(self):
    return hash((self.source_name, self.source_type, self.source_metadata, self.destination_name, self.action,
                 self.destination_metadata))
