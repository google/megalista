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


class OriginType(Enum):
  BIG_QUERY, \
  CSV = range(2)
  # TODO: CSV not yet implemented


class Execution:

  def __init__(self,
               origin_name,  # type: str,
               origin_type,  # type: OriginType
               origin_metadata,  # type: str,
               action,  # type: Action
               destination_name,  # type str
               destination_metadata,  # type: Iterable[str]
               ):
    self._origin_name = origin_name
    self._origin_type = origin_type
    self._origin_metadata = origin_metadata
    self._action = action
    self._destination_name = destination_name
    self._destination_metadata = destination_metadata

  @property
  def origin_name(self):
    return self._origin_name

  @property
  def origin_type(self):
    return self._origin_type

  @property
  def origin_metadata(self):
    return self._origin_metadata

  @property
  def action(self):
    return self._action

  @property
  def destination_name(self):
    return self._destination_name

  @property
  def destination_metadata(self):
    return self._destination_metadata

  def __str__(self):
    return 'Origin name: {}. Action: {}. Destination name: {}'.format(self.origin_name, self.action,
                                                                      self.destination_name)

  def __eq__(self, other):
    return self.origin_name == other.origin_name \
           and self.origin_type == other.origin_type \
           and self.origin_metadata == other.origin_metadata \
           and self.action == other.action \
           and self.destination_name == other.destination_name \
           and self.destination_metadata == other.destination_metadata

  def __hash__(self):
    return hash((self.origin_name, self.origin_type, self.origin_metadata, self.action, self.destination_name,
                 self.destination_metadata))
