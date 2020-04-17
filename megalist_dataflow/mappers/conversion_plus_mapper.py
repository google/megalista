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


class ConversionPlusMapper():
  def __init__(self, sheets_config, sheet_id, sheet_range):
    self.sheets_config = sheets_config
    self.sheet_id = sheet_id
    self.sheet_range = sheet_range
    self.config = None

  def _check_cd(self, key, rule, conversion):
    # TODO: não entendi isso aqui
    # acho que isso é o cap.
    custom_dimension = 'cd' + key
    op = rule['op']
    if custom_dimension in conversion:
      if op == 'EQUALS':
        return conversion[custom_dimension] == rule['value']
      if op == 'GREATER_EQUAL':
        return float(conversion[custom_dimension]) >= float(rule['value'])
      if op == 'LESS_EQUAL':
        return float(conversion[custom_dimension]) <= float(rule['value'])
    return False

  def _boost_conversion(self, conversion):
    for key in self.config:
      rule = self.config[key]
      if self._check_cd(key, rule, conversion):
        conversion['amount'] = round(float(conversion['amount']) * float(rule['multiplier']))
    return conversion

  def boost_conversions(self, conversions):
    if self.config is None:
      self.config = self.sheets_config.get_config(self.sheet_id.get(), self.sheet_range.get())
    boosted_conversions = [{'execution': conversion['execution'], 'row': self._boost_conversion(conversion['row'])}
                           for conversion in conversions]
    return boosted_conversions
