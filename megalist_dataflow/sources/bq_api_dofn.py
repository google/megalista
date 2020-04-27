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


from apache_beam import DoFn

from google.cloud import bigquery

import logging

from utils.execution import SourceType


class BigQueryApiDoFn(DoFn):
  """
  DoFn with Execution as input and lines read from BigQuery as output.
  """

  def __init__(
      self,
      query_batch_size=20000  # type: int
  ):
    super().__init__()
    self._query_batch_size = query_batch_size

  def start_bundle(self):
    pass

  def process(self, execution, *args, **kwargs):
    if execution.source.source_type is not SourceType.BIG_QUERY:
      raise NotImplementedError

    client = bigquery.Client()

    table_name = execution.source.source_metadata[0] + '.' + execution.source.source_metadata[1]

    logging.getLogger().info('Reading from table %s for Execution (%s)', table_name, str(execution))

    rows_iterator = client.list_rows(table_name, page_size=self._query_batch_size)
    for row in rows_iterator:
      yield {'execution': execution, 'row': self._convert_row_to_dict(row)}

  @staticmethod
  def _convert_row_to_dict(row):
    dict = {}
    for key, value in row.items():
      dict[key] = value
    return dict
