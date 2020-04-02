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

from google.cloud import bigquery

from sources.base_bounded_source import BaseBoundedSource


class BigQueryApiSource(BaseBoundedSource):
  """
  Source implemented using BigQuery's API.

  This source always reads all rows in a table.
  Each returned row can be accessed by column index (row[0]) or column name (row[column])
  """

  def __init__(self,
      dataset_name,  # type: str
      table_name,  # type: str
      query_batch_size=20000,  # type: int
  ):
    self._table_name = dataset_name + '.' + table_name
    self._query_batch_size = query_batch_size
    super().__init__()

  def _do_count(self):
    client = bigquery.Client()
    query_job = client.query('select count(*) from ' + self._table_name)
    for row in query_job:
      return row[0]

  def read(self, range_tracker):
    client = bigquery.Client()

    start_position = range_tracker.start_position()
    stop_position = range_tracker.stop_position()

    rows_iterator = client.list_rows(
        self._table_name,
        start_index=start_position,
        max_results=stop_position - start_position,
        page_size=self._query_batch_size)
    for row in rows_iterator:
      yield row
