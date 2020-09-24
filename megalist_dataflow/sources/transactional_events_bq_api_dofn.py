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


import logging

from apache_beam import DoFn
from google.cloud import bigquery

from megalist_dataflow.utils.execution import SourceType


class TransactionalEventsBigQueryApiDoFn(DoFn):
  """
  DoFn with Execution as input and lines read from BigQuery as output.
  This implementation is specific to measurement protocol as it joins source table with uploaded table in order to
  filter result that were already uploaded to GA.
  """

  def __init__(
      self,
      bq_ops_dataset,
      query_batch_size=20000  # type: int
  ):
    super().__init__()
    self._query_batch_size = query_batch_size
    self._first_element_processed = False
    self._bq_ops_dataset = str(bq_ops_dataset)

  def create_uploaded_table(self, uploaded_table_name):
    if self._first_element_processed:
      return

    client = bigquery.Client()
    
    query = "CREATE TABLE IF NOT EXISTS " + uploaded_table_name + " ( \
             timestamp TIMESTAMP OPTIONS(description= 'Event timestamp'), \
             uuid STRING OPTIONS(description='Event unique identifier'))\
             PARTITION BY _PARTITIONDATE \
             OPTIONS(partition_expiration_days=7)"  
    client.query(query) 

  def start_bundle(self):
    pass

  def process(self, execution, *args, **kwargs):
    if execution.source.source_type is not SourceType.BIG_QUERY:
      raise NotImplementedError

    #initialize destination table for uploaded events
    uploaded_table_name = self._bq_ops_dataset + '.' + execution.source.source_metadata[1] +'_uploaded'
    self.create_uploaded_table(uploaded_table_name)

    client = bigquery.Client()

    table_name = execution.source.source_metadata[0] + '.' + execution.source.source_metadata[1]
    query = "select data.* from " + table_name + " data \
             left join " + uploaded_table_name + " uploaded on data.uuid = uploaded.uuid \
             where uploaded.uuid is null;"

    logging.getLogger("megalista.TransactionalEventsBigQueryApiDoFn").info(
      'Reading from table %s for Execution (%s)', table_name, str(execution))
    rows_iterator = client.query(query).result(page_size=self._query_batch_size)
    for row in rows_iterator:
      yield {'execution': execution, 'row': self._convert_row_to_dict(row)}

  @staticmethod
  def _convert_row_to_dict(row):
    dict = {}
    for key, value in row.items():
      dict[key] = value
    return dict
