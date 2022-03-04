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
from typing import Any, List, Iterable, Tuple, Dict
from datetime import datetime

import apache_beam as beam
import logging
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from apache_beam.io.gcp.bigquery import ReadFromBigQueryRequest

from models.execution import Execution

from data_sources.base_data_source import BaseDataSource

from models.execution import TransactionalType

_BIGQUERY_PAGE_SIZE = 20000

_LOGGER_NAME = 'megalista.data_sources.BigQuery'

class BigQueryDataSource(BaseDataSource):
    def __init__(self, transatcional_type: TransactionalType, bq_ops_dataset: str):
        self._transatcional_type = transatcional_type
        self._bq_ops_dataset = bq_ops_dataset
    
    def retrieve_data(self, execution: Execution) -> Iterable[Tuple[Execution, Dict[str, Any]]]:
        if self._transatcional_type == TransactionalType.NOT_TRANSACTIONAL:
            return self._retrieve_data_non_transactional(execution)
        else:
            return self._retrieve_data_transactional(execution)
    
    def _retrieve_data_non_transactional(self, execution: Execution) -> Iterable[Tuple[Execution, Dict[str, Any]]]:
        client = bigquery.Client()
        table_name = self._get_table_name(execution.source.source_metadata, False)
        query = f"SELECT data.* FROM {table_name} AS data"
        logging.getLogger(_LOGGER_NAME).info(f'Reading from table {table_name} for Execution {execution}')
        rows_iterator = client.query(query).result(page_size=_BIGQUERY_PAGE_SIZE)
        for row in rows_iterator:
            yield execution, _convert_row_to_dict(row)
    
    def _retrieve_data_transactional(self, execution: Execution) -> Iterable[Tuple[Execution, Dict[str, Any]]]:
        table_name = self._get_table_name(execution.source.source_metadata, False)
        uploaded_table_name = self._get_table_name(execution.source.source_metadata, True)
        client = bigquery.Client()

        query = None
        if self._transactional_type == TransactionalType.UUID:
            query = f"CREATE TABLE IF NOT EXISTS `{uploaded_table_name}` ( \
                timestamp TIMESTAMP OPTIONS(description= 'Event timestamp'), \
                uuid STRING OPTIONS(description='Event unique identifier')) \
                PARTITION BY _PARTITIONDATE \
                OPTIONS(partition_expiration_days=15)"
        elif self._transactional_type == TransactionalType.GCLID_TIME:
            query = f"CREATE TABLE IF NOT EXISTS `{uploaded_table_name}` ( \
                timestamp TIMESTAMP OPTIONS(description= 'Event timestamp'), \
                gclid STRING OPTIONS(description= 'Original gclid'), \
                time STRING OPTIONS(description= 'Original time')) \
                PARTITION BY _PARTITIONDATE \
                OPTIONS(partition_expiration_days=15)"
        logging.getLogger(_LOGGER_NAME).info(
            f"Creating table `{uploaded_table_name}` if it doesn't exist")

        client.query(query).result()

        query = None
        if self._transactional_type == TransactionalType.UUID:
            query = f"SELECT data.* FROM `{table_name}` AS data \
                LEFT JOIN `{uploaded_table_name}` AS uploaded USING(uuid) \
                WHERE uploaded.uuid IS NULL;"
        elif self._transactional_type == TransactionalType.GCLID_TIME:
            query =  f"SELECT data.* FROM `{table_name}` AS data \
                LEFT JOIN `{uploaded_table_name}` AS uploaded USING(gclid, time) \
                WHERE uploaded.gclid IS NULL;"

        logging.getLogger(_LOGGER_NAME).info(
            f'Reading from table `{table_name}` for Execution {execution}')
        rows_iterator = client.query(query).result(page_size=_BIGQUERY_PAGE_SIZE)
        for row in rows_iterator:
            yield execution, _convert_row_to_dict(row)
  
    def write_transactional_info(self, rows, execution: Execution):
        table_name = self._get_table_name(execution.source.source_metadata, True)

        client = bigquery.Client()
        table = client.get_table(table_name)
        now = datetime.now().timestamp()
        results = client.insert_rows(table,
            self._get_bq_rows(rows, now),
            self._get_schema_fields())

        for result in results:
          logging.getLogger(_LOGGER_NAME).error(result['errors'])
    
    def _get_table_name(self, source_metadata: list, uploaded: bool):
        dataset = None
        if self._is_transactional and uploaded:
            dataset = self._bq_ops_dataset
        else:
            dataset = source_metadata[0]
        table_name = dataset + '.' + source_metadata[1]
        if uploaded:
            table_name = f"{table_name}_uploaded"
        table_name = table_name.replace('`', '')
        table_name = f"{table_name}"
        return table_name
    
    def _get_schema_fields(self):
        if self._transactional_type == TransactionalType.UUID:
            return SchemaField("uuid", "string"), SchemaField("timestamp", "timestamp")
        if self._transactional_type == TransactionalType.GCLID_TIME:
            return SchemaField("gclid", "string"), SchemaField("time", "string"), SchemaField("timestamp", "timestamp")
        raise Exception(f'Unrecognized TransactionalType: {self._transactional_type}')

    def _get_bq_rows(self, rows, now):
        if self._transactional_type == TransactionalType.UUID:
            return [{'uuid': row['uuid'], 'timestamp': now} for row in rows]
        if self._transactional_type == TransactionalType.GCLID_TIME:
            return [{'gclid': row['gclid'], 'time': row['time'], 'timestamp': now} for row in rows]
        raise Exception(f'Unrecognized TransactionalType: {self._transactional_type}')


def _convert_row_to_dict(row):
    dict = {}
    for key, value in row.items():
        dict[key] = value
    return dict
 