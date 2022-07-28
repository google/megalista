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
from string import Template

import apache_beam as beam
import logging
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField, Client
from apache_beam.io.gcp.bigquery import ReadFromBigQueryRequest

from models.execution import Execution, SourceType, DestinationType

from data_sources.base_data_source import BaseDataSource
import data_sources.data_schemas as DataSchemas

from models.execution import TransactionalType

_BIGQUERY_PAGE_SIZE = 20000

_LOGGER_NAME = 'megalista.data_sources.BigQuery'

class BigQueryDataSource(BaseDataSource):
    def __init__(self, transactional_type: TransactionalType, bq_ops_dataset: str, bq_location: str, source_type: SourceType, source_name: str, destination_type: DestinationType, destination_name: str):
        self._transactional_type = transactional_type
        self._bq_ops_dataset = bq_ops_dataset
        self._bq_location = bq_location
        
        self._source_type = source_type
        self._source_name = source_name
        self._destination_type = destination_type
        self._destination_name = destination_name
  
        if transactional_type is not TransactionalType.NOT_TRANSACTIONAL:
            if not bq_ops_dataset:
                raise Exception(f'Missing bq_ops_dataset for this uploader. Source="{self._source_name}". Destination="{self._destination_name}"')

    
    def retrieve_data(self, execution: Execution) -> Iterable[Tuple[Execution, Dict[str, Any]]]:
        if self._transactional_type == TransactionalType.NOT_TRANSACTIONAL:
            return self._retrieve_data_non_transactional(execution)
        else:
            return self._retrieve_data_transactional(execution)
    
    def _retrieve_data_non_transactional(self, execution: Execution) -> Iterable[Tuple[Execution, Dict[str, Any]]]:
        client = self._get_bq_client()

        table_name = self._get_table_name(execution.source.source_metadata, False)
        cols = self._get_table_columns(client, table_name)
        if DataSchemas.validate_data_columns(cols, self._destination_type):
            cols = DataSchemas.get_cols_names(cols, self._destination_type)
            query = f"SELECT {','.join(cols)} FROM `{table_name}` AS data"
            logging.getLogger(_LOGGER_NAME).info(f'Reading from table {table_name} for Execution {execution}')
            for row in client.query(query).result(page_size=_BIGQUERY_PAGE_SIZE):
                yield execution, _convert_row_to_dict(row)
        else:
            raise ValueError(f'Data source incomplete. {DataSchemas.get_error_message(cols, self._destination_type)} Source="{self._source_name}". Destination="{self._destination_name}"')
    
    def _retrieve_data_transactional(self, execution: Execution) -> Iterable[Tuple[Execution, Dict[str, Any]]]:
        table_name = self._get_table_name(execution.source.source_metadata, False)
        uploaded_table_name = self._get_table_name(execution.source.source_metadata, True)
        client = self._get_bq_client()

        self._ensure_control_table_exists(client, uploaded_table_name)
        
        cols = self._get_table_columns(client, table_name)
        logging.getLogger(_LOGGER_NAME).info(f'Destination Type: {self._destination_type}')
        if DataSchemas.validate_data_columns(cols, self._destination_type):
            cols = DataSchemas.get_cols_names(cols, self._destination_type)
            query_cols = ','.join(['data.' + col for col in cols])
            template = None
            if self._transactional_type == TransactionalType.UUID:
                template = "SELECT $query_cols FROM `$table_name` AS data \
                                LEFT JOIN $uploaded_table_name AS uploaded USING(uuid) \
                                WHERE uploaded.uuid IS NULL;"
            elif self._transactional_type == TransactionalType.GCLID_TIME:
                template = "SELECT $query_cols FROM `$table_name` AS data \
                                LEFT JOIN $uploaded_table_name AS uploaded USING(gclid, time) \
                                WHERE uploaded.gclid IS NULL;"
            else:
                raise Exception(f'Unrecognized TransactionalType: {self._transactional_type}. Source="{self._source_name}". Destination="{self._destination_name}"')

            query = Template(template).substitute(table_name=table_name, uploaded_table_name=uploaded_table_name, query_cols=query_cols)
            logging.getLogger(_LOGGER_NAME).info(
                f'Reading from table `{table_name}` for Execution {execution}')
            for row in client.query(query).result(page_size=_BIGQUERY_PAGE_SIZE):
                yield execution, _convert_row_to_dict(row)
        else:
            raise ValueError(f'Data source incomplete. {DataSchemas.get_error_message(cols, self._destination_type)} Source="{self._source_name}". Destination="{self._destination_name}"')

  
    def _ensure_control_table_exists(self, client: Client, uploaded_table_name: str):
        template = None
        if self._transactional_type == TransactionalType.UUID:
            template = "CREATE TABLE IF NOT EXISTS `$uploaded_table_name` ( \
                            timestamp TIMESTAMP OPTIONS(description= 'Event timestamp'), \
                            uuid STRING OPTIONS(description='Event unique identifier')) \
                            PARTITION BY _PARTITIONDATE \
                            OPTIONS(partition_expiration_days=15)"
        elif self._transactional_type == TransactionalType.GCLID_TIME:
            template = "CREATE TABLE IF NOT EXISTS `$uploaded_table_name` ( \
                            timestamp TIMESTAMP OPTIONS(description= 'Event timestamp'), \
                            gclid STRING OPTIONS(description= 'Original gclid'), \
                            time STRING OPTIONS(description= 'Original time')) \
                            PARTITION BY _PARTITIONDATE \
                            OPTIONS(partition_expiration_days=15)"
        else:
            raise Exception(f'Unrecognized TransactionalType: {self._transactional_type}. Source="{self._source_name}". Destination="{self._destination_name}"')

        query = Template(template).substitute(uploaded_table_name=uploaded_table_name)

        logging.getLogger(_LOGGER_NAME).info(
            f"Creating table `{uploaded_table_name}` if it doesn't exist")

        client.query(query).result()

    def write_transactional_info(self, rows, execution: Execution):
        table_name = self._get_table_name(execution.source.source_metadata, True)

        client = self._get_bq_client()
        table = client.get_table(table_name)
        now = self._get_now()
        results = client.insert_rows(table,
            self._get_bq_rows(rows, now),
            self._get_schema_fields())

        for result in results:
            logging.getLogger(_LOGGER_NAME).error(result['errors'])
    
    def _get_now(self):
        return datetime.now().timestamp()
        
    def _get_table_name(self, source_metadata: list, uploaded: bool):
        dataset = None
        if self._transactional_type != TransactionalType.NOT_TRANSACTIONAL and uploaded:
            dataset = self._bq_ops_dataset
        else:
            dataset = source_metadata[0]
        table_name = dataset + '.' + source_metadata[1]
        if uploaded:
            table_name = f"{table_name}_uploaded"
        return table_name.replace('`', '')
    
    def _get_schema_fields(self):
        if self._transactional_type == TransactionalType.UUID:
            return SchemaField("uuid", "string"), SchemaField("timestamp", "timestamp")
        if self._transactional_type == TransactionalType.GCLID_TIME:
            return SchemaField("gclid", "string"), SchemaField("time", "string"), SchemaField("timestamp", "timestamp")
        raise Exception(f'Unrecognized TransactionalType: {self._transactional_type}. Source="{self._source_name}". Destination="{self._destination_name}"')

    def _get_bq_rows(self, rows, now):
        if self._transactional_type == TransactionalType.UUID:
            return [{'uuid': row['uuid'], 'timestamp': now} for row in rows]
        if self._transactional_type == TransactionalType.GCLID_TIME:
            return [{'gclid': row['gclid'], 'time': row['time'], 'timestamp': now} for row in rows]
        raise Exception(f'Unrecognized TransactionalType: {self._transactional_type}. Source="{self._source_name}". Destination="{self._destination_name}"')

    def _get_table_columns(self, client, table_name):
        table = client.get_table(table_name)
        return [schema.name for schema in table.schema]

    def _get_bq_client(self):
        return bigquery.Client(location=self._bq_location)
        
def _convert_row_to_dict(row):
    dict = {}
    for key, value in row.items():
        dict[key] = value
    return dict
 