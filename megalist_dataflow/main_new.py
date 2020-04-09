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

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from mappers.pii_hashing_mapper import PIIHashingMapper
from sources.bq_api_dofn import BigQueryApiDoFn
from sources.spreadsheet_execution_source import SpreadsheetExecutionSource
from uploaders.google_ads_user_list_remover import GoogleAdsUserListRemoverDoFn

from uploaders.google_ads_user_list_uploader import GoogleAdsUserListUploaderDoFn
from utils.execution import Action
from utils.group_by_execution_dofn import GroupByExecutionDoFn
from utils.oauth_credentials import OAuthCredentials
from utils.options import DataflowOptions
from utils.sheets_config import SheetsConfig


def filter_by_action(execution, action):
  return execution.action is action


def run(argv=None):
  pipeline_options = PipelineOptions()
  dataflow_options = pipeline_options.view_as(DataflowOptions)
  oauth_credentials = OAuthCredentials(dataflow_options.client_id, dataflow_options.client_secret,
                                       dataflow_options.developer_token, dataflow_options.refresh_token)

  sheets_config = SheetsConfig(oauth_credentials)
  hasher = PIIHashingMapper()

  with beam.Pipeline(options=pipeline_options) as pipeline:
    executions = (pipeline | 'Load executions' >> beam.io.Read(
      SpreadsheetExecutionSource(sheets_config, dataflow_options.setup_sheet_id)))

    _add_google_ads_user_list_upload(executions, hasher, oauth_credentials, dataflow_options)
    _add_google_ads_user_list_removal(executions, hasher, oauth_credentials, dataflow_options)

    # todo: update trix at the end


def _add_google_ads_user_list_upload(pipeline, hasher, oauth_credentials, dataflow_options):
  (
      pipeline | 'Filter - Google Ads user list add' >> beam.Filter(filter_by_action, Action.ADS_USER_LIST_UPLOAD)
      | 'Read users table - Google Ads user list add' >> beam.ParDo(BigQueryApiDoFn())
      | 'Group elements - Google Ads user list add' >> beam.ParDo(GroupByExecutionDoFn())
      | 'Hash Users - Google Ads user list add' >> beam.Map(hasher.hash_users_with_execution)
      | 'Upload - Google Ads user list add' >> beam.ParDo(
    GoogleAdsUserListUploaderDoFn(oauth_credentials, dataflow_options.developer_token,
                                  dataflow_options.customer_id, dataflow_options.app_id))
  )


def _add_google_ads_user_list_removal(pipeline, hasher, oauth_credentials, dataflow_options):
  (
      pipeline | 'Filter - Google Ads user list remove' >> beam.Filter(filter_by_action, Action.ADS_USER_LIST_REMOVE)
      | 'Read users table - Google Ads user list remove' >> beam.ParDo(BigQueryApiDoFn())
      | 'Group elements - Google Ads user list remove' >> beam.ParDo(GroupByExecutionDoFn())
      | 'Hash Users - Google Ads user list remove' >> beam.Map(hasher.hash_users_with_execution)
      | 'Upload - Google Ads user list remove' >> beam.ParDo(
    GoogleAdsUserListRemoverDoFn(oauth_credentials, dataflow_options.developer_token,
                                 dataflow_options.customer_id, dataflow_options.app_id))
  )


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
