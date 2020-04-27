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

from mappers.ads_ssd_hashing_mapper import AdsSSDHashingMapper
from mappers.conversion_plus_mapper import ConversionPlusMapper
from mappers.ads_user_list_pii_hashing_mapper import AdsUserListPIIHashingMapper
from sources.filter_load_and_group_data import FilterLoadAndGroupData
from sources.spreadsheet_execution_source import SpreadsheetExecutionSource
from uploaders.campaign_manager_conversion_uploader import CampaignManagerConversionUploaderDoFn
from uploaders.google_ads_offline_conversions_uploader import GoogleAdsOfflineUploaderDoFn
from uploaders.google_ads_ssd_uploader import GoogleAdsSSDUploaderDoFn

from uploaders.google_ads_customer_match.contact_info_uploader import GoogleAdsCustomerMatchContactInfoUploaderDoFn
from uploaders.google_ads_customer_match.mobile_uploader import GoogleAdsCustomerMatchMobileUploaderDoFn
from uploaders.google_ads_customer_match.user_id_uploader import GoogleAdsCustomerMatchUserIdUploaderDoFn
from uploaders.google_analytics_user_list_uploader import GoogleAnalyticsUserListUploaderDoFn

from utils.execution import DestinationType, Execution
from utils.oauth_credentials import OAuthCredentials
from utils.options import DataflowOptions
from utils.sheets_config import SheetsConfig


# TODO: Do not fail the whole pipeline if a branch fail


def filter_by_action(execution: Execution, destination_type: DestinationType):
  return execution.destination.destination_type is destination_type


def run(argv=None):
  pipeline_options = PipelineOptions()
  dataflow_options = pipeline_options.view_as(DataflowOptions)
  oauth_credentials = OAuthCredentials(dataflow_options.client_id, dataflow_options.client_secret,
                                       dataflow_options.developer_token, dataflow_options.refresh_token)

  sheets_config = SheetsConfig(oauth_credentials)
  # conversion_plus_mapper = ConversionPlusMapper(
  #   sheets_config, dataflow_options.cp_sheet_id, dataflow_options.cp_sheet_range)
  user_list_hasher = AdsUserListPIIHashingMapper()

  with beam.Pipeline(options=pipeline_options) as pipeline:
    executions = (pipeline | 'Load executions' >> beam.io.Read(
      SpreadsheetExecutionSource(sheets_config, dataflow_options.setup_sheet_id)))

    _add_google_ads_user_list_upload(executions, user_list_hasher, oauth_credentials, dataflow_options)
    _add_google_ads_offline_conversion(executions, None, oauth_credentials, dataflow_options)
    _add_google_ads_ssd(executions, AdsSSDHashingMapper(), oauth_credentials, dataflow_options)
    _add_ga_user_list(executions, oauth_credentials, dataflow_options)
    _add_cm_conversion(executions, oauth_credentials, dataflow_options)

    # todo: update trix at the end


def _add_google_ads_user_list_upload(pipeline, hasher, oauth_credentials, dataflow_options):
  (
      pipeline
      | 'Load Data -  Google Ads user list add' >> FilterLoadAndGroupData([
    DestinationType.ADS_CUSTOMER_MATCH_MOBILE_DEVICE_ID_UPLOAD,
    DestinationType.ADS_CUSTOMER_MATCH_CONTACT_INFO_UPLOAD,
    DestinationType.ADS_CUSTOMER_MATCH_USER_ID_UPLOAD])
      | 'Hash Users - Google Ads user list add' >> beam.Map(hasher.hash_users)
      | 'Upload - Google Ads Contact Info user list add' >> beam.ParDo(
    GoogleAdsCustomerMatchContactInfoUploaderDoFn(oauth_credentials,
                                                  dataflow_options.developer_token))
      | 'Upload - Google Ads Mobile Device Id list add' >> beam.ParDo(
    GoogleAdsCustomerMatchMobileUploaderDoFn(oauth_credentials,
                                             dataflow_options.developer_token))
      | 'Upload - Google Ads UserId list add' >> beam.ParDo(GoogleAdsCustomerMatchUserIdUploaderDoFn(oauth_credentials,
                                                                                                     dataflow_options.developer_token))
  )


def _add_google_ads_offline_conversion(pipeline, conversion_plus_mapper, oauth_credentials, dataflow_options):
  (
      pipeline
      | 'Load Data -  Google Ads user list conversion' >> FilterLoadAndGroupData([DestinationType.ADS_OFFLINE_CONVERSION])
      # | 'Boost Conversions' >> beam.Map(conversion_plus_mapper.boost_conversions)
      | 'Upload - Google Ads offline conversion' >> beam.ParDo(GoogleAdsOfflineUploaderDoFn(oauth_credentials,
                                                                                            dataflow_options.developer_token))
  )


def _add_google_ads_ssd(pipeline, hasher, oauth_credentials, dataflow_options):
  (
      pipeline
      | 'Load Data -  Google Ads SSD conversion' >> FilterLoadAndGroupData([DestinationType.ADS_SSD_UPLOAD], 50)
      | 'Hash Users - Google Ads SSD remove' >> beam.Map(hasher.map_conversions)
      | 'Upload - Google Ads SSD' >> beam.ParDo(GoogleAdsSSDUploaderDoFn(oauth_credentials,
                                                                         dataflow_options.developer_token))
  )


def _add_ga_user_list(pipeline, oauth_credentials, dataflow_options):
  (
      pipeline
      | 'Load Data -  GA user list' >> FilterLoadAndGroupData([DestinationType.GA_USER_LIST_UPLOAD])
      | 'Upload - GA user list' >> beam.ParDo(GoogleAnalyticsUserListUploaderDoFn(oauth_credentials))
  )


def _add_cm_conversion(pipeline, oauth_credentials, dataflow_options):
  (
      pipeline
      | 'Load Data -  CM conversion' >> FilterLoadAndGroupData([DestinationType.CM_OFFLINE_CONVERSION])
      | 'Upload - CM conversion' >> beam.ParDo(CampaignManagerConversionUploaderDoFn(oauth_credentials))
  )


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
