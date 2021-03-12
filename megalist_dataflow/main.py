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

import logging
import warnings

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from mappers.ads_ssd_hashing_mapper import AdsSSDHashingMapper
from mappers.ads_user_list_pii_hashing_mapper import AdsUserListPIIHashingMapper
from sources.spreadsheet_execution_source import SpreadsheetExecutionSource
from sources.batches_from_executions import BatchesFromExecutions
from uploaders.appsflyer.appsflyer_s2s_uploader_async import AppsFlyerS2SUploaderDoFn
from uploaders.campaign_manager.campaign_manager_conversion_uploader import CampaignManagerConversionUploaderDoFn
from uploaders.google_ads.customer_match.contact_info_uploader import GoogleAdsCustomerMatchContactInfoUploaderDoFn
from uploaders.google_ads.customer_match.mobile_uploader import GoogleAdsCustomerMatchMobileUploaderDoFn
from uploaders.google_ads.customer_match.user_id_uploader import GoogleAdsCustomerMatchUserIdUploaderDoFn
from uploaders.google_ads.conversions.google_ads_offline_conversions_uploader import GoogleAdsOfflineUploaderDoFn
from uploaders.google_ads.conversions.google_ads_ssd_uploader import GoogleAdsSSDUploaderDoFn
from uploaders.google_analytics.google_analytics_data_import_uploader import GoogleAnalyticsDataImportUploaderDoFn
from uploaders.google_analytics.google_analytics_measurement_protocol import GoogleAnalyticsMeasurementProtocolUploaderDoFn
from uploaders.google_analytics.google_analytics_user_list_uploader import GoogleAnalyticsUserListUploaderDoFn
from uploaders.google_analytics.google_analytics_4_measurement_protocol import GoogleAnalytics4MeasurementProtocolUploaderDoFn
from uploaders.google_analytics.google_analytics_data_import_eraser import GoogleAnalyticsDataImportEraser
from uploaders.big_query.transactional_events_results_writer import TransactionalEventsResultsWriter
from models.execution import DestinationType
from models.execution import Execution
from models.oauth_credentials import OAuthCredentials
from models.options import DataflowOptions
from models.sheets_config import SheetsConfig

warnings.filterwarnings(
    "ignore", "Your application has authenticated using end user credentials")


def filter_by_action(execution: Execution, destination_type: DestinationType):
    return execution.destination.destination_type is destination_type


class MegalistaStep(beam.PTransform):
    def __init__(self, oauth_credentials, dataflow_options=None, hasher=None):
        self._oauth_credentials = oauth_credentials
        self._dataflow_options = dataflow_options
        self._hasher = hasher

    def expand(self, executions):
        pass

class GoogleAdsSSDStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | 'Load Data -  Google Ads SSD' >> BatchesFromExecutions(DestinationType.ADS_SSD_UPLOAD, 50)
            | 'Hash Users - Google Ads SSD' >> beam.Map(self._hasher.map_batch)
            | 'Upload - Google Ads SSD' >> beam.ParDo(GoogleAdsSSDUploaderDoFn(self._oauth_credentials,
                                                                               self._dataflow_options.developer_token))
        )


class GoogleAdsCustomerMatchMobileDeviceIdStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | 'Load Data - Google Ads Customer Match Mobile Device Id' >> BatchesFromExecutions(DestinationType.ADS_CUSTOMER_MATCH_MOBILE_DEVICE_ID_UPLOAD)
            | 'Hash Users - Google Ads Customer Match Contact Info' >> beam.Map(self._hasher.hash_users)
            | 'Upload - Google Ads Customer Match Mobile Device Id' >> beam.ParDo(
                GoogleAdsCustomerMatchMobileUploaderDoFn(self._oauth_credentials, self._dataflow_options.developer_token))
        )


class GoogleAdsCustomerMatchContactInfoStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | 'Load Data - Google Ads Customer Match Contact Info' >> BatchesFromExecutions(DestinationType.ADS_CUSTOMER_MATCH_CONTACT_INFO_UPLOAD)
            | 'Hash Users - Google Ads Customer Match Contact Info' >> beam.Map(self._hasher.hash_users)
            | 'Upload - Google Ads Customer Match Contact Info' >> beam.ParDo(
                GoogleAdsCustomerMatchContactInfoUploaderDoFn(self._oauth_credentials, self._dataflow_options.developer_token))
        )


class GoogleAdsCustomerMatchUserIdStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | 'Load Data - Google Ads Customer Match User Id' >> BatchesFromExecutions(DestinationType.ADS_CUSTOMER_MATCH_USER_ID_UPLOAD)
            | 'Hash Users - Google Ads Customer Match Contact Info' >> beam.Map(self._hasher.hash_users)
            | 'Upload - Google Ads Customer User Device Id' >> beam.ParDo(
                GoogleAdsCustomerMatchUserIdUploaderDoFn(self._oauth_credentials, self._dataflow_options.developer_token))
        )


class GoogleAdsOfflineConversionsStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | 'Load Data - GoogleAdsOfflineConversions' >> BatchesFromExecutions(DestinationType.ADS_OFFLINE_CONVERSION)
            | 'Upload - GoogleAdsOfflineConversions' >> beam.ParDo(GoogleAdsOfflineUploaderDoFn(self._oauth_credentials,
                                                                                                self._dataflow_options.developer_token))
        )


class GoogleAnalyticsUserListStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | 'Load Data -  GA user list' >> BatchesFromExecutions(DestinationType.GA_USER_LIST_UPLOAD, 5000000)
            | 'Upload - GA user list' >> beam.ParDo(GoogleAnalyticsUserListUploaderDoFn(self._oauth_credentials))
        )


class GoogleAnalyticsDataImportStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | 'Load Data -  GA data import' >> BatchesFromExecutions(DestinationType.GA_DATA_IMPORT, 1000000)
            | 'Delete Data -  GA data import' >> beam.ParDo(GoogleAnalyticsDataImportEraser(self._oauth_credentials))
            | 'Upload - GA data import' >> beam.ParDo(GoogleAnalyticsDataImportUploaderDoFn(self._oauth_credentials))
        )


class GoogleAnalyticsMeasurementProtocolStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | 'Load Data - GA measurement protocol' >>
              BatchesFromExecutions(DestinationType.GA_MEASUREMENT_PROTOCOL, 20, transactional=True)
            | 'Upload - GA measurement protocol' >>
              beam.ParDo(GoogleAnalyticsMeasurementProtocolUploaderDoFn())
            | 'Persist results - GA measurement protocol' >> beam.ParDo(TransactionalEventsResultsWriter(self._dataflow_options.bq_ops_dataset))
        )


class GoogleAnalytics4MeasurementProtocolStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | 'Load Data - GA 4 measurement protocol' >> BatchesFromExecutions(DestinationType.GA_4_MEASUREMENT_PROTOCOL, 20, 
              transactional=True)
            | 'Upload - GA 4 measurement protocol' >>
              beam.ParDo(GoogleAnalytics4MeasurementProtocolUploaderDoFn())
            | 'Persist results - GA 4 measurement protocol' >> beam.ParDo(TransactionalEventsResultsWriter(self._dataflow_options.bq_ops_dataset))
        )

class CampaignManagerConversionStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | 'Load Data -  CM conversion' >> BatchesFromExecutions(DestinationType.CM_OFFLINE_CONVERSION, 1000, transactional=True)
            | 'Upload - CM conversion' >> beam.ParDo(CampaignManagerConversionUploaderDoFn(self._oauth_credentials))
            | 'Persist results - CM conversion' >> beam.ParDo(
              TransactionalEventsResultsWriter(self._dataflow_options.bq_ops_dataset))
        )

class AppsFlyerEventsStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | 'Load Data - AppsFlyer S2S events' >>
            BatchesFromExecutions(DestinationType.APPSFLYER_S2S_EVENTS, 1000, transactional=True)
            | 'Upload - AppsFlyer S2S events' >>
            beam.ParDo(AppsFlyerS2SUploaderDoFn(self._dataflow_options.appsflyer_dev_key))
            | 'Persist results - AppsFlyer S2S events' >> beam.ParDo(TransactionalEventsResultsWriter(self._dataflow_options.bq_ops_dataset))
        )


def run(argv=None):
    pipeline_options = PipelineOptions()
    dataflow_options = pipeline_options.view_as(DataflowOptions)
    oauth_credentials = OAuthCredentials(
        dataflow_options.client_id,
        dataflow_options.client_secret,
        dataflow_options.access_token,
        dataflow_options.refresh_token)

    sheets_config = SheetsConfig(oauth_credentials)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        executions = (pipeline | 'Load executions' >> beam.io.Read(
            SpreadsheetExecutionSource(sheets_config, dataflow_options.setup_sheet_id)))

        executions | GoogleAdsSSDStep(
            oauth_credentials, dataflow_options, AdsSSDHashingMapper())
        executions | GoogleAdsCustomerMatchMobileDeviceIdStep(
            oauth_credentials, dataflow_options, AdsUserListPIIHashingMapper())
        executions | GoogleAdsCustomerMatchContactInfoStep(
            oauth_credentials, dataflow_options, AdsUserListPIIHashingMapper())
        executions | GoogleAdsCustomerMatchUserIdStep(
            oauth_credentials, dataflow_options, AdsUserListPIIHashingMapper())
        executions | GoogleAdsOfflineConversionsStep(
            oauth_credentials, dataflow_options)
        executions | GoogleAnalyticsUserListStep(oauth_credentials)
        executions | GoogleAnalyticsDataImportStep(oauth_credentials)
        executions | GoogleAnalyticsMeasurementProtocolStep(
            oauth_credentials, dataflow_options)
        executions | GoogleAnalytics4MeasurementProtocolStep(
            oauth_credentials, dataflow_options)
        executions | CampaignManagerConversionStep(oauth_credentials, dataflow_options)
        executions | AppsFlyerEventsStep(oauth_credentials, dataflow_options)

        # todo: update trix at the end


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.ERROR)
    logging.getLogger("megalista").setLevel(logging.INFO)
    run()
    logging.getLogger("megalista").info("Completed successfully!")
