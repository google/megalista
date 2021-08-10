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
from mappers.ads_user_list_pii_hashing_mapper import \
    AdsUserListPIIHashingMapper
from models.execution import DestinationType, Execution
from models.json_config import JsonConfig
from models.oauth_credentials import OAuthCredentials
from models.options import DataflowOptions
from models.sheets_config import SheetsConfig
from sources.batches_from_executions import BatchesFromExecutions
from sources.primary_execution_source import PrimaryExecutionSource
from uploaders.big_query.transactional_events_results_writer import TransactionalEventsResultsWriter
from uploaders.campaign_manager.campaign_manager_conversion_uploader import CampaignManagerConversionUploaderDoFn
from uploaders.google_ads.conversions.google_ads_offline_conversions_uploader import GoogleAdsOfflineUploaderDoFn
from uploaders.google_ads.conversions.google_ads_ssd_uploader import GoogleAdsSSDUploaderDoFn
from uploaders.google_ads.customer_match.contact_info_uploader import GoogleAdsCustomerMatchContactInfoUploaderDoFn
from uploaders.google_ads.customer_match.mobile_uploader import GoogleAdsCustomerMatchMobileUploaderDoFn
from uploaders.google_ads.customer_match.user_id_uploader import GoogleAdsCustomerMatchUserIdUploaderDoFn
from uploaders.google_analytics.google_analytics_4_measurement_protocol import GoogleAnalytics4MeasurementProtocolUploaderDoFn
from uploaders.google_analytics.google_analytics_data_import_eraser import GoogleAnalyticsDataImportEraser
from uploaders.google_analytics.google_analytics_data_import_uploader import GoogleAnalyticsDataImportUploaderDoFn
from uploaders.google_analytics.google_analytics_measurement_protocol import GoogleAnalyticsMeasurementProtocolUploaderDoFn
from uploaders.google_analytics.google_analytics_user_list_uploader import GoogleAnalyticsUserListUploaderDoFn
from third_party import THIRD_PARTY_STEPS

warnings.filterwarnings(
    "ignore", "Your application has authenticated using end user credentials"
)

ADS_CM_HASHER = AdsUserListPIIHashingMapper()

def filter_by_action(execution: Execution, destination_type: DestinationType):
    return execution.destination.destination_type is destination_type


class MegalistaStepParams():
    def __init__(self, oauth_credentials, dataflow_options):
        self._oauth_credentials = oauth_credentials
        self._dataflow_options = dataflow_options

    @property
    def oauth_credentials(self):
        return self._oauth_credentials

    @property
    def dataflow_options(self):
        return self._dataflow_options

class MegalistaStep(beam.PTransform):
    def __init__(self, params: MegalistaStepParams):
        self._params = params

    @property
    def params(self):
        return self._params

    def expand(self, executions):
        pass

class GoogleAdsSSDStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data -  Google Ads SSD"
            >> BatchesFromExecutions(DestinationType.ADS_SSD_UPLOAD, 5000)
            | "Hash Users - Google Ads SSD" >> beam.Map(AdsSSDHashingMapper().map_batch)
            | "Upload - Google Ads SSD"
            >> beam.ParDo(
                GoogleAdsSSDUploaderDoFn(
                    self.params._oauth_credentials,
                    self.params._dataflow_options.developer_token
                )
            )
        )


class GoogleAdsCustomerMatchMobileDeviceIdStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data - Google Ads Customer Match Mobile Device Id"
            >> BatchesFromExecutions(
                DestinationType.ADS_CUSTOMER_MATCH_MOBILE_DEVICE_ID_UPLOAD
            )
            | "Hash Users - Google Ads Customer Match Contact Info"
            >> beam.Map(ADS_CM_HASHER.hash_users)
            | "Upload - Google Ads Customer Match Mobile Device Id"
            >> beam.ParDo(
                GoogleAdsCustomerMatchMobileUploaderDoFn(
                    self.params._oauth_credentials,
                    self.params._dataflow_options.developer_token
                )
            )
        )


class GoogleAdsCustomerMatchContactInfoStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data - Google Ads Customer Match Contact Info"
            >> BatchesFromExecutions(
                DestinationType.ADS_CUSTOMER_MATCH_CONTACT_INFO_UPLOAD
            )
            | "Hash Users - Google Ads Customer Match Contact Info"
            >> beam.Map(ADS_CM_HASHER.hash_users)
            | "Upload - Google Ads Customer Match Contact Info"
            >> beam.ParDo(
                GoogleAdsCustomerMatchContactInfoUploaderDoFn(
                    self.params._oauth_credentials,
                    self.params._dataflow_options.developer_token
                )
            )
        )


class GoogleAdsCustomerMatchUserIdStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data - Google Ads Customer Match User Id"
            >> BatchesFromExecutions(DestinationType.ADS_CUSTOMER_MATCH_USER_ID_UPLOAD)
            | "Hash Users - Google Ads Customer Match Contact Info"
            >> beam.Map(ADS_CM_HASHER.hash_users)
            | "Upload - Google Ads Customer User Device Id"
            >> beam.ParDo(
                GoogleAdsCustomerMatchUserIdUploaderDoFn(
                    self.params._oauth_credentials,
                    self.params._dataflow_options.developer_token
                )
            )
        )


class GoogleAdsOfflineConversionsStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data - GoogleAdsOfflineConversions"
            >> BatchesFromExecutions(DestinationType.ADS_OFFLINE_CONVERSION, 2000)
            | "Upload - GoogleAdsOfflineConversions"
            >> beam.ParDo(
                GoogleAdsOfflineUploaderDoFn(
                    self.params._oauth_credentials,
                    self.params._dataflow_options.developer_token
                )
            )
        )


class GoogleAnalyticsUserListStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data -  GA user list"
            >> BatchesFromExecutions(DestinationType.GA_USER_LIST_UPLOAD, 5000000)
            | "Upload - GA user list"
            >> beam.ParDo(GoogleAnalyticsUserListUploaderDoFn(self.params._oauth_credentials))
        )


class GoogleAnalyticsDataImportStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data -  GA data import"
            >> BatchesFromExecutions(DestinationType.GA_DATA_IMPORT, 1000000)
            | "Delete Data -  GA data import"
            >> beam.ParDo(GoogleAnalyticsDataImportEraser(self.params._oauth_credentials))
            | "Upload - GA data import"
            >> beam.ParDo(
                GoogleAnalyticsDataImportUploaderDoFn(self.params._oauth_credentials)
            )
        )


class GoogleAnalyticsMeasurementProtocolStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data - GA measurement protocol"
            >> BatchesFromExecutions(
                DestinationType.GA_MEASUREMENT_PROTOCOL, 20, transactional=True
            )
            | "Upload - GA measurement protocol"
            >> beam.ParDo(GoogleAnalyticsMeasurementProtocolUploaderDoFn())
            | "Persist results - GA measurement protocol"
            >> beam.ParDo(
                TransactionalEventsResultsWriter(self.params._dataflow_options.bq_ops_dataset)
            )
        )


class GoogleAnalytics4MeasurementProtocolStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data - GA 4 measurement protocol"
            >> BatchesFromExecutions(
                DestinationType.GA_4_MEASUREMENT_PROTOCOL, 20, transactional=True
            )
            | "Upload - GA 4 measurement protocol"
            >> beam.ParDo(GoogleAnalytics4MeasurementProtocolUploaderDoFn())
            | "Persist results - GA 4 measurement protocol"
            >> beam.ParDo(
                TransactionalEventsResultsWriter(self.params._dataflow_options.bq_ops_dataset)
            )
        )


class CampaignManagerConversionStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data -  CM conversion"
            >> BatchesFromExecutions(
                DestinationType.CM_OFFLINE_CONVERSION, 1000, transactional=True
            )
            | "Upload - CM conversion"
            >> beam.ParDo(
                CampaignManagerConversionUploaderDoFn(self.params._oauth_credentials)
            )
            | "Persist results - CM conversion"
            >> beam.ParDo(
                TransactionalEventsResultsWriter(self.params._dataflow_options.bq_ops_dataset)
            )
        )


def run(argv=None):
    pipeline_options = PipelineOptions()
    dataflow_options = pipeline_options.view_as(DataflowOptions)
    oauth_credentials = OAuthCredentials(
        dataflow_options.client_id,
        dataflow_options.client_secret,
        dataflow_options.access_token,
        dataflow_options.refresh_token,
    )

    sheets_config = SheetsConfig(oauth_credentials)
    json_config = JsonConfig()
    execution_source = PrimaryExecutionSource(
        sheets_config,
        json_config,
        dataflow_options.setup_sheet_id,
        dataflow_options.setup_json_url,
        dataflow_options.setup_firestore_collection,
    )

    params = MegalistaStepParams(oauth_credentials, dataflow_options)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        executions = pipeline | "Load executions" >> beam.io.Read(execution_source)

        executions | GoogleAdsSSDStep(params)
        executions | GoogleAdsCustomerMatchMobileDeviceIdStep(params)
        executions | GoogleAdsCustomerMatchContactInfoStep(params)
        executions | GoogleAdsCustomerMatchUserIdStep(params)
        executions | GoogleAdsOfflineConversionsStep(params)
        executions | GoogleAnalyticsUserListStep(params)
        executions | GoogleAnalyticsDataImportStep(params)
        executions | GoogleAnalyticsMeasurementProtocolStep(params)
        executions | GoogleAnalytics4MeasurementProtocolStep(params)
        executions | CampaignManagerConversionStep(params)

        # Add third party steps
        for step in THIRD_PARTY_STEPS:
          executions | step(params)
        # todo: update trix at the end


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.ERROR)
    logging.getLogger("megalista").setLevel(logging.INFO)
    run()
    logging.getLogger("megalista").info("Completed successfully!")
