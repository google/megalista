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
from apache_beam import coders
from apache_beam.options.pipeline_options import PipelineOptions

from error.error_handling import ErrorHandler, ErrorNotifier, GmailNotifier
from mappers.ads_user_list_pii_hashing_mapper import \
  AdsUserListPIIHashingMapper
from mappers.dv_user_list_pii_hashing_mapper import \
  DVUserListPIIHashingMapper
from mappers.executions_grouped_by_source_mapper import ExecutionsGroupedBySourceMapper, ExecutionsGroupedBySourceCombineFn
from models.execution import DataRowsGroupedBySource, DestinationType, Execution, ExecutionsGroupedBySource
from models.json_config import JsonConfig
from models.oauth_credentials import OAuthCredentials
from models.options import DataflowOptions
from models.sheets_config import SheetsConfig
from sources.batches_from_executions import BatchesFromExecutions, ExecutionsGroupedBySourceCoder, TransactionalType, DataRowsGroupedBySourceCoder, ExecutionCoder
from sources.primary_execution_source import PrimaryExecutionSource
from third_party import THIRD_PARTY_STEPS
from uploaders.support.transactional_events_results_writer import TransactionalEventsResultsWriter
from uploaders.campaign_manager.campaign_manager_conversion_uploader import CampaignManagerConversionUploaderDoFn
from uploaders.google_ads.conversions.google_ads_offline_conversions_calls_uploader import GoogleAdsOfflineUploaderCallsDoFn
from uploaders.google_ads.conversions.google_ads_offline_conversions_uploader import GoogleAdsOfflineUploaderDoFn
from uploaders.google_ads.conversions.google_ads_ssd_uploader import GoogleAdsSSDUploaderDoFn
from uploaders.google_ads.customer_match.contact_info_uploader import GoogleAdsCustomerMatchContactInfoUploaderDoFn
from uploaders.google_ads.customer_match.mobile_uploader import GoogleAdsCustomerMatchMobileUploaderDoFn
from uploaders.google_ads.customer_match.user_id_uploader import GoogleAdsCustomerMatchUserIdUploaderDoFn
from uploaders.google_analytics.google_analytics_4_measurement_protocol import \
  GoogleAnalytics4MeasurementProtocolUploaderDoFn
from uploaders.google_analytics.google_analytics_data_import_eraser import GoogleAnalyticsDataImportEraser
from uploaders.google_analytics.google_analytics_data_import_uploader import GoogleAnalyticsDataImportUploaderDoFn
from uploaders.google_analytics.google_analytics_measurement_protocol import \
  GoogleAnalyticsMeasurementProtocolUploaderDoFn
from uploaders.google_analytics.google_analytics_user_list_uploader import GoogleAnalyticsUserListUploaderDoFn
from uploaders.display_video.customer_match.contact_info_uploader import DisplayVideoCustomerMatchContactInfoUploaderDoFn
from uploaders.display_video.customer_match.mobile_uploader import DisplayVideoCustomerMatchMobileUploaderDoFn


warnings.filterwarnings(
    "ignore", "Your application has authenticated using end user credentials"
)

ADS_CM_HASHER = AdsUserListPIIHashingMapper()
DV_CM_HASHER = DVUserListPIIHashingMapper()
EXECUTIONS_MAPPER = ExecutionsGroupedBySourceMapper()

def filter_by_action(execution: Execution, destination_type: DestinationType):
    return execution.destination.destination_type is destination_type


class MegalistaStepParams():
    def __init__(self, oauth_credentials, dataflow_options, error_notifier: ErrorNotifier):
        self._oauth_credentials = oauth_credentials
        self._dataflow_options = dataflow_options
        self._error_notifier = error_notifier

    @property
    def oauth_credentials(self):
        return self._oauth_credentials

    @property
    def dataflow_options(self):
        return self._dataflow_options

    @property
    def error_notifier(self):
        return self._error_notifier


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
            >> BatchesFromExecutions(
                ErrorHandler(DestinationType.ADS_SSD_UPLOAD, self.params.error_notifier),
                self.params.dataflow_options, 
                DestinationType.ADS_SSD_UPLOAD, 
                5000
            )
            | "Hash Users - Google Ads SSD" >> beam.Map(ADS_CM_HASHER.hash_users)
            | "Upload - Google Ads SSD"
            >> beam.ParDo(
                GoogleAdsSSDUploaderDoFn(
                    self.params._oauth_credentials,
                    self.params._dataflow_options.developer_token,
                    ErrorHandler(DestinationType.ADS_SSD_UPLOAD, self.params.error_notifier)
                )
            )
        )


class GoogleAdsCustomerMatchMobileDeviceIdStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data - Google Ads Customer Match Mobile Device Id"
            >> BatchesFromExecutions(
                ErrorHandler(DestinationType.ADS_CUSTOMER_MATCH_MOBILE_DEVICE_ID_UPLOAD, self.params.error_notifier),
                self.params.dataflow_options, 
                DestinationType.ADS_CUSTOMER_MATCH_MOBILE_DEVICE_ID_UPLOAD
            )
            | "Hash Users - Google Ads Customer Match Contact Info"
            >> beam.Map(ADS_CM_HASHER.hash_users)
            | "Upload - Google Ads Customer Match Mobile Device Id"
            >> beam.ParDo(
                GoogleAdsCustomerMatchMobileUploaderDoFn(
                    self.params._oauth_credentials,
                    self.params._dataflow_options.developer_token,
                    ErrorHandler(DestinationType.ADS_CUSTOMER_MATCH_MOBILE_DEVICE_ID_UPLOAD, self.params.error_notifier)
                )
            )
        )


class GoogleAdsCustomerMatchContactInfoStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data - Google Ads Customer Match Contact Info"
            >> BatchesFromExecutions(
                ErrorHandler(DestinationType.ADS_CUSTOMER_MATCH_CONTACT_INFO_UPLOAD, self.params.error_notifier),
                self.params.dataflow_options, 
                DestinationType.ADS_CUSTOMER_MATCH_CONTACT_INFO_UPLOAD
            )
            | "Hash Users - Google Ads Customer Match Contact Info"
            >> beam.Map(ADS_CM_HASHER.hash_users)
            | "Upload - Google Ads Customer Match Contact Info"
            >> beam.ParDo(
                GoogleAdsCustomerMatchContactInfoUploaderDoFn(
                    self.params._oauth_credentials,
                    self.params._dataflow_options.developer_token,
                  ErrorHandler(DestinationType.ADS_CUSTOMER_MATCH_CONTACT_INFO_UPLOAD, self.params.error_notifier)
                )
            )
        )


class GoogleAdsCustomerMatchUserIdStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data - Google Ads Customer Match User Id"
            >> BatchesFromExecutions(
                ErrorHandler(DestinationType.ADS_CUSTOMER_MATCH_USER_ID_UPLOAD, self.params.error_notifier),
                self.params.dataflow_options, 
                DestinationType.ADS_CUSTOMER_MATCH_USER_ID_UPLOAD
            )
            | "Hash Users - Google Ads Customer Match Contact Info"
            >> beam.Map(ADS_CM_HASHER.hash_users)
            | "Upload - Google Ads Customer User Device Id"
            >> beam.ParDo(
                GoogleAdsCustomerMatchUserIdUploaderDoFn(
                    self.params._oauth_credentials,
                    self.params._dataflow_options.developer_token,
                    ErrorHandler(DestinationType.ADS_CUSTOMER_MATCH_USER_ID_UPLOAD, self.params.error_notifier)
                )
            )
        )


class GoogleAdsOfflineConversionsStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data - GoogleAdsOfflineConversions"
            >> BatchesFromExecutions(
                ErrorHandler(DestinationType.ADS_OFFLINE_CONVERSION, self.params.error_notifier),
                self.params.dataflow_options, 
                DestinationType.ADS_OFFLINE_CONVERSION,
                2000,
                TransactionalType.GCLID_TIME)
            | "Upload - GoogleAdsOfflineConversions"
            >> beam.ParDo(
                GoogleAdsOfflineUploaderDoFn(
                    self.params._oauth_credentials,
                    self.params._dataflow_options.developer_token,
                    ErrorHandler(DestinationType.ADS_OFFLINE_CONVERSION, self.params.error_notifier)
                )
            )
            | "Persist results - GoogleAdsOfflineConversions"
            >> TransactionalEventsResultsWriter(
                self.params._dataflow_options,
                TransactionalType.GCLID_TIME,
                ErrorHandler(DestinationType.ADS_OFFLINE_CONVERSION, self.params.error_notifier))
        )


class GoogleAdsOfflineConversionsCallsStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data - GoogleAdsOfflineConversionsCalls"
            >> BatchesFromExecutions(
                ErrorHandler(DestinationType.ADS_OFFLINE_CONVERSION_CALLS, self.params.error_notifier),
                self.params.dataflow_options, 
                DestinationType.ADS_OFFLINE_CONVERSION_CALLS,
                2000,
                TransactionalType.NOT_TRANSACTIONAL)
            | "Upload - GoogleAdsOfflineConversionsCalls"
            >> beam.ParDo(
                GoogleAdsOfflineUploaderCallsDoFn(
                    self.params._oauth_credentials,
                    self.params._dataflow_options.developer_token,
                    ErrorHandler(DestinationType.ADS_OFFLINE_CONVERSION_CALLS, self.params.error_notifier)
                )
            )
            | "Persist results - GoogleAdsOfflineConversions"
            >> TransactionalEventsResultsWriter(
                self.params._dataflow_options,
                TransactionalType.GCLID_TIME,
                ErrorHandler(DestinationType.ADS_OFFLINE_CONVERSION_CALLS, self.params.error_notifier))
        )


class GoogleAnalyticsUserListStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data -  GA user list"
            >> BatchesFromExecutions(
                ErrorHandler(DestinationType.GA_USER_LIST_UPLOAD, self.params.error_notifier),
                self.params.dataflow_options, 
                DestinationType.GA_USER_LIST_UPLOAD, 
                5000000
            )
            | "Upload - GA user list"
            >> beam.ParDo(GoogleAnalyticsUserListUploaderDoFn(self.params._oauth_credentials,
                                                              ErrorHandler(DestinationType.GA_USER_LIST_UPLOAD,
                                                                           self.params.error_notifier)))
        )


class GoogleAnalyticsDataImportStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data -  GA data import"
            >> BatchesFromExecutions(
                ErrorHandler(DestinationType.GA_DATA_IMPORT, self.params.error_notifier),
                self.params.dataflow_options, 
                DestinationType.GA_DATA_IMPORT, 
                1000000
            )
            | "Delete Data -  GA data import"
            >> beam.ParDo(
          GoogleAnalyticsDataImportEraser(self.params._oauth_credentials,
                                          ErrorHandler(DestinationType.GA_DATA_IMPORT, self.params.error_notifier)))
            | "Upload - GA data import"
            >> beam.ParDo(
          GoogleAnalyticsDataImportUploaderDoFn(self.params._oauth_credentials,
                                                ErrorHandler(DestinationType.GA_DATA_IMPORT,
                                                             self.params.error_notifier))
            )
        )


class GoogleAnalyticsMeasurementProtocolStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data - GA measurement protocol"
            >> BatchesFromExecutions(
                ErrorHandler(DestinationType.GA_MEASUREMENT_PROTOCOL, self.params.error_notifier),
                self.params.dataflow_options, 
                DestinationType.GA_MEASUREMENT_PROTOCOL,
                20,
                TransactionalType.UUID)
            | "Upload - GA measurement protocol"
            >> beam.ParDo(GoogleAnalyticsMeasurementProtocolUploaderDoFn(
                ErrorHandler(DestinationType.GA_MEASUREMENT_PROTOCOL, self.params.error_notifier)))
            | "Persist results - GA measurement protocol"
            >> TransactionalEventsResultsWriter(
                self.params._dataflow_options,
                TransactionalType.UUID,
                ErrorHandler(DestinationType.GA_MEASUREMENT_PROTOCOL, self.params.error_notifier))
        )


class GoogleAnalytics4MeasurementProtocolStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data - GA 4 measurement protocol"
            >> BatchesFromExecutions(
                ErrorHandler(DestinationType.GA_4_MEASUREMENT_PROTOCOL, self.params.error_notifier),
                self.params.dataflow_options, 
                DestinationType.GA_4_MEASUREMENT_PROTOCOL,
                20,
                TransactionalType.UUID)
            | "Upload - GA 4 measurement protocol"
            >> beam.ParDo(GoogleAnalytics4MeasurementProtocolUploaderDoFn(
                ErrorHandler(DestinationType.GA_4_MEASUREMENT_PROTOCOL, self.params.error_notifier)))
            | "Persist results - GA 4 measurement protocol"
            >> TransactionalEventsResultsWriter(
                self.params._dataflow_options,
                TransactionalType.UUID,
                ErrorHandler(DestinationType.GA_4_MEASUREMENT_PROTOCOL, self.params.error_notifier))
        )


class CampaignManagerConversionStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data -  CM conversion"
            >> BatchesFromExecutions(
                ErrorHandler(DestinationType.CM_OFFLINE_CONVERSION, self.params.error_notifier),
                self.params.dataflow_options, 
                DestinationType.CM_OFFLINE_CONVERSION,
                1000,
                TransactionalType.UUID)
            | "Upload - CM conversion"
            >> beam.ParDo(
                CampaignManagerConversionUploaderDoFn(self.params._oauth_credentials,
                                                      ErrorHandler(DestinationType.CM_OFFLINE_CONVERSION,
                                                                   self.params.error_notifier))
            )
            | "Persist results - CM conversion"
            >> TransactionalEventsResultsWriter(
                self.params._dataflow_options,
                TransactionalType.UUID,
                ErrorHandler(DestinationType.CM_OFFLINE_CONVERSION, self.params.error_notifier))
        )

class DisplayVideoCustomerMatchDeviceIdStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data - Display & Video Customer Match Device Id"
            >> BatchesFromExecutions(
                ErrorHandler(DestinationType.DV_CUSTOMER_MATCH_DEVICE_ID_UPLOAD, self.params.error_notifier),
                self.params.dataflow_options, 
                DestinationType.DV_CUSTOMER_MATCH_DEVICE_ID_UPLOAD,
            )
            | "Hash Users - Display & Video Customer Match Contact Info"
            >> beam.Map(DV_CM_HASHER.hash_users)
            | "Upload - Display & Video Customer Match Mobile Device Id"
            >> beam.ParDo(
                DisplayVideoCustomerMatchMobileUploaderDoFn(
                    self.params._oauth_credentials,
                    self.params._dataflow_options.developer_token,
                    ErrorHandler(DestinationType.DV_CUSTOMER_MATCH_DEVICE_ID_UPLOAD, self.params.error_notifier)
                )
            )
        )


class DisplayVideoCustomerMatchContactInfoStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data - Display & Video Customer Match Contact Info"
            >> BatchesFromExecutions(
                ErrorHandler(DestinationType.DV_CUSTOMER_MATCH_CONTACT_INFO_UPLOAD, self.params.error_notifier),
                self.params.dataflow_options, 
                DestinationType.DV_CUSTOMER_MATCH_CONTACT_INFO_UPLOAD
            )
            | "Hash Users - Display & Video Customer Match Contact Info"
            >> beam.Map(DV_CM_HASHER.hash_users)
            | "Upload - Display & Video Customer Match Contact Info"
            >> beam.ParDo(
                DisplayVideoCustomerMatchContactInfoUploaderDoFn(
                    self.params._oauth_credentials,
                    self.params._dataflow_options.developer_token,
                  ErrorHandler(DestinationType.DV_CUSTOMER_MATCH_CONTACT_INFO_UPLOAD, self.params.error_notifier)
                )
            )
        )

class LoadExecutionsStep(MegalistaStep):
    def __init__(self, params, execution_source):
        super().__init__(params)
        self._execution_source = execution_source

    def expand(self, pipeline):
        return (pipeline 
            | "Read config" >> beam.io.Read(self._execution_source)
            | "Transform into tuples" >> beam.Map(lambda execution: (execution.source.source_name, execution))
            | "Group by source name" >> beam.CombinePerKey(ExecutionsGroupedBySourceCombineFn())
            | "Encapsulate into object" >> beam.Map(ExecutionsGroupedBySourceMapper().encapsulate)
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
    json_config = JsonConfig(dataflow_options)
    execution_source = PrimaryExecutionSource(
        sheets_config,
        json_config,
        dataflow_options.setup_sheet_id,
        dataflow_options.setup_json_url,
        dataflow_options.setup_firestore_collection,
    )

    error_notifier = GmailNotifier(dataflow_options.notify_errors_by_email, oauth_credentials,
                                   dataflow_options.errors_destination_emails)
    params = MegalistaStepParams(oauth_credentials, dataflow_options, error_notifier)

    coders.registry.register_coder(Execution, ExecutionCoder)
    coders.registry.register_coder(ExecutionsGroupedBySource, ExecutionsGroupedBySourceCoder)
    coders.registry.register_coder(DataRowsGroupedBySource, DataRowsGroupedBySourceCoder)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        executions = (pipeline 
            | "Load executions" >> LoadExecutionsStep(params, execution_source)
        )

        executions | "Ads SSD" >> GoogleAdsSSDStep(params)
        executions | "Ads Audiences Device" >> GoogleAdsCustomerMatchMobileDeviceIdStep(params)
        executions | "Ads Audiences Contact" >> GoogleAdsCustomerMatchContactInfoStep(params)
        executions | "Ads Audiences User ID" >> GoogleAdsCustomerMatchUserIdStep(params)
        executions | "Ads OCI (Click)" >> GoogleAdsOfflineConversionsStep(params)
        executions | "Ads OCI (Calls)" >> GoogleAdsOfflineConversionsCallsStep(params)
        executions | "GA 360 User List" >> GoogleAnalyticsUserListStep(params)
        executions | "GA 360 Data Import" >> GoogleAnalyticsDataImportStep(params)
        executions | "GA 360 MP" >> GoogleAnalyticsMeasurementProtocolStep(params)
        executions | "GA4 MP" >> GoogleAnalytics4MeasurementProtocolStep(params)
        executions | "CM OCI" >> CampaignManagerConversionStep(params)
        executions | "DV360 Audiences Device" >> DisplayVideoCustomerMatchDeviceIdStep(params)
        executions | "DV360 Audiences Contact" >> DisplayVideoCustomerMatchContactInfoStep(params)

        # Add third party steps
        for step in THIRD_PARTY_STEPS:
          executions | step(params)
        # todo: update trix at the end


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.ERROR)
    logging.getLogger("megalista").setLevel(logging.INFO)
    run()
    logging.getLogger("megalista").info("Completed successfully!")
