# Copyright 2022 Google LLC
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

import apache_beam as beam

from .megalista_step import MegalistaStep
from sources.batches_from_executions import BatchesFromExecutions, TransactionalType
from models.execution import DestinationType
from error.error_handling import ErrorHandler

from uploaders.support.transactional_events_results_writer import (
    TransactionalEventsResultsWriter,
)
from uploaders.campaign_manager.campaign_manager_conversion_uploader import (
    CampaignManagerConversionUploaderDoFn,
)
from uploaders.google_ads.conversions.google_ads_offline_conversions_calls_uploader import (
    GoogleAdsOfflineUploaderCallsDoFn,
)
from uploaders.google_ads.conversions.google_ads_offline_conversions_uploader import (
    GoogleAdsOfflineUploaderDoFn,
)
from uploaders.google_ads.conversions.google_ads_offline_conversion_adjustments_uploader_gclid import (
    GoogleAdsOfflineAdjustmentGclidUploaderDoFn,
)
from uploaders.google_ads.conversions.google_ads_offline_conversion_adjustments_uploader_order_id import (
    GoogleAdsOfflineAdjustmentOrderIdUploaderDoFn,
)
from uploaders.google_ads.conversions.google_ads_enhanced_conversions_leads_uploader import (
    GoogleAdsECLeadsUploaderDoFn,
)
from uploaders.google_ads.conversions.google_ads_ssd_uploader import (
    GoogleAdsSSDUploaderDoFn,
)
from uploaders.google_ads.conversions.google_ads_ssi_uploader import (
    GoogleAdsSSIUploaderDoFn,
)
from uploaders.google_ads.customer_match.contact_info_uploader import (
    GoogleAdsCustomerMatchContactInfoUploaderDoFn,
)
from uploaders.google_ads.customer_match.mobile_uploader import (
    GoogleAdsCustomerMatchMobileUploaderDoFn,
)
from uploaders.google_ads.customer_match.user_id_uploader import (
    GoogleAdsCustomerMatchUserIdUploaderDoFn,
)
from uploaders.google_analytics.google_analytics_4_measurement_protocol import (
    GoogleAnalytics4MeasurementProtocolUploaderDoFn,
)
from uploaders.google_analytics.google_analytics_data_import_eraser import (
    GoogleAnalyticsDataImportEraser,
)
from uploaders.google_analytics.google_analytics_data_import_uploader import (
    GoogleAnalyticsDataImportUploaderDoFn,
)
from uploaders.google_analytics.google_analytics_measurement_protocol import (
    GoogleAnalyticsMeasurementProtocolUploaderDoFn,
)
from uploaders.google_analytics.google_analytics_user_list_uploader import (
    GoogleAnalyticsUserListUploaderDoFn,
)
from uploaders.display_video.customer_match.contact_info_uploader import (
    DisplayVideoCustomerMatchContactInfoUploaderDoFn,
)
from uploaders.display_video.customer_match.mobile_uploader import (
    DisplayVideoCustomerMatchMobileUploaderDoFn,
)

from mappers.ads_user_list_pii_hashing_mapper import AdsUserListPIIHashingMapper
from mappers.dv_user_list_pii_hashing_mapper import DVUserListPIIHashingMapper

from third_party import THIRD_PARTY_STEPS

ADS_CM_HASHER = AdsUserListPIIHashingMapper()
DV_CM_HASHER = DVUserListPIIHashingMapper()


class GoogleAdsSSDStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data -  Google Ads SSD"
            >> BatchesFromExecutions(
                ErrorHandler(
                    DestinationType.ADS_SSD_UPLOAD, self.params.error_notifier
                ),
                self.params.dataflow_options,
                DestinationType.ADS_SSD_UPLOAD,
                5000,
            )
            | "Hash Users - Google Ads SSD" >> beam.Map(ADS_CM_HASHER.hash_users)
            | "Upload - Google Ads SSD"
            >> beam.ParDo(
                GoogleAdsSSDUploaderDoFn(
                    self.params._oauth_credentials,
                    self.params._dataflow_options.developer_token,
                    ErrorHandler(
                        DestinationType.ADS_SSD_UPLOAD, self.params.error_notifier
                    ),
                )
            )
        )

class GoogleAdsSSIStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data -  Google Ads SSI"
            >> BatchesFromExecutions(
                ErrorHandler(
                    DestinationType.ADS_SSI_UPLOAD, self.params.error_notifier
                ),
                self.params.dataflow_options,
                DestinationType.ADS_SSI_UPLOAD,
                5000,
            )
            | "Hash Users - Google Ads SSI" >> beam.Map(ADS_CM_HASHER.hash_users)
            | "Upload - Google Ads SSI"
            >> beam.ParDo(
                GoogleAdsSSIUploaderDoFn(
                    self.params._oauth_credentials,
                    self.params._dataflow_options.developer_token,
                    ErrorHandler(
                        DestinationType.ADS_SSI_UPLOAD, self.params.error_notifier
                    ),
                )
            )
        )

class GoogleAdsCustomerMatchMobileDeviceIdStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data - Google Ads Customer Match Mobile Device Id"
            >> BatchesFromExecutions(
                ErrorHandler(
                    DestinationType.ADS_CUSTOMER_MATCH_MOBILE_DEVICE_ID_UPLOAD,
                    self.params.error_notifier,
                ),
                self.params.dataflow_options,
                DestinationType.ADS_CUSTOMER_MATCH_MOBILE_DEVICE_ID_UPLOAD,
            )
            | "Hash Users - Google Ads Customer Match Contact Info"
            >> beam.Map(ADS_CM_HASHER.hash_users)
            | "Upload - Google Ads Customer Match Mobile Device Id"
            >> beam.ParDo(
                GoogleAdsCustomerMatchMobileUploaderDoFn(
                    self.params._oauth_credentials,
                    self.params._dataflow_options.developer_token,
                    ErrorHandler(
                        DestinationType.ADS_CUSTOMER_MATCH_MOBILE_DEVICE_ID_UPLOAD,
                        self.params.error_notifier,
                    ),
                )
            )
        )


class GoogleAdsCustomerMatchContactInfoStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data - Google Ads Customer Match Contact Info"
            >> BatchesFromExecutions(
                ErrorHandler(
                    DestinationType.ADS_CUSTOMER_MATCH_CONTACT_INFO_UPLOAD,
                    self.params.error_notifier,
                ),
                self.params.dataflow_options,
                DestinationType.ADS_CUSTOMER_MATCH_CONTACT_INFO_UPLOAD,
            )
            | "Hash Users - Google Ads Customer Match Contact Info"
            >> beam.Map(ADS_CM_HASHER.hash_users)
            | "Upload - Google Ads Customer Match Contact Info"
            >> beam.ParDo(
                GoogleAdsCustomerMatchContactInfoUploaderDoFn(
                    self.params._oauth_credentials,
                    self.params._dataflow_options.developer_token,
                    ErrorHandler(
                        DestinationType.ADS_CUSTOMER_MATCH_CONTACT_INFO_UPLOAD,
                        self.params.error_notifier,
                    ),
                )
            )
        )


class GoogleAdsCustomerMatchUserIdStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data - Google Ads Customer Match User Id"
            >> BatchesFromExecutions(
                ErrorHandler(
                    DestinationType.ADS_CUSTOMER_MATCH_USER_ID_UPLOAD,
                    self.params.error_notifier,
                ),
                self.params.dataflow_options,
                DestinationType.ADS_CUSTOMER_MATCH_USER_ID_UPLOAD,
            )
            | "Hash Users - Google Ads Customer Match Contact Info"
            >> beam.Map(ADS_CM_HASHER.hash_users)
            | "Upload - Google Ads Customer User Device Id"
            >> beam.ParDo(
                GoogleAdsCustomerMatchUserIdUploaderDoFn(
                    self.params._oauth_credentials,
                    self.params._dataflow_options.developer_token,
                    ErrorHandler(
                        DestinationType.ADS_CUSTOMER_MATCH_USER_ID_UPLOAD,
                        self.params.error_notifier,
                    ),
                )
            )
        )


class GoogleAdsOfflineConversionsStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data - GoogleAdsOfflineConversions"
            >> BatchesFromExecutions(
                ErrorHandler(
                    DestinationType.ADS_OFFLINE_CONVERSION, self.params.error_notifier
                ),
                self.params.dataflow_options,
                DestinationType.ADS_OFFLINE_CONVERSION,
                2000,
                TransactionalType.GCLID_TIME,
            )
            | "Upload - GoogleAdsOfflineConversions"
            >> beam.ParDo(
                GoogleAdsOfflineUploaderDoFn(
                    self.params._oauth_credentials,
                    self.params._dataflow_options.developer_token,
                    ErrorHandler(
                        DestinationType.ADS_OFFLINE_CONVERSION,
                        self.params.error_notifier,
                    ),
                )
            )
            | "Persist results - GoogleAdsOfflineConversions"
            >> TransactionalEventsResultsWriter(
                self.params._dataflow_options,
                TransactionalType.GCLID_TIME,
                ErrorHandler(
                    DestinationType.ADS_OFFLINE_CONVERSION, self.params.error_notifier
                ),
            )
        )


class GoogleAdsOfflineConversionAdjustmentsGclidStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data - GoogleAdsOfflineConversionAdjustments"
            >> BatchesFromExecutions(
                ErrorHandler(
                    DestinationType.ADS_OFFLINE_CONVERSION_ADJUSTMENT_GCLID,
                    self.params.error_notifier,
                ),
                self.params.dataflow_options,
                DestinationType.ADS_OFFLINE_CONVERSION_ADJUSTMENT_GCLID,
                2000,
                TransactionalType.GCLID_TIME,
            )
            | "Upload - GoogleAdsOfflineConversionAdjustments"
            >> beam.ParDo(
                GoogleAdsOfflineAdjustmentGclidUploaderDoFn(
                    self.params._oauth_credentials,
                    self.params._dataflow_options.developer_token,
                    ErrorHandler(
                        DestinationType.ADS_OFFLINE_CONVERSION_ADJUSTMENT_GCLID,
                        self.params.error_notifier,
                    ),
                )
            )
            | "Persist results - GoogleAdsOfflineConversionAdjustments"
            >> TransactionalEventsResultsWriter(
                self.params._dataflow_options,
                TransactionalType.GCLID_TIME,
                ErrorHandler(
                    DestinationType.ADS_OFFLINE_CONVERSION_ADJUSTMENT_GCLID,
                    self.params.error_notifier,
                ),
            )
        )


class GoogleAdsOfflineConversionAdjustmentsOrderIdStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data - GoogleAdsOfflineConversionAdjustments"
            >> BatchesFromExecutions(
                ErrorHandler(
                    DestinationType.ADS_OFFLINE_CONVERSION_ADJUSTMENT_ORDER_ID,
                    self.params.error_notifier,
                ),
                self.params.dataflow_options,
                DestinationType.ADS_OFFLINE_CONVERSION_ADJUSTMENT_ORDER_ID,
                2000,
                TransactionalType.ORDER_ID_TIME,
            )
            | "Upload - GoogleAdsOfflineConversionAdjustments"
            >> beam.ParDo(
                GoogleAdsOfflineAdjustmentOrderIdUploaderDoFn(
                    self.params._oauth_credentials,
                    self.params._dataflow_options.developer_token,
                    ErrorHandler(
                        DestinationType.ADS_OFFLINE_CONVERSION_ADJUSTMENT_ORDER_ID,
                        self.params.error_notifier,
                    ),
                )
            )
            | "Persist results - GoogleAdsOfflineConversionAdjustments"
            >> TransactionalEventsResultsWriter(
                self.params._dataflow_options,
                TransactionalType.ORDER_ID_TIME,
                ErrorHandler(
                    DestinationType.ADS_OFFLINE_CONVERSION_ADJUSTMENT_ORDER_ID,
                    self.params.error_notifier,
                ),
            )
        )
    
class GoogleAdsOfflineConversionAdjustmentsGclidStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data - GoogleAdsOfflineConversionAdjustments"
            >> BatchesFromExecutions(
                ErrorHandler(DestinationType.ADS_OFFLINE_CONVERSION_ADJUSTMENT_GCLID,
                             self.params.error_notifier),
                self.params.dataflow_options,
                DestinationType.ADS_OFFLINE_CONVERSION_ADJUSTMENT_GCLID,
                2000,
                TransactionalType.GCLID_TIME)
            | "Upload - GoogleAdsOfflineConversionAdjustments"
            >> beam.ParDo(
                GoogleAdsOfflineAdjustmentGclidUploaderDoFn(
                    self.params._oauth_credentials,
                    self.params._dataflow_options.developer_token,
                    ErrorHandler(DestinationType.ADS_OFFLINE_CONVERSION_ADJUSTMENT_GCLID,
                                 self.params.error_notifier)
                )
            )
            | "Persist results - GoogleAdsOfflineConversions"
            >> TransactionalEventsResultsWriter(
                self.params._dataflow_options,
                TransactionalType.GCLID_TIME,
                ErrorHandler(DestinationType.ADS_OFFLINE_CONVERSION_ADJUSTMENT_GCLID, self.params.error_notifier))
        )

class GoogleAdsOfflineConversionAdjustmentsOrderIdStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data - GoogleAdsOfflineConversionAdjustments"
            >> BatchesFromExecutions(
                ErrorHandler(DestinationType.ADS_OFFLINE_CONVERSION_ADJUSTMENT_ORDER_ID,
                             self.params.error_notifier),
                self.params.dataflow_options,
                DestinationType.ADS_OFFLINE_CONVERSION_ADJUSTMENT_ORDER_ID,
                2000,
                TransactionalType.ORDER_ID_TIME)
            | "Upload - GoogleAdsOfflineConversionAdjustments"
            >> beam.ParDo(
                GoogleAdsOfflineAdjustmentOrderIdUploaderDoFn(
                    self.params._oauth_credentials,
                    self.params._dataflow_options.developer_token,
                    ErrorHandler(DestinationType.ADS_OFFLINE_CONVERSION_ADJUSTMENT_ORDER_ID,
                                 self.params.error_notifier)
                )
            )
            | "Persist results - GoogleAdsOfflineConversions"
            >> TransactionalEventsResultsWriter(
                self.params._dataflow_options,
                TransactionalType.ORDER_ID_TIME,
                ErrorHandler(DestinationType.ADS_OFFLINE_CONVERSION_ADJUSTMENT_ORDER_ID, self.params.error_notifier))
        )

class GoogleAdsOfflineConversionsCallsStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data - GoogleAdsOfflineConversionsCalls"
            >> BatchesFromExecutions(
                ErrorHandler(
                    DestinationType.ADS_OFFLINE_CONVERSION_CALLS,
                    self.params.error_notifier,
                ),
                self.params.dataflow_options,
                DestinationType.ADS_OFFLINE_CONVERSION_CALLS,
                2000,
                TransactionalType.NOT_TRANSACTIONAL,
            )
            | "Upload - GoogleAdsOfflineConversionsCalls"
            >> beam.ParDo(
                GoogleAdsOfflineUploaderCallsDoFn(
                    self.params._oauth_credentials,
                    self.params._dataflow_options.developer_token,
                    ErrorHandler(
                        DestinationType.ADS_OFFLINE_CONVERSION_CALLS,
                        self.params.error_notifier,
                    ),
                )
            )
            | "Persist results - GoogleAdsOfflineConversions"
            >> TransactionalEventsResultsWriter(
                self.params._dataflow_options,
                TransactionalType.GCLID_TIME,
                ErrorHandler(
                    DestinationType.ADS_OFFLINE_CONVERSION_CALLS,
                    self.params.error_notifier,
                ),
            )
        )


class GoogleAdsECLeadsStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data - GoogleAdsECLeadsConversions"
            >> BatchesFromExecutions(
                ErrorHandler(
                    DestinationType.ADS_ENHANCED_CONVERSION_LEADS,
                    self.params.error_notifier,
                ),
                self.params.dataflow_options,
                DestinationType.ADS_ENHANCED_CONVERSION_LEADS,
                2000,
                TransactionalType.UUID,
            )
            | "Hash Users - Google Ads EC for Leads Contact Info"
            >> beam.Map(ADS_CM_HASHER.hash_users)
            | "Upload - GoogleAdsECLeadsConversions"
            >> beam.ParDo(
                GoogleAdsECLeadsUploaderDoFn(
                    self.params._oauth_credentials,
                    self.params._dataflow_options.developer_token,
                    ErrorHandler(
                        DestinationType.ADS_ENHANCED_CONVERSION_LEADS,
                        self.params.error_notifier,
                    ),
                )
            )
            | "Persist results - GoogleAdsECLeadsConversions"
            >> TransactionalEventsResultsWriter(
                self.params._dataflow_options,
                TransactionalType.UUID,
                ErrorHandler(
                    DestinationType.ADS_ENHANCED_CONVERSION_LEADS,
                    self.params.error_notifier,
                ),
            )
        )


class GoogleAnalyticsUserListStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data -  GA user list"
            >> BatchesFromExecutions(
                ErrorHandler(
                    DestinationType.GA_USER_LIST_UPLOAD, self.params.error_notifier
                ),
                self.params.dataflow_options,
                DestinationType.GA_USER_LIST_UPLOAD,
                5000000,
            )
            | "Upload - GA user list"
            >> beam.ParDo(
                GoogleAnalyticsUserListUploaderDoFn(
                    self.params._oauth_credentials,
                    ErrorHandler(
                        DestinationType.GA_USER_LIST_UPLOAD, self.params.error_notifier
                    ),
                )
            )
        )


class GoogleAnalyticsDataImportStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data -  GA data import"
            >> BatchesFromExecutions(
                ErrorHandler(
                    DestinationType.GA_DATA_IMPORT, self.params.error_notifier
                ),
                self.params.dataflow_options,
                DestinationType.GA_DATA_IMPORT,
                1000000,
            )
            | "Delete Data -  GA data import"
            >> beam.ParDo(
                GoogleAnalyticsDataImportEraser(
                    self.params._oauth_credentials,
                    ErrorHandler(
                        DestinationType.GA_DATA_IMPORT, self.params.error_notifier
                    ),
                )
            )
            | "Upload - GA data import"
            >> beam.ParDo(
                GoogleAnalyticsDataImportUploaderDoFn(
                    self.params._oauth_credentials,
                    ErrorHandler(
                        DestinationType.GA_DATA_IMPORT, self.params.error_notifier
                    ),
                )
            )
        )


class GoogleAnalyticsMeasurementProtocolStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data - GA measurement protocol"
            >> BatchesFromExecutions(
                ErrorHandler(
                    DestinationType.GA_MEASUREMENT_PROTOCOL, self.params.error_notifier
                ),
                self.params.dataflow_options,
                DestinationType.GA_MEASUREMENT_PROTOCOL,
                20,
                TransactionalType.UUID,
            )
            | "Upload - GA measurement protocol"
            >> beam.ParDo(
                GoogleAnalyticsMeasurementProtocolUploaderDoFn(
                    ErrorHandler(
                        DestinationType.GA_MEASUREMENT_PROTOCOL,
                        self.params.error_notifier,
                    )
                )
            )
            | "Persist results - GA measurement protocol"
            >> TransactionalEventsResultsWriter(
                self.params._dataflow_options,
                TransactionalType.UUID,
                ErrorHandler(
                    DestinationType.GA_MEASUREMENT_PROTOCOL, self.params.error_notifier
                ),
            )
        )


class GoogleAnalytics4MeasurementProtocolStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data - GA 4 measurement protocol"
            >> BatchesFromExecutions(
                ErrorHandler(
                    DestinationType.GA_4_MEASUREMENT_PROTOCOL,
                    self.params.error_notifier,
                ),
                self.params.dataflow_options,
                DestinationType.GA_4_MEASUREMENT_PROTOCOL,
                20,
                TransactionalType.UUID,
            )
            | "Upload - GA 4 measurement protocol"
            >> beam.ParDo(
                GoogleAnalytics4MeasurementProtocolUploaderDoFn(
                    ErrorHandler(
                        DestinationType.GA_4_MEASUREMENT_PROTOCOL,
                        self.params.error_notifier,
                    )
                )
            )
            | "Persist results - GA 4 measurement protocol"
            >> TransactionalEventsResultsWriter(
                self.params._dataflow_options,
                TransactionalType.UUID,
                ErrorHandler(
                    DestinationType.GA_4_MEASUREMENT_PROTOCOL,
                    self.params.error_notifier,
                ),
            )
        )


class CampaignManagerConversionStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data -  CM conversion"
            >> BatchesFromExecutions(
                ErrorHandler(
                    DestinationType.CM_OFFLINE_CONVERSION, self.params.error_notifier
                ),
                self.params.dataflow_options,
                DestinationType.CM_OFFLINE_CONVERSION,
                1000,
                TransactionalType.UUID,
            )
            | "Upload - CM conversion"
            >> beam.ParDo(
                CampaignManagerConversionUploaderDoFn(
                    self.params._oauth_credentials,
                    ErrorHandler(
                        DestinationType.CM_OFFLINE_CONVERSION,
                        self.params.error_notifier,
                    ),
                )
            )
            | "Persist results - CM conversion"
            >> TransactionalEventsResultsWriter(
                self.params._dataflow_options,
                TransactionalType.UUID,
                ErrorHandler(
                    DestinationType.CM_OFFLINE_CONVERSION, self.params.error_notifier
                ),
            )
        )


class DisplayVideoCustomerMatchDeviceIdStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data - Display & Video Customer Match Device Id"
            >> BatchesFromExecutions(
                ErrorHandler(
                    DestinationType.DV_CUSTOMER_MATCH_DEVICE_ID_UPLOAD,
                    self.params.error_notifier,
                ),
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
                    ErrorHandler(
                        DestinationType.DV_CUSTOMER_MATCH_DEVICE_ID_UPLOAD,
                        self.params.error_notifier,
                    ),
                )
            )
        )


class DisplayVideoCustomerMatchContactInfoStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | "Load Data - Display & Video Customer Match Contact Info"
            >> BatchesFromExecutions(
                ErrorHandler(
                    DestinationType.DV_CUSTOMER_MATCH_CONTACT_INFO_UPLOAD,
                    self.params.error_notifier,
                ),
                self.params.dataflow_options,
                DestinationType.DV_CUSTOMER_MATCH_CONTACT_INFO_UPLOAD,
            )
            | "Hash Users - Display & Video Customer Match Contact Info"
            >> beam.Map(DV_CM_HASHER.hash_users)
            | "Upload - Display & Video Customer Match Contact Info"
            >> beam.ParDo(
                DisplayVideoCustomerMatchContactInfoUploaderDoFn(
                    self.params._oauth_credentials,
                    self.params._dataflow_options.developer_token,
                    ErrorHandler(
                        DestinationType.DV_CUSTOMER_MATCH_CONTACT_INFO_UPLOAD,
                        self.params.error_notifier,
                    ),
                )
            )
        )


PROCESSING_STEPS = [
    ["Ads SSD", GoogleAdsSSDStep],
    ["Ads SSI", GoogleAdsSSIStep],
    ["Ads Audiences Device", GoogleAdsCustomerMatchMobileDeviceIdStep],
    ["Ads Audiences Contact", GoogleAdsCustomerMatchContactInfoStep],
    ["Ads Audiences User ID", GoogleAdsCustomerMatchUserIdStep],
    ["Ads OCI (Click)", GoogleAdsOfflineConversionsStep],
    ["Ads OCI (Calls)", GoogleAdsOfflineConversionsCallsStep],
    ["Ads OCA (gclid)", GoogleAdsOfflineConversionAdjustmentsGclidStep],
    ["Ads OCA (order id)", GoogleAdsOfflineConversionAdjustmentsOrderIdStep],
    ["Ads ECLeads", GoogleAdsECLeadsStep],
    ["GA 360 User List", GoogleAnalyticsUserListStep],
    ["GA 360 Data Import", GoogleAnalyticsDataImportStep],
    ["GA 360 MP", GoogleAnalyticsMeasurementProtocolStep],
    ["GA4 MP", GoogleAnalytics4MeasurementProtocolStep],
    ["CM OCI", CampaignManagerConversionStep],
    ["DV360 Audiences Device", DisplayVideoCustomerMatchDeviceIdStep],
    ["DV360 Audiences Contact", DisplayVideoCustomerMatchContactInfoStep],
]


class ProcessingStep(MegalistaStep):
    def expand(self, executions):
        processing_results = []

        # Add execution steps
        for name, step in PROCESSING_STEPS:
            processing_results.append(executions | name >> step(self._params))

        # Add third party steps
        for name, step in THIRD_PARTY_STEPS:
            processing_results.append(executions | name >> step(self._params))

        return processing_results
