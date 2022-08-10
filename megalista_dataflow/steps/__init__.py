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

from .processing_steps import GoogleAdsSSDStep
from .processing_steps import GoogleAdsCustomerMatchMobileDeviceIdStep, GoogleAdsCustomerMatchContactInfoStep, GoogleAdsCustomerMatchUserIdStep
from .processing_steps import GoogleAdsOfflineConversionsStep, GoogleAdsOfflineConversionsCallsStep
from .processing_steps import GoogleAnalyticsUserListStep, GoogleAnalyticsDataImportStep, GoogleAnalyticsMeasurementProtocolStep
from .processing_steps import GoogleAnalytics4MeasurementProtocolStep
from .processing_steps import CampaignManagerConversionStep
from .processing_steps import DisplayVideoCustomerMatchDeviceIdStep, DisplayVideoCustomerMatchContactInfoStep

PROCESSING_STEPS = [
    ["Ads SSD", GoogleAdsSSDStep],
    ["Ads Audiences Device", GoogleAdsCustomerMatchMobileDeviceIdStep],
    ["Ads Audiences Contact", GoogleAdsCustomerMatchContactInfoStep],
    ["Ads Audiences User ID", GoogleAdsCustomerMatchUserIdStep],
    ["Ads OCI (Click)", GoogleAdsOfflineConversionsStep],
    ["Ads OCI (Calls)", GoogleAdsOfflineConversionsCallsStep],
    ["GA 360 User List", GoogleAnalyticsUserListStep],
    ["GA 360 Data Import", GoogleAnalyticsDataImportStep],
    ["GA 360 MP", GoogleAnalyticsMeasurementProtocolStep],
    ["GA4 MP", GoogleAnalytics4MeasurementProtocolStep],
    ["CM OCI", CampaignManagerConversionStep],
    ["DV360 Audiences Device", DisplayVideoCustomerMatchDeviceIdStep],
    ["DV360 Audiences Contact", DisplayVideoCustomerMatchContactInfoStep]
]
