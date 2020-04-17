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

from apache_beam.options.pipeline_options import PipelineOptions


class DataflowOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    # OAUTH
    parser.add_value_provider_argument(
      '--client_id',
      help='Client Id for the Google APIs')
    parser.add_value_provider_argument(
      '--client_secret',
      help='Client Secret for the Google APIs')
    parser.add_value_provider_argument(
      '--refresh_token',
      help='OAUTH Refresh Token for the Google APIs')
    parser.add_value_provider_argument(
      '--access_token',
      help='OAUTH Access Token for the Google APIs')
    # Set up
    parser.add_value_provider_argument(
      '--setup_sheet_id',
      help='Id of Spreadsheet with execution info')
    # Google Ads
    parser.add_value_provider_argument(
      '--developer_token',
      help='Developer Token for Google Ads API')
    parser.add_value_provider_argument(
      '--customer_id',
      help='Google Ads Customer ID')
    parser.add_value_provider_argument(
      '--ssd_conversion_name',
      help='Google Ads Store Sales Direct Conversion Name')
    parser.add_value_provider_argument(
      '--ssd_external_upload_id',
      help='Google Ads Store Sales Direct External Upload Id')
    parser.add_value_provider_argument(
      '--app_id',
      help='Id for the App in the Play Store')
    # Google Analytics
    parser.add_value_provider_argument(
      '--google_analytics_account_id',
      help='Google Analytics Account ID')
    parser.add_value_provider_argument(
      '--google_analytics_web_property_id',
      help='Google Analytics Web Property ID')
    parser.add_value_provider_argument(
      '--google_analytics_user_id_custom_dim',
      help='Google Analytics User Id Custom Dimension')
    parser.add_value_provider_argument(
      '--google_analytics_buyer_custom_dim',
      help='Google Analytics Buyer Custom Dimension')
    parser.add_value_provider_argument(
      '--google_analytics_view_id',
      help='Google Analytics View Id')
    # Campaign Manager
    parser.add_value_provider_argument(
      '--dcm_profile_id',
      help='Campaign Manager Profile Id')
    parser.add_value_provider_argument(
      '--floodlight_activity_id',
      help='Floodlight Activity Id')
    parser.add_value_provider_argument(
      '--floodlight_configuration_id',
      help='Floodlight Configuration Id')
    # Conversion Plus
    parser.add_value_provider_argument(
      '--cp_sheet_id',
      help='Conversion Plus Sheet Id')
    parser.add_value_provider_argument(
      '--cp_sheet_range',
      help='Name of the Conversion Plus Sheet config range')
    parser.add_value_provider_argument(
      '--cp_conversion_name',
      help='Google Ads Conversion Name')
    # GCP
    parser.add_value_provider_argument(
      '--dataset_id',
      default='megalist',
      help='ID of BigQuery Dataset')
    parser.add_value_provider_argument(
      '--table_id',
      default='crm_upload',
      help='ID of BigQuery Table to read')
    parser.add_argument(
      '--gcp_project_id',
      help='ID Google Cloud Project to use')
    parser.add_argument(
      '--output',
      help='Output file to write results to.')
