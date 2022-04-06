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

from apache_beam.options.pipeline_options import PipelineOptions


class DataflowOptions(PipelineOptions):

  @classmethod
  def _add_argparse_args(cls, parser):
    # OAUTH
    parser.add_value_provider_argument(
        '--client_id', help='Client Id for the Google APIs')
    parser.add_value_provider_argument(
        '--client_secret', help='Client Secret for the Google APIs')
    parser.add_value_provider_argument(
        '--refresh_token', help='OAUTH Refresh Token for the Google APIs')
    parser.add_value_provider_argument(
        '--access_token', help='OAUTH Access Token for the Google APIs')
    # Set up
    parser.add_value_provider_argument(
        '--setup_sheet_id',
        help='Id of Spreadsheet with execution info (don\'t set if using JSON or Firestore)')
    parser.add_value_provider_argument(
        '--setup_json_url',
        help='URL of JSON file with execution info (don\'t set if using Sheet or Firestore)')
    parser.add_value_provider_argument(
        '--setup_firestore_collection', help='Name of Google Cloud Firestore collection with execution info (don\'t set if using Sheet or JSON)')
    parser.add_value_provider_argument(
        '--bq_ops_dataset',
        help='Auxliary bigquery dataset used for Megalista operations')
    # Google Ads
    parser.add_value_provider_argument(
        '--developer_token', help='Developer Token for Google Ads API')
    parser.add_value_provider_argument(
        '--customer_id', help='Google Ads Customer Id')
    # Google Analytics
    parser.add_value_provider_argument(
        '--google_analytics_account_id', help='Google Analytics account Id')
    parser.add_value_provider_argument(
        '--google_analytics_web_property_id',
        help='Google Analytics web property Id')
    parser.add_value_provider_argument(
        '--google_analytics_buyer_custom_dim',
        help='Google Analytics buyer custom dimension')
    parser.add_value_provider_argument(
        '--google_analytics_user_id_custom_dim',
        help='Google Analytics User Id custom dimension')
    # Campaign Manager
    parser.add_value_provider_argument(
        '--dcm_profile_id', help='CampaignManager profile Id')
    parser.add_value_provider_argument(
        '--floodlight_activity_id',
        help='CampaignManager floodlight activity Id')
    parser.add_value_provider_argument(
        '--floodlight_configuration_id',
        help='CampaignManager floodlight configuration Id')
    # Conversion Plus
    parser.add_value_provider_argument(
        '--cp_sheet_id', help='Conversion Plus Sheet Id')
    parser.add_value_provider_argument(
        '--cp_sheet_range',
        help='Name of the Conversion Plus Sheet config range')
    # BigQuery
    parser.add_value_provider_argument(
        '--dataset_id', default='megalist', help='BigQuery dataset Id')
    parser.add_value_provider_argument(
        '--table_id', default='crm_upload', help='BigQuery dataset Id')
    # GCP
    parser.add_argument(
        '--gcp_project_id', help='ID Google Cloud Project to use')
    parser.add_argument('--output', help='Output file to write results to.')
    # APPSFLYER
    parser.add_value_provider_argument(
        '--appsflyer_dev_key', help='Developer key for AppsFlyer S2S API')
    # S3 - AWS
    parser.add_value_provider_argument(
        '--aws_access_key_id', default=None, help='AWS S3 access key - id')
    parser.add_value_provider_argument(
        '--aws_secret_access_key', default=None, help='AWS S3 access key - secret')
    