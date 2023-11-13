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

from email.policy import default
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
    parser.add_value_provider_argument(
        '--bq_location',
        help='Location of Megalista operations (default us-central-1)')
    # Google Ads
    parser.add_value_provider_argument(
        '--developer_token', help='Developer Token for Google Ads API')
    # APPSFLYER
    parser.add_value_provider_argument(
        '--appsflyer_dev_key', help='Developer key for AppsFlyer S2S API')
    # AWS S3
    parser.add_value_provider_argument(
        '--aws_access_key_id', help='Access Key for S3 (AWS)')
    parser.add_value_provider_argument(
        '--aws_secret_access_key', help='Access Secret for S3 (AWS)')
    # ERRORS EMAIL NOTIFICATION
    parser.add_value_provider_argument(
      '--notify_errors_by_email', help='Should send errors by email. True or False')
    parser.add_value_provider_argument(
      '--errors_destination_emails', help='Emails for sending errors separated by comma')
    # DEBUG
    parser.add_value_provider_argument(
      '--show_code_lines_in_log', 
      default=False,
      help='Should show code lines in log messages. True or False')
    # DEBUG
    parser.add_value_provider_argument(
      '--collect_usage_stats', 
      default=True,
      help='Optin to collect usage stats. True or False. This helps us suporting the solution')