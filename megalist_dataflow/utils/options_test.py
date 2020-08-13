# Copyright 2019 Google LLC
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

from megalist_dataflow.utils.options import DataflowOptions
from apache_beam.options.pipeline_options import PipelineOptions


def test_options(mocker):
    parser = mocker.MagicMock()
    DataflowOptions._add_argparse_args(parser)
    parser.add_value_provider_argument.assert_any_call("--client_id", help=mocker.ANY)
    parser.add_value_provider_argument.assert_any_call("--client_secret", help=mocker.ANY)
    parser.add_value_provider_argument.assert_any_call("--refresh_token", help=mocker.ANY)
    parser.add_value_provider_argument.assert_any_call("--access_token", help=mocker.ANY)
    parser.add_value_provider_argument.assert_any_call("--developer_token", help=mocker.ANY)
    parser.add_value_provider_argument.assert_any_call("--customer_id", help=mocker.ANY)
    parser.add_value_provider_argument.assert_any_call("--google_analytics_account_id", help=mocker.ANY)
    parser.add_value_provider_argument.assert_any_call("--google_analytics_web_property_id", help=mocker.ANY)
    parser.add_value_provider_argument.assert_any_call("--google_analytics_buyer_custom_dim", help=mocker.ANY)
    parser.add_value_provider_argument.assert_any_call("--google_analytics_user_id_custom_dim", help=mocker.ANY)
    parser.add_value_provider_argument.assert_any_call("--dcm_profile_id", help=mocker.ANY)
    parser.add_value_provider_argument.assert_any_call("--floodlight_activity_id", help=mocker.ANY)
    parser.add_value_provider_argument.assert_any_call("--floodlight_configuration_id", help=mocker.ANY)
    parser.add_value_provider_argument.assert_any_call("--dataset_id", default="megalist", help=mocker.ANY)
    parser.add_value_provider_argument.assert_any_call("--table_id", default="crm_upload", help=mocker.ANY)
    parser.add_argument.assert_any_call("--gcp_project_id", help=mocker.ANY)
    parser.add_argument.assert_any_call("--output", help=mocker.ANY)
