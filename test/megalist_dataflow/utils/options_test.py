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
    pipeline_options = PipelineOptions()
    mocker.spy(DataflowOptions, '_add_argparse_args')
    pipeline_options.view_as(DataflowOptions)
    assert DataflowOptions._add_argparse_args.call_count == 1
