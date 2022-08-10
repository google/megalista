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
import errorhandler
import sys

import apache_beam as beam
from apache_beam import coders
from apache_beam.options.pipeline_options import PipelineOptions

from error.error_handling import ErrorHandler, ErrorNotifier, GmailNotifier

from models.execution import DataRowsGroupedBySource, Execution, ExecutionsGroupedBySource
from sources.batches_from_executions import ExecutionsGroupedBySourceCoder, DataRowsGroupedBySourceCoder, ExecutionCoder
from models.json_config import JsonConfig
from models.oauth_credentials import OAuthCredentials
from models.options import DataflowOptions
from models.sheets_config import SheetsConfig
from sources.primary_execution_source import PrimaryExecutionSource
from third_party import THIRD_PARTY_STEPS
from steps import PROCESSING_STEPS
from steps.megalista_step import MegalistaStepParams
from steps.load_executions_step import LoadExecutionsStep
from steps.last_step import LastStep


warnings.filterwarnings(
    "ignore", "Your application has authenticated using end user credentials"
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
        # Add load executions step
        executions = (pipeline 
            | "Load executions" >> LoadExecutionsStep(params, execution_source)
        )

        processing_results = []

        # Add execution steps
        for name, step in PROCESSING_STEPS:
            processing_results.append(executions | name >> step(params))

        # Add third party steps
        for name, step in THIRD_PARTY_STEPS:
            processing_results.append(executions | name >> step(params))
        # todo: update trix at the end

        tuple(processing_results) | LastStep(params)

if __name__ == "__main__":
    error_handler = errorhandler.ErrorHandler()
    stream_handler = logging.StreamHandler(stream=sys.stderr)
    logging.getLogger().setLevel(logging.ERROR)
    logging.getLogger("megalista").setLevel(logging.INFO)
    logging.getLogger().addHandler(stream_handler)
    run()

    if logging_handler.has_errors:
        logging.getLogger("megalista").critical('Completed with errors')
        raise SystemExit(1)
    
    logging.getLogger("megalista").info("Completed successfully!")
    exit(0)