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

from config.version import MEGALISTA_VERSION

from error.error_handling import GmailNotifier
from config.logging import LoggingConfig

from models.execution import DataRowsGroupedBySource, Execution, ExecutionsGroupedBySource
from sources.batches_from_executions import ExecutionsGroupedBySourceCoder, DataRowsGroupedBySourceCoder, ExecutionCoder
from models.json_config import JsonConfig
from models.oauth_credentials import OAuthCredentials
from models.options import DataflowOptions
from models.sheets_config import SheetsConfig
from sources.primary_execution_source import PrimaryExecutionSource
from steps.processing_steps import ProcessingStep
from steps.megalista_step import MegalistaStepParams
from steps.load_executions_step import LoadExecutionsStep
from steps.last_step import LastStep


warnings.filterwarnings(
    "ignore", "Your application has authenticated using end user credentials"
)

def run():
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
        dataflow_options.show_code_lines_in_log
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

        processing_results = executions | "Execute integrations" >> ProcessingStep(params)

        tuple(processing_results) | "Consolidate results" >> LastStep(params)

if __name__ == "__main__":
    run()

    logging_handler = LoggingConfig.get_logging_handler()
    if logging_handler is None:
        logging.getLogger("megalista").info(f"MEGALISTA build {MEGALISTA_VERSION}: Clould not find error interception handler. Skipping error intereception.")
    else:
        if logging_handler.has_errors:
            logging.getLogger("megalista").critical(f'MEGALISTA build {MEGALISTA_VERSION}: Completed with errors')
            raise SystemExit(1)
        else:
            logging.getLogger("megalista").info(f"MEGALISTA build {MEGALISTA_VERSION}: Completed successfully!")
    exit(0)
