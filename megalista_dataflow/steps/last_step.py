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

import datetime
import apache_beam as beam
import logging
from error.logging_handler import LoggingHandler
from config.version import MEGALISTA_VERSION
from models.execution import Execution
from .megalista_step import MegalistaStep
from config.logging import LoggingConfig
from steps.megalista_step import MegalistaStepParams


class LastStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | beam.Flatten()
            | beam.CombineGlobally(CombineExecutionsFn())
            | beam.ParDo(PrintResultsDoFn(self._params))
        )


class CombineExecutionsFn(beam.CombineFn):
    def create_accumulator(self):
        return {}

    def add_input(self, accumulator, input: Execution):
        key = f"{input.source.source_name} -> {input.destination.destination_name}"
        if key not in accumulator:
            accumulator[key] = input
        return accumulator

    def merge_accumulators(self, accumulators):
        merged = {}
        for accum in accumulators:
            for item in accum.items():
                key = item[0]
                if key not in merged:
                    merged[key] = item[1]
        return merged

    def extract_output(self, accumulator):
        return accumulator


class PrintResultsDoFn(beam.DoFn):
    def __init__(self, params: MegalistaStepParams):
        self._params = params

    def check_stats(self, statistics):
        try:
            if (
                self._params._dataflow_options.collect_usage_stats != "False"
                and len(statistics) > 0
            ):
                from tadau.measurement_protocol import Tadau

                Tadau().process(
                    [
                        {
                            "client_id": f"{int(datetime.datetime.now().timestamp()*10e3)}",
                            "name": "Megalista",
                            "version": f"{MEGALISTA_VERSION}",
                            **stat,
                        }
                        for stat in statistics
                    ]
                )

        except:
            pass

    def process(self, executions):
        logging_handler = LoggingConfig.get_logging_handler()

        if logging_handler is None:
            logging.getLogger("megalista").info(
                f"Clould not find error interception handler. Skipping error intereception."
            )
        else:
            if logging_handler.has_errors:
                logging.getLogger("megalista.LOG").error(
                    f"SUMMARY OF ERRORS:\n{LoggingHandler.format_records(logging_handler.error_records)}"
                )

        # Runs stats silently
        try:
            self.check_stats(
                [
                    {
                        "action": "ran",
                        "solution": executed._destination._destination_type.name,
                        "target": executed._destination._destination_metadata[0],
                        "ads": executed._account_config.google_ads_account_id,
                        "cm": executed._account_config.campaign_manager_profile_id,
                        "ga": executed._account_config.google_analytics_account_id,
                    }
                    for executed in executions.values()
                ]
            )
            if logging_handler:
                if logging_handler.has_errors:
                    self.check_stats(
                        [
                            {
                                "action": "error",
                                "solution": logging_handler.error_records[i].name,
                                "message": logging_handler.error_records[i].message[
                                    :500
                                ],
                            }
                            for i in range(len(logging_handler.error_records))
                        ]
                    )
        except:
            pass
