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

from distutils.log import Log
import apache_beam as beam
from config import logging
from models.execution import Execution
from .megalista_step import MegalistaStep
from config.logging import LoggingConfig

class LastStep(MegalistaStep):
    def expand(self, executions):
        return (
            executions
            | beam.Flatten()
            | beam.CombineGlobally(CombineExecutionsFn())
            | beam.ParDo(PrintResultsDoFn())
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
    def process(self, executions):
        if logging.has_errors():
          logging.get_logger("megalista.LOG").error(f"SUMMARY OF ERRORS:\n{logging.get_formatted_error_list()}")
