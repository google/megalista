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

from unittest.mock import MagicMock
from steps.megalista_step import MegalistaStepParams
from steps.processing_steps import PROCESSING_STEPS, ProcessingStep
from third_party import THIRD_PARTY_STEPS

from models.options import DataflowOptions
from models.json_config import JsonConfig
from sources.json_execution_source import JsonExecutionSource
from models.execution import AccountConfig, DataRow, DataRowsGroupedBySource, SourceType, DestinationType, TransactionalType, Execution, Source, Destination, ExecutionsGroupedBySource
from apache_beam.options.value_provider import StaticValueProvider


from typing import List, Dict
import pytest
from pytest_mock import MockFixture

def processing_step_expand_test():
    params: MegalistaStepParams = MegalistaStepParams('', DataflowOptions(), None)
    step: ProcessingStep  = ProcessingStep(params)
    executions: List[Execution] = list()
    processing_results = step.expand(executions)

    assert len(processing_results) == len(PROCESSING_STEPS) + len(THIRD_PARTY_STEPS)