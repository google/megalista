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

from .megalista_step import MegalistaStep
from mappers.executions_grouped_by_source_mapper import ExecutionsGroupedBySourceMapper, ExecutionsGroupedBySourceCombineFn

EXECUTIONS_MAPPER = ExecutionsGroupedBySourceMapper()

class LoadExecutionsStep(MegalistaStep):
    def __init__(self, params, execution_source):
        super().__init__(params)
        self._execution_source = execution_source

    def expand(self, pipeline):
        return (pipeline 
            | "Read config" >> beam.io.Read(self._execution_source)
            | "Transform into tuples" >> beam.Map(lambda execution: (execution.source.source_name, execution))
            | "Group by source name" >> beam.CombinePerKey(ExecutionsGroupedBySourceCombineFn())
            | "Encapsulate into object" >> beam.Map(ExecutionsGroupedBySourceMapper().encapsulate)
        )