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

from sources.batches_from_executions import BatchesFromExecutions, DataRowsGroupedBySourceCoder, _INT_MAX
from models.execution import AccountConfig, DataRow, DataRowsGroupedBySource, SourceType, DestinationType, TransactionalType, Execution, Source, Destination, ExecutionsGroupedBySource

from typing import List
import pytest

def _get_execution() -> Execution:
    return Execution(
        AccountConfig('', False, '', '', ''),
        Source(
            'source_name',
            SourceType.BIG_QUERY,
            []
        ),
        Destination(
            'destination_name',
            DestinationType.ADS_CUSTOMER_MATCH_CONTACT_INFO_UPLOAD,
            []
        )
    )

@pytest.fixture
def execution() -> Execution:
    return _get_execution()

@pytest.fixture
def executions_grouped_by_source() -> ExecutionsGroupedBySource:
    return ExecutionsGroupedBySource(
        'source_name',
        [_get_execution()]
    )

@pytest.fixture
def data_rows_grouped_by_source_coder() -> DataRowsGroupedBySourceCoder:
    return DataRowsGroupedBySourceCoder()

def test_data_rows_grouped_by_source_estimate_size_zero(mocker, data_rows_grouped_by_source_coder: DataRowsGroupedBySourceCoder, executions_grouped_by_source: ExecutionsGroupedBySource):
    data_rows: List[DataRow] = []
    o = DataRowsGroupedBySource(executions_grouped_by_source, data_rows)
    assert data_rows_grouped_by_source_coder.estimate_size(o) == 0

def test_data_rows_grouped_by_source_estimate_size_overflow(mocker, data_rows_grouped_by_source_coder: DataRowsGroupedBySourceCoder, executions_grouped_by_source: ExecutionsGroupedBySource):
    item: DataRow = DataRow({
        'phone': '5ecdb1fcdba73c56fc682fceb87166537e7d3990cbefcadb31ee23fe0add6322'
    })
    data_rows: List[DataRow] = [item for _ in range(100000000)]

    o = DataRowsGroupedBySource(executions_grouped_by_source, data_rows)
    assert data_rows_grouped_by_source_coder.estimate_size(o) == _INT_MAX

def test_batch_elements(mocker, execution):
    item: DataRow = DataRow({
        'phone': '5ecdb1fcdba73c56fc682fceb87166537e7d3990cbefcadb31ee23fe0add6322'
    })
    data_rows: List[DataRow] = [item for _ in range(11)]
    batch_elements = BatchesFromExecutions._BatchElements(2, None)
    grouped_elements = (execution, data_rows)
    amount_of_batches = 0
    for _ in batch_elements.process(grouped_elements):
        amount_of_batches = amount_of_batches + 1
    assert amount_of_batches == 6