from apache_beam.io import OffsetRangeTracker
from apache_beam.io import iobase
from apache_beam.io.iobase import RangeTracker
from apache_beam.io.iobase import SourceBundle

from google.cloud import bigquery

from typing import Any
from typing import Iterator
from typing import Optional


def _get_max_results(start_position, stop_position, query_batch_size):
  positions_diff = stop_position - start_position
  if positions_diff < query_batch_size:
    return positions_diff
  return query_batch_size


class BigQueryApiSource(iobase.BoundedSource):
  """
  Source implemented using BigQuery's API.

  This source always reads all rows in a table.
  Each returned row can be accessed by column index (row[0]) or column name (row[column])
  """

  def __init__(self,
      dataset_name,  # type: str
      table_name,  # type: str
      query_batch_size=20000,  # type: int
  ):
    self._table_name = dataset_name + '.' + table_name
    self._query_batch_size = query_batch_size
    self._table_count = None

  def _get_table_count(self):
    if self._table_count is None:
      client = bigquery.Client()
      query_job = client.query('select count(*) from ' + self._table_name)
      for row in query_job:
        self._table_count = row[0]

    return self._table_count

  def split(self,
      desired_bundle_size,  # type: int
      start_position=None,  # type: Optional[Any]
      stop_position=None,  # type: Optional[Any]
  ):  # type: (...) -> Iterator[SourceBundle]
    if start_position is None:
      start_position = 0
    if stop_position is None:
      stop_position = self._get_table_count()

    bundle_start = start_position
    while bundle_start < stop_position:
      bundle_stop = min(stop_position, bundle_start + desired_bundle_size)
      yield iobase.SourceBundle(
          weight=(bundle_stop - bundle_start),
          source=self,
          start_position=bundle_start,
          stop_position=bundle_stop)
      bundle_start = bundle_stop

  def get_range_tracker(self,
      start_position,  # type: Optional[Any]
      stop_position,  # type: Optional[Any]
  ):  # type: (...) -> RangeTracker
    if start_position is None:
      start_position = 0
    if stop_position is None:
      stop_position = self._get_table_count()

    return OffsetRangeTracker(start_position, stop_position)

  def read(self, range_tracker):
    client = bigquery.Client()

    start_position = range_tracker.start_position()
    stop_position = range_tracker.stop_position()

    while start_position < stop_position:
      max_results = _get_max_results(start_position, stop_position, self._query_batch_size)
      print('---------- reading from position ' + str(start_position) + ' to ' + str(start_position + max_results))
      rows_iterator = client.list_rows(self._table_name, start_index=start_position, max_results=max_results)
      for row in rows_iterator:
        yield row
      start_position = start_position + max_results
