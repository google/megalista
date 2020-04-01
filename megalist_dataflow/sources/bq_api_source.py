from google.cloud import bigquery

from sources.base_bounded_source import BaseBoundedSource


def _get_max_results(start_position, stop_position, query_batch_size):
  positions_diff = stop_position - start_position
  if positions_diff < query_batch_size:
    return positions_diff
  return query_batch_size


class BigQueryApiSource(BaseBoundedSource):
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
    super().__init__()

  def _do_count(self):
    client = bigquery.Client()
    query_job = client.query('select count(*) from ' + self._table_name)
    for row in query_job:
      return row[0]

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
