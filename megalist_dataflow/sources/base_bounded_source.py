from abc import abstractmethod

from apache_beam.io import OffsetRangeTracker
from apache_beam.io import iobase
from apache_beam.io.iobase import RangeTracker
from apache_beam.io.iobase import SourceBundle

from typing import Any
from typing import Iterator
from typing import Optional


class BaseBoundedSource(iobase.BoundedSource):
  """
    Abstract class implementing common methods of BoundedSource applicable to a fixed size Source
  """

  def __init__(self):
    self._count = None

  def count(self):
    if self._count is None:
      self._count = self._do_count()
    return self._count

  @abstractmethod
  def _do_count(self):
    """
    :return: Size of source
    """
    raise NotImplementedError

  def split(self,
      desired_bundle_size,  # type: int
      start_position=None,  # type: Optional[Any]
      stop_position=None,  # type: Optional[Any]
  ):  # type: (...) -> Iterator[SourceBundle]
    if start_position is None:
      start_position = 0
    if stop_position is None:
      stop_position = self.count()

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
      stop_position = self.count()

    return OffsetRangeTracker(start_position, stop_position)
