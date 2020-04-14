from apache_beam import PTransform, DoFn

import apache_beam as beam

from sources.bq_api_dofn import BigQueryApiDoFn
from utils.execution import Action
from utils.group_by_execution_dofn import GroupByExecutionDoFn


def filter_by_action(execution, action):
  return execution.action is action


class FilterLoadAndGroupData(PTransform):
  """
  Filter the received executions by the received action,
  load the data using the received source and group by that batch size and Execution.
  """

  def __init__(
      self,
      action  # type: Action
  ):
    super().__init__()
    self._source_dofn = BigQueryApiDoFn()
    self._action = action

  def expand(self, input_or_inputs):
    # todo: rotear a source baseado no tipo presente na Execution
    return input_or_inputs | \
           beam.Filter(filter_by_action, self._action) | \
           beam.ParDo(self._source_dofn) | \
           beam.ParDo(GroupByExecutionDoFn())
