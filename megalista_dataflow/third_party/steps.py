import sys

from error.error_handling import ErrorHandler

sys.path.append('..') # Adds higher directory to python modules path.

import apache_beam as beam

from third_party.uploaders.appsflyer.appsflyer_s2s_uploader_async import AppsFlyerS2SUploaderDoFn
from models.execution import DestinationType
from uploaders.support.transactional_events_results_writer import TransactionalEventsResultsWriter
from sources.batches_from_executions import BatchesFromExecutions, TransactionalType


class AppsFlyerEventsStep(beam.PTransform):
    def __init__(self, params):
        self.params = params

    def expand(self, executions):
        return (
            executions
            | 'Load Data - AppsFlyer S2S events' >>
            BatchesFromExecutions(
                self.params._dataflow_options,
                DestinationType.APPSFLYER_S2S_EVENTS,
                1000,
                TransactionalType.UUID)
            | 'Upload - AppsFlyer S2S events' >>
            beam.ParDo(AppsFlyerS2SUploaderDoFn(self.params.dataflow_options.appsflyer_dev_key,
                                                ErrorHandler(DestinationType.APPSFLYER_S2S_EVENTS,
                                                             self.params.error_notifier)))
            | 'Persist results - AppsFlyer S2S events' >> beam.ParDo(
                            TransactionalEventsResultsWriter(
                              self.params.dataflow_options,
                              TransactionalType.UUID))
        )
