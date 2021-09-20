import sys
sys.path.append('..') # Adds higher directory to python modules path.

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from third_party.uploaders.appsflyer.appsflyer_s2s_uploader_async import AppsFlyerS2SUploaderDoFn
from models.execution import DestinationType
from uploaders.big_query.transactional_events_results_writer import TransactionalEventsResultsWriter
from sources.batches_from_executions import BatchesFromExecutions

class AppsFlyerEventsStep(beam.PTransform):
    def __init__(self, params):
        self.params = params

    def expand(self, executions):
        return (
            executions
            | 'Load Data - AppsFlyer S2S events' >>
            BatchesFromExecutions(
                DestinationType.APPSFLYER_S2S_EVENTS,
                self.params.sheets_writer,
                1000,
                True,
                self.params.dataflow_options.bq_ops_dataset)
            | 'Upload - AppsFlyer S2S events' >>
            beam.ParDo(AppsFlyerS2SUploaderDoFn(self.params.dataflow_options.appsflyer_dev_key))
            | 'Persist results - AppsFlyer S2S events' >> beam.ParDo(TransactionalEventsResultsWriter(self.params.dataflow_options.bq_ops_dataset))
        )
