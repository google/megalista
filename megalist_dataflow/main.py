# Copyright 2019 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import math

import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.io.gcp.datastore.v1new.datastoreio import WriteToDatastore
from apache_beam.options.pipeline_options import PipelineOptions
from utils.options import DataflowOptions
from utils.oauth_credentials import OAuthCredentials
from mappers.pii_hashing_mapper import PIIHashingMapper
from mappers.datastore_entity_mapper import DatastoreEntityMapper
from reducers.bloom_filter_reducer import BloomFilterReducer
from uploaders.google_ads_user_list_uploader import GoogleAdsUserListUploaderDoFn
from uploaders.google_analytics_user_list_uploader import GoogleAnalyticsUserListUploaderDoFn
from uploaders.campaign_manager_conversion_uploader import CampaignManagerConversionUploaderDoFn


def run(argv=None):
    pipeline_options = PipelineOptions()
    dataflow_options = pipeline_options.view_as(DataflowOptions)
    hasher = PIIHashingMapper()
    datastore_mapper = DatastoreEntityMapper(
        dataflow_options.gcp_project_id, 1024000)
    oauth_credentials = OAuthCredentials(dataflow_options.client_id, dataflow_options.client_secret,
                                         dataflow_options.developer_token, dataflow_options.refresh_token)
    with beam.Pipeline(options=pipeline_options) as pipeline:
        users = (pipeline
                 | 'Read Table' >> beam.io.Read(beam.io.BigQuerySource('megalist.buyers')))

        batched_users = (users 
                         | 'Batch Users' >> beam.util.BatchElements(min_batch_size=5000, max_batch_size=5000))

        google_ads = (batched_users
                        | 'Hash Users' >> beam.Map(hasher.hash_users)
                        | 'Upload to Ads' >> beam.ParDo(GoogleAdsUserListUploaderDoFn(oauth_credentials, dataflow_options.developer_token, dataflow_options.customer_id)))

        google_analytics = (batched_users
                            | 'Upload to Analytics' >> beam.ParDo(GoogleAnalyticsUserListUploaderDoFn(oauth_credentials, dataflow_options.google_analytics_account_id, dataflow_options.google_analytics_web_property_id, dataflow_options.customer_id, dataflow_options.google_analytics_user_id_custom_dim, dataflow_options.google_analytics_buyer_custom_dim)))

        campaign_manager = (users
                            | beam.util.BatchElements(min_batch_size=1000, max_batch_size=1000)
                            | 'Upload to CampaignManager' >> beam.ParDo(CampaignManagerConversionUploaderDoFn(oauth_credentials, dataflow_options.dcm_profile_id, dataflow_options.floodlight_activity_id, dataflow_options.floodlight_configuration_id)))

        containing_set = (batched_users
                            | 'Bloom Filter Apply' >> beam.CombineGlobally(BloomFilterReducer(50000000))
                            | 'Transform to Datastore entities' >> beam.FlatMap(datastore_mapper.batch_entities)
                            | 'Write to Datastore' >> WriteToDatastore(dataflow_options.gcp_project_id))

        result = pipeline.run()
        result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
