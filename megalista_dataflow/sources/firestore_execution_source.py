# Copyright 2021 Google LLC
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
import distutils.util
import logging

from apache_beam.options.value_provider import ValueProvider

from google.cloud import firestore
from sources.base_bounded_source import BaseBoundedSource
from models.execution import Destination, DestinationType
from models.execution import Execution, AccountConfig
from models.execution import Source, SourceType


class FirestoreExecutionSource(BaseBoundedSource):
  """
  Read Execution data from a Firestore collection. The collection name is set-up in the parameter "setup_firestore_collection"
  """

  def __init__(
      self,
      setup_firestore_collection: ValueProvider
  ):
    super().__init__()
    self._setup_firestore_collection = setup_firestore_collection

  def _do_count(self):
    collections = firestore.Client().collection(self._setup_firestore_collection.get()).get()
    return len(collections)

  def read(self, range_tracker):
    def document_to_dict(doc):
      if not doc.exists:
          return None
      doc_dict = doc.to_dict()
      doc_dict['id'] = doc.id
      return doc_dict

    firestore_collection = self._setup_firestore_collection.get()
    logging.getLogger("megalista.FirestoreExecutionSource").info(f"Loading Firestore collection {firestore_collection}...")
    db = firestore.Client()
    entries = db.collection(self._setup_firestore_collection.get()).where('active', '==', 'yes').stream()
    entries = [document_to_dict(doc) for doc in entries]
    
    account_data = document_to_dict(db.collection(self._setup_firestore_collection.get()).document('account_config').get())
  
    if not account_data:
      raise Exception('Firestore collection is absent')
    google_ads_id = account_data.get('google_ads_id', 'empty')
    mcc_trix = account_data.get('mcc_trix', 'FALSE')
    mcc = False if mcc_trix is None else bool(distutils.util.strtobool(mcc_trix))
    app_id = account_data.get('app_id', 'empty')
    google_analytics_account_id = account_data.get('google_analytics_account_id', 'empty')
    campaign_manager_account_id = account_data.get('campaign_manager_account_id', 'empty')
    
    account_config = AccountConfig(google_ads_id, mcc, google_analytics_account_id, campaign_manager_account_id, app_id)
    logging.getLogger("megalista.FirestoreExecutionSource").info(f"Loaded: {account_config}")
    
    sources = self._read_sources(entries)
    destinations = self._read_destination(entries)
    if entries:
      for entry in entries:
        if entry['active'].upper() == 'YES':
          logging.getLogger("megalista.FirestoreExecutionSource").info(
            f"Executing step Source:{sources[entry['source_name']].source_name} -> Destination:{destinations[entry['destination_name']].destination_name}")
          yield Execution(account_config, sources[entry['source_name']], destinations[entry['destination_name']])
    else:
      logging.getLogger("megalista.FirestoreExecutionSource").warn("No schedules found!")

  def _read_sources(self, entries):
    sources = {}
    if entries:
      for entry in entries:
        metadata = [entry['bq_dataset'], entry['bq_table']] #TODO: flexibilize for other source types
        source = Source(entry['source_name'], SourceType[entry['source']], metadata)
        sources[source.source_name] = source
    else:
      logging.getLogger("megalista.FirestoreExecutionSource").warn("No sources found!")
    return sources

  def _read_destination(self, entries):
    def create_metadata_list(entry):
      metadata_list = {
        'ADS_OFFLINE_CONVERSION': ['gads_conversion_name'],
        'ADS_ENHANCED_CONVERSION': ['gads_conversion_label', 'gads_conversion_tracking_id', 'gads_currency_code'],
        'ADS_SSD_UPLOAD': ['gads_conversion_name', 'gads_external_upload_id', 'gads_hash',
          'gads_currency_code', 'gads_account'],
        'ADS_CUSTOMER_MATCH_CONTACT_INFO_UPLOAD': ['gads_audience_name', 'gads_operation', 'gads_hash',
          'metadata_padding', 'gads_account'],
        'ADS_CUSTOMER_MATCH_MOBILE_DEVICE_ID_UPLOAD': ['gads_audience_name', 'gads_operation', 
          'metadata_padding', 'gads_app_id', 'gads_account'],
        'ADS_CUSTOMER_MATCH_USER_ID_UPLOAD': ['gads_audience_name', 'gads_operation', 'gads_hash', 
          'metadata_padding', 'gads_account'],
        'GA_MEASUREMENT_PROTOCOL': ['google_analytics_property_id', 'google_analytics_non_interaction'],
        'GA_DATA_IMPORT': ['google_analytics_property_id', 'google_analytics_data_import_name'],
        'GA_USER_LIST_UPLOAD': ['google_analytics_property_id', 'google_analytics_view_id',
          'google_analytics_data_import_name', 'google_analytics_user_id_list_name',
          'google_analytics_user_id_custom_dim', 'google_analytics_buyer_custom_dim'],
        'CM_OFFLINE_CONVERSION': ['campaign_manager_floodlight_activity_id',
          'campaign_manager_floodlight_configuration_id'],
        'APPSFLYER_S2S_EVENTS': ['appsflyer_app_id']
      }

      entry_type = entry['type']
      metadata = metadata_list.get(entry_type, None)
      if not metadata: 
        raise Exception(f'Upload type not implemented: {entry_type}')
      entry_metadata = []
      for m in metadata:
        # metadata_padding stands for the N/A fields in Sheets, preserving list indexes
        if m == 'metadata_padding':
          entry_metadata.append('N/A')
        elif m in entry:
          entry_metadata.append(entry[m])
        else:
          raise Exception(f'Missing field in Firestore document for {entry_type}: {m}')
      return entry_metadata


    destinations = {}
    if entries:
      for entry in entries:
        destination = Destination(entry['destination_name'], DestinationType[entry['type']], create_metadata_list(entry))
        destinations[destination.destination_name] = destination
    else:
      logging.getLogger("megalista.FirestoreExecutionSource").warn("No destinations found!")
    return destinations