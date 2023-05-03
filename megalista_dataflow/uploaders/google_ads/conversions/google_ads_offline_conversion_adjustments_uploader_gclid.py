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
from typing import Dict, Any, List

from uploaders import utils
from uploaders.google_ads.conversions.google_ads_offline_conversion_adjustments_uploader import GoogleAdsOfflineAdjustmentUploaderDoFn
from models.execution import Batch, DestinationType, AccountConfig


class GoogleAdsOfflineAdjustmentGclidUploaderDoFn(
    GoogleAdsOfflineAdjustmentUploaderDoFn):

  def populate_adjustments(self, rows:List[Dict[str, Any]], conversion_resource_name: str, adjustment_type: str) -> Dict[str, Any]:

    conversion_adjustments = [{
       'adjustment_type': adjustment_type,
       'restatement_value': {
         'adjusted_value': float(str(conversion['amount'])) if adjustment_type == 'RESTATEMENT' else None,
         'currency_code': None,  # defaults to account currency
        },
        'conversion_action': conversion_resource_name,
        'adjustment_date_time': utils.format_date(conversion['time']),
        'gclid_date_time_pair': {
                    'gclid': conversion['gclid'],
                    'conversion_date_time': utils.format_date(conversion['conversion_time']),
                }
        } for conversion in rows]
    return conversion_adjustments
    
  def _get_new_batch_with_successfully_uploaded_elements(self, batch: Batch, response):
    def gclid_lambda(result):
        return result.gclid_date_time_pair.gclid
    
    successful_gclids = list(
        map(gclid_lambda, filter(gclid_lambda, response.results))
    )

    successful_elements = list(
        filter(
            lambda element: element['gclid'] in successful_gclids, batch.elements
        )
    )
    return Batch(batch.execution, successful_elements)
  
