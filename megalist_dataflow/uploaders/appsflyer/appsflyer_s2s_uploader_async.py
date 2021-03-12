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

import logging

import apache_beam as beam
import time
from datetime import datetime
from typing import Any, List

import asyncio
from aiohttp import ClientSession, ClientTimeout

from uploaders import utils
from models.execution import DestinationType, Batch


class AppsFlyerS2SUploaderDoFn(beam.DoFn):
  def __init__(self, dev_key):
    super().__init__()
    self.API_URL = "https://api2.appsflyer.com/inappevent/"
    self.dev_key = dev_key
    self.app_id = None
    self.timeout = ClientTimeout(total=15) #15 sec timeout

  def start_bundle(self):
    pass


  async def _prepare_and_send(self, session, row, success_elements):

    #prepare payload
    payload = {
      "appsflyer_id": row['appsflyer_id'],
      "eventName": row['event_eventName'],
      "eventValue": "",
      "af_events_api" :"true"
    }
    self.bind_key(payload, row, 'device_ids_idfa','idfa')
    self.bind_key(payload, row, 'device_ids_advertising_id','advertising_id')
    self.bind_key(payload, row, 'device_ids_oaid','oaid')
    self.bind_key(payload, row, 'device_ids_amazon_aid','amazon_aid')
    self.bind_key(payload, row, 'device_ids_imei','imei')
    self.bind_key(payload, row, 'customer_user_id','customer_user_id')
    self.bind_key(payload, row, 'ip','ip')
    self.bind_key(payload, row, 'event_eventValue','eventValue')
    self.bind_key(payload, row, 'event_eventTime','eventTime')
    if 'eventTime' in payload:
      payload['eventTime'] = payload['eventTime'].strftime("%Y-%m-%d %H:%M:%S.%f")
    self.bind_key(payload, row, 'event_eventCurrency','eventCurrency')


    # run request asyncronously.
    response = await self._send_http_request(session, payload, 1)
    if response == 200:
      success_elements.append(row)
    return response


  async def _send_http_request(self, session, payload, curr_retry):
    url = self.API_URL + self.app_id
    headers = {
      "authentication": self.dev_key.get(),
      'Content-Type': 'application/json' 
    }

    try:
      async with session.post(url, headers=headers, json=payload,
      raise_for_status=False, timeout=15) as response:
        if response.status != 200:
          if curr_retry < 3:
            await asyncio.sleep(curr_retry)
            return await self._send_http_request(session, payload, curr_retry+1)
          else:
            logging.getLogger("megalista.AppsFlyerS2SUploader").error(
              f"Fail to send event. Response code: {response.status}, "
              f"reason: {response.reason}")
            #print(await response.text()) #uncomment to troubleshoot
        return response.status

    except Exception as exc:
      if curr_retry < 3:
        await asyncio.sleep(curr_retry)
        return await self._send_http_request(session, payload, curr_retry+1)
      else:
        logging.getLogger("megalista.AppsFlyerS2SUploader").error('Error inserting event: ' + str(exc))
        return -1


  async def _async_request_runner(self, elements, success_elements):
    tasks = []
    
    # Create client session to prevent multiple connections
    async with ClientSession(timeout=self.timeout) as session:

      # For each event
      for element in elements:
          task = asyncio.ensure_future(self._prepare_and_send(session, element, success_elements))
          tasks.append(task)

      responses = asyncio.gather(*tasks)
      return await responses


  def bind_key(self, payload, row, row_key, name):
     if row_key in row and row[row_key] is not None and row[row_key] != "":
      payload[name] = row[row_key]


  @utils.safe_process(logger=logging.getLogger("megalista.AppsFlyerS2SUploader"))
  def process(self, batch: Batch, **kwargs):
    success_elements: List[Any] = []
    start_datetime = datetime.now()
    execution = batch.execution
    
    self.app_id = execution.destination.destination_metadata[0]

    #send all requests asyncronously
    loop = asyncio.new_event_loop()
    future = asyncio.ensure_future(self._async_request_runner(batch.elements, success_elements), loop = loop)
    responses = loop.run_until_complete(future)


    #wait to avoid api trotle
    delta_sec = (datetime.now()-start_datetime).total_seconds()
    min_duration_sec = len(batch.elements)/500 #Using Rate limitation = 500 per sec
    if delta_sec < min_duration_sec:
      time.sleep(min_duration_sec - delta_sec)
    logging.getLogger("megalista.AppsFlyerS2SUploader").info(
      f"Successfully uploaded {len(success_elements)}/{len(batch.elements)} events.")

    yield Batch(execution, success_elements)
