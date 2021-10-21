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

import datetime
import logging
import pytz



MAX_RETRIES = 3

timezone = pytz.timezone('America/Sao_Paulo')


def get_ads_client(oauth_credentials, developer_token, login_customer_id):
    from google.ads.googleads.client import GoogleAdsClient
    from google.ads.googleads import oauth2

    oauth2_client = oauth2.get_installed_app_credentials(
        oauth_credentials.get_client_id(), oauth_credentials.get_client_secret(),
        oauth_credentials.get_refresh_token())

    return GoogleAdsClient(
        oauth2_client, developer_token,
        login_customer_id=login_customer_id)


def get_ads_service(service_name, version, oauth_credentials, developer_token,
                    login_customer_id):
    return get_ads_client(oauth_credentials, developer_token, login_customer_id).get_service(service_name, version=version)


def format_date(date):
    if isinstance(date, datetime.datetime):
        pdate = date
    else:
        pdate = datetime.datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%f')

    pdate = timezone.localize(pdate)
    str_timezone = pdate.strftime("%z")
    return f'{datetime.datetime.strftime(pdate, "%Y-%m-%d %H:%M:%S")}{str_timezone[-5:-2]}:{str_timezone[-2:]}'


def safe_process(logger):
    def deco(func):
        def inner(*args, **kwargs):
            batch = args[1]
            if not batch:
                logger.warning('Skipping upload, received no elements.')
                return
            logger.info(f'Uploading {len(batch.elements)} rows...')
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.error(f'Error uploading data for :{batch.elements}')
                logger.error(e, exc_info=True)
                logger.exception('Error uploading data.')

        return inner

    return deco


def safe_call_api(function, logger, *args, **kwargs):
    current_retry = 1
    _do_safe_call_api(function, logger, current_retry, *args, **kwargs)


def _do_safe_call_api(function, logger, current_retry, *args, **kwargs):
    try:
        return function(*args, *kwargs)
    except Exception as e:
        if current_retry < MAX_RETRIES:
            logger.exception(
                f'Fail number {current_retry}. Stack track follows. Trying again.')
            current_retry += 1
            return _do_safe_call_api(function, logger, current_retry, *args, **kwargs)


def convert_datetime_tz(dt, origin_tz, destination_tz):
    datetime_obj = pytz.timezone(origin_tz).localize(dt)
    return datetime_obj.astimezone(pytz.timezone(destination_tz))


def print_partial_error_messages(logger_name, action, response):
    partial_failure = getattr(response, 'partial_failure_error', None)
    if partial_failure is not None and partial_failure.message != '':
        message = f'Error on {action}: {partial_failure.message}.'
        logging.getLogger(logger_name).error(message)
    results = getattr(response, 'results', [])
    for result in results:
        message = f'gclid {result.gclid} uploaded.'
        logging.getLogger(logger_name).debug(message)