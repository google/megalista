# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import io
import logging

from models.options import DataflowOptions
from google.cloud import storage
from google.oauth2.credentials import Credentials

_LOGGER_NAME = 'megalista.data_sources.FileProvider'

import boto3

class FileProvider:
  def __init__(self, path: str, dataflow_options: DataflowOptions):
    self._path = path
    self._dataflow_options = dataflow_options
    self._provider = self._define_file_provider()
    
  def read(self):
    return self._provider.read()

  def write(self, data):
    return self._provider.write(data)

  def _define_file_provider(self):
    file_provider = None
    if self._path.startswith('s3://'):
      #S3
      file_provider = self._S3FileProvider(self._path, self._dataflow_options)
      pass
    elif self._path.startswith('gs://') or self._path.startswith('https://'):
      #GCP Storage
      #- https is for keeping consistency with previous implementation of JSON Config.
      file_provider = self._GCSFileProvider(self._path, self._dataflow_options)
      pass
    elif self._path.startswith('file://') or not '://' in self._path:
      #Local File
      file_provider = self._LocalFileProvider(self._path)
    if file_provider is None:
      logging.getLogger(_LOGGER_NAME).error(f'Could not define File Provider. path="{self._path}"')
    return file_provider

  class _LocalFileProvider():
    def __init__(self, path: str):
      if path.startswith('file://'):
        path = path[7:]
      self._path = path

    def read(self):
      try:
        file = open(self._path, 'rb')
        data = file.read()
        file.close()
        return data
      except Exception as e:
        logging.getLogger(_LOGGER_NAME).info(f'Unable to read "{self._path}". {e}')
        return None
    
    def write(self, data):
      try:
        file = open(self._path, 'wb')
        file.write(data)
        file.close()
      except Exception as e:
        logging.getLogger(_LOGGER_NAME).error(f'Error on writing "{self._path}". {e}')

  class _S3FileProvider():
    def __init__(self, path: str, dataflow_options: DataflowOptions):
      self._path = path
      if path.startswith('s3://'):
        path = path[5:]
      bucket_name = path.split('/')[0]
      key = '/'.join(path.split('/')[1:])
      self._bucket_name = bucket_name
      self._key = key
      
      self._s3_client = None

      if 'aws_access_key_id' in dataflow_options.get_all_options():
        if dataflow_options.aws_access_key_id.get() != None:
          self._s3_client = boto3.client(
            's3',
            aws_access_key_id = dataflow_options.aws_access_key_id.get(),
            aws_secret_access_key = dataflow_options.aws_secret_access_key.get()
          )
        else:
          self._s3_client = boto3.client('s3')
      else:
        self._s3_client = boto3.client('s3')

      logging.getLogger(_LOGGER_NAME).info(f'S3 File Provider initiated. Bucket: "{bucket_name}". Key="{key}"')
        
    def read(self):
      try:
        response = self._s3_client.get_object(
          Bucket=self._bucket_name,
          Key=self._key
        )
        
        return response['Body'].read()
      except Exception as e:
        logging.getLogger(_LOGGER_NAME).info(f'Unable to read "{self._path}". {e}')
        return None
    
    def write(self, data):
      try:
        response = self._s3_client.put_object(
          Bucket=self._bucket_name,
          Key=self._key,
          Body=data
        )
      except Exception as e:
        logging.getLogger(_LOGGER_NAME).error(f'Error on writing "{self._path}". {e}')
        
  class _GCSFileProvider():
    def __init__(self, path: str, dataflow_options: DataflowOptions):
      self._path = path
      if path.startswith('gs://'):
        path = path[5:]
      elif path.startswith('https://'):
        path = path[8:]
      bucket_name = path.split('/')[0]
      file_path = '/'.join(path.split('/')[1:])
      self._bucket_name = bucket_name
      self._file_path = file_path
      logging.getLogger(_LOGGER_NAME).info(f'GCP Storage File Provider initiated. Bucket: "{bucket_name}". Path="{file_path}"')

      credentials = Credentials(
        token=dataflow_options.access_token.get(),
        refresh_token=dataflow_options.refresh_token.get(),
        client_id=dataflow_options.client_id.get(),
        client_secret=dataflow_options.client_secret.get(),
        token_uri='https://accounts.google.com/o/oauth2/token',
        scopes=['https://www.googleapis.com/auth/devstorage.read_write'])
      
      self._gcs_client = storage.Client(credentials=credentials)

    def read(self):
      try:
        bucket = self._gcs_client.get_bucket(self._bucket_name)
        blob = bucket.blob(self._file_path)
        return blob.download_as_bytes()
        
      except Exception as e:
        logging.getLogger(_LOGGER_NAME).info(f'Unable to read "{self._path}". {e}')
        return None
    
    def write(self, data):
      try:
        bucket = self._gcs_client.get_bucket(self._bucket_name)
        blob = bucket.blob(self._file_path)
        file = io.BytesIO(data)
        blob.upload_from_file(file)
        
      except Exception as e:
        logging.getLogger(_LOGGER_NAME).error(f'Error on writing "{self._path}". {e}')
        