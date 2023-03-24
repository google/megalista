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

"""
This module is responsible for handling file operatios. As of now, it handles the following scenarios:
- Files located in the same location as the code (filesystem)
- Google Cloud Storage
- Amazon S3
"""

import io
from config import logging
from os.path import exists
from urllib.parse import ParseResultBytes

from models.options import DataflowOptions
from models.execution import SourceType
from google.cloud import storage
from google.oauth2.credentials import Credentials

import boto3
import botocore

_LOGGER_NAME = 'megalista.data_sources.FileProvider'


class FileProvider:
  def __init__(self, path: str, dataflow_options: DataflowOptions, source_type: SourceType, source_name: str, can_skip_read: bool):
    self._path = path
    self._dataflow_options = dataflow_options
    self._source_type = source_type
    self._source_name = source_name
    self._can_skip_read = can_skip_read

    self._provider = self._define_file_provider()

  def read(self):
    return self._provider.read()

  def write(self, data):
    return self._provider.write(data)

  def _define_file_provider(self):
    file_provider = None
    if self._path.startswith('s3://'):
      #S3
      return self._S3FileProvider(self._path, self._dataflow_options, self._can_skip_read, self._source_name)
    elif self._path.startswith('gs://') or self._path.startswith('https://'):
      #GCP Storage
      #- https is for keeping consistency with previous implementation of JSON Config.
      return self._GCSFileProvider(self._path, self._dataflow_options, self._can_skip_read, self._source_name)
    elif self._path.startswith('file://') or not '://' in self._path:
      #Local File
      return self._LocalFileProvider(self._path, self._dataflow_options, self._can_skip_read, self._source_name)
    raise NotImplementedError(f'Could not define File Provider. Path="{self._path}". Source="{self._source_name}"')

  class _BaseFileProvider:
    def __init__(self, path: str, dataflow_options: DataflowOptions, can_skip_read: bool, source_name: str):
      self._path = path
      self._dataflow_options = dataflow_options
      self._can_skip_read = can_skip_read
      self._source_name = source_name

  class _LocalFileProvider(_BaseFileProvider):
    def __init__(self, path: str, dataflow_options: DataflowOptions, can_skip_read: bool, source_name: str):
      super().__init__(path, dataflow_options, can_skip_read, source_name)
      if path.startswith('file://'):
        path = path[7:]
      self._cleaned_path = path

    def read(self):
      if exists(self._cleaned_path):
        file = open(self._cleaned_path, 'rb')
        data = file.read()
        file.close()
        return data
      elif self._can_skip_read:
        return b''
      else:
        raise FileNotFoundError(f'Could not find file. Path="{self._path}". Source="{self._source_name}"')

    
    def write(self, data):
      file = open(self._path, 'wb')
      file.write(data)
      file.close()

  class _S3FileProvider(_BaseFileProvider):
    def __init__(self, path: str, dataflow_options: DataflowOptions, can_skip_read: bool, source_name: str):
      super().__init__(path, dataflow_options, can_skip_read, source_name)
      if path.startswith('s3://'):
        path = path[5:]
      bucket_name = path.split('/')[0]
      key = '/'.join(path.split('/')[1:])
      self._bucket_name = bucket_name
      self._key = key
      
      self._s3_client = None
      self._s3_resource = None

      if 'aws_access_key_id' in dataflow_options.get_all_options():
        if dataflow_options.aws_access_key_id.get() != None:
          self._s3_client = boto3.client(
            's3',
            aws_access_key_id = dataflow_options.aws_access_key_id.get(),
            aws_secret_access_key = dataflow_options.aws_secret_access_key.get()
          )
          self._s3_resource = boto3.resource(
            's3',
            aws_access_key_id = dataflow_options.aws_access_key_id.get(),
            aws_secret_access_key = dataflow_options.aws_secret_access_key.get()
          )
        else:
          self._s3_client = boto3.client('s3')
          self._s3_resource = boto3.resource('s3')
      else:
        self._s3_client = boto3.client('s3')
        self._s3_resource = boto3.resource('s3')

      logging.getLogger(_LOGGER_NAME).info(f'S3 File Provider initiated. Bucket: "{bucket_name}". Key="{key}"')
        
    def read(self):
      try:
        self._s3_resource.Object(self._bucket_name, self._key).load()
      except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
          if self._can_skip_read:
            return b''
          else:
            raise FileNotFoundError(f'Could not find file. Path="{self._path}". Source="{self._source_name}"')
        else:
          raise FileNotFoundError(f'Could not find file. Path="{self._path}". Source="{self._source_name}"')
      else:
        response = self._s3_client.get_object(
          Bucket=self._bucket_name,
          Key=self._key
        )
        return response['Body'].read()

    def write(self, data):
      response = self._s3_client.put_object(
        Bucket=self._bucket_name,
        Key=self._key,
        Body=data
      )
        
  class _GCSFileProvider(_BaseFileProvider):
    def __init__(self, path: str, dataflow_options: DataflowOptions, can_skip_read: bool, source_name: str):
      super().__init__(path, dataflow_options, can_skip_read, source_name)
      if path.startswith('gs://'):
        path = path[5:]
      elif path.startswith('https://'):
        path = path[8:]
      bucket_name = path.split('/')[0]
      file_path = '/'.join(path.split('/')[1:])
      self._bucket_name = bucket_name
      self._file_path = file_path
      logging.getLogger(_LOGGER_NAME).info(f'GCP Storage File Provider initiated. Bucket: "{bucket_name}". Path="{file_path}"')
      
      self._gcs_client = storage.Client()

    def read(self):
      bucket = self._gcs_client.get_bucket(self._bucket_name)
      blob = bucket.blob(self._file_path)
      if blob.exists():
        return blob.download_as_bytes()
      elif self._can_skip_read:
        return b''
      else:
        raise FileNotFoundError(f'Could not find file. Path="{self._path}". Source="{self._source_name}"')

    def write(self, data):
      bucket = self._gcs_client.get_bucket(self._bucket_name)
      blob = bucket.blob(self._file_path)
      file = io.BytesIO(data)
      blob.upload_from_file(file)
