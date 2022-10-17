# Copyright 2022 Google LLC
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

FROM python:3.9-slim

# pre requirements
RUN apt-get update
RUN apt-get install -y curl build-essential

# Worker
RUN adduser --gecos [] --disabled-password worker

# copy files
WORKDIR /app
COPY --chown=worker:worker megalista_dataflow megalista_dataflow
COPY --chown=worker:worker deployment/docker/entrypoint.sh .
COPY --chown=worker:worker deployment/docker/service-account-file.json .

# pip
USER worker
RUN python -m ensurepip --upgrade
RUN pip install --upgrade pip setuptools wheel
RUN pip install --user --no-warn-script-location -r megalista_dataflow/requirements.txt

# set entrypoint
RUN ["chmod", "+x", "entrypoint.sh"]
ENTRYPOINT [ "/app/entrypoint.sh" ]

