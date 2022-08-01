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

from error.error_handling import ErrorNotifier

class MegalistaStepParams():
    def __init__(self, oauth_credentials, dataflow_options, error_notifier: ErrorNotifier):
        self._oauth_credentials = oauth_credentials
        self._dataflow_options = dataflow_options
        self._error_notifier = error_notifier

    @property
    def oauth_credentials(self):
        return self._oauth_credentials

    @property
    def dataflow_options(self):
        return self._dataflow_options

    @property
    def error_notifier(self):
        return self._error_notifier


class MegalistaStep(beam.PTransform):
    def __init__(self, params: MegalistaStepParams):
        self._params = params

    @property
    def params(self):
        return self._params

    def expand(self, executions):
        pass