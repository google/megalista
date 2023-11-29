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

class OAuthCredentials():
    def __init__(self, client_id, client_secret, access_token, refresh_token):
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = access_token
        self.refresh_token = refresh_token

    def get_client_id(self):
        logging.getLogger("megalista").info(
            f"[PETLOVE] client_id: {self.client_id.get()} type: {type(self.client_id.get())}"
        )
        return self.client_id.get()

    def get_client_secret(self):
        logging.getLogger("megalista").info(
            f"[PETLOVE] client_secret: {self.client_secret.get()} type: {type(self.client_secret.get())}"
        )
        return self.client_secret.get()

    def get_access_token(self):
        logging.getLogger("megalista").info(
            f"[PETLOVE] access_token: {self.access_token.get()} type: {type(self.access_token.get())}"
        )
        return self.access_token.get()

    def get_refresh_token(self):
        logging.getLogger("megalista").info(
            f"[PETLOVE] refresh_token: {self.refresh_token.get()} type: {type(self.refresh_token.get())}"
        )
        return self.refresh_token.get()
