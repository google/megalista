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
import re
from typing import List, Any

class BaseUtils:
    @staticmethod
    def filter_text_only_numbers(text: str) -> str:
        return re.sub(r'[^0-9]', '', text)

    @staticmethod
    def trim(text: str) -> str:
        return text.strip()

    @staticmethod
    def trim_items_array(array: List[Any]) -> List[Any]:
        return [BaseUtils.trim(item) if item is str else item for item in array]

class Utils(BaseUtils):
    pass