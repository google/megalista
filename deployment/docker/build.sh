#!/bin/bash
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

if [ $# != 1 ]; then
    echo "Usage: $0 container_tag"
    exit 1
fi

echo
echo "${bold}┌──────────────────────────────────┐${reset}"
echo "${bold}│      Megalista Deployment        │${reset}"
echo "${bold}└──────────────────────────────────┘${reset}"
echo
echo "${bold}${text_red}This is not an officially supported Google product.${reset}"
echo "${bold}Megalista docker image will be built with the following tag: ${text_green}$1${bold}${reset}"
echo "Build container"
docker build ../../ -t $1 -f Dockerfile
echo "${bold}${text_green}Finished. Image build: $1${reset}"
