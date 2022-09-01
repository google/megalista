#!/bin/bash

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
echo "Update commit info inside code"
sed -i "s/MEGALISTA_VERSION\s*=.*/MEGALISTA_VERSION = '$(git rev-parse HEAD)'/" ../../megalista_dataflow/config/version.py
echo "Build container"
docker build ../../ -t $1 -f Dockerfile
echo "Cleanup"
sed -i "s/MEGALISTA_VERSION\s*=.*/MEGALISTA_VERSION = '\[megalista_version\]'/" ../../megalista_dataflow/config/version.py
echo "${bold}${text_green}Finished. Image build: $1${reset}"
