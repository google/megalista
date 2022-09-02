#!/bin/bash
echo "Environment variables handling"

ENV_VARS=`env | grep ^MEGALISTA_.*`
params=""

for var in $ENV_VARS;
do
    IFS='='
    read key value <<< "$var"
    key=`echo "${key}" | tr [:upper:] [:lower:]`
    if [[ $key == megalista_* ]]
    then
        params="${params} --${key:10} ${value}"
    fi
    IFS=' '
done

export GOOGLE_APPLICATION_CREDENTIALS=/app/megalista_dataflow/service-account-file.json

echo "Activating virual environment (python)"
source virtual_env/bin/activate
echo "Running Megalista"
python megalista_dataflow/main.py \
    --runner DirectRunner \
    --direct_num_workers 0 \
    --direct_running_mode multi_threading \
    ${params}