#!/bin/bash
ENV_VARS=`env | grep ^MEGALISTA_.*`
params=""

for var in $ENV_VARS;
do
    IFS='='
    read key value <<< "$var"
    key=`echo "${key}" | tr [:upper:] [:lower:]`
    params="${params} --${key:10} ${value}"
    IFS=' '
done

echo "Running Megalista"
python megalista_dataflow/main.py --runner DirectRunner --direct_num_workers 0 --direct_running_mode multi_threading ${params}