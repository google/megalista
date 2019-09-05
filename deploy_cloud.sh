#!/bin/bash
cd megalist_dataflow
python3 -m main --runner DataflowRunner --project vuru-beacons-test --gcp_project_id vuru-beacons-test --temp_location gs://megalist-data/tmp/  --setup_file ./setup.py --template_location gs://megalist-data/templates/megalist
gsutil cp megalist_metadata gs://megalist-data/templates/megalist_metadata
cd ..
cd cloud_functions
cd is_new_buyer
gcloud functions deploy is_new_buyer --runtime python37 --trigger-http
cd ..
cd ..
cd examples
gsutil cp generate_megalist_token.py gs://megalist-data/generate_megalist_token.py
cd ..