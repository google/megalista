#!/bin/bash

####

#Initialize the configuration for a Dataflow or a Dataflow Prime job

#Usage:
#Fill out the parameters in the config.json file and upload to cloud storage 
#Run ./run_dataflow.sh gs://{BUCKET_NAME}/config.json [dataflow_service_option] in the megalista_dataflow directory

#Args:
#gs://{BUCKET_NAME}/config.json: The gsutil uri of the config.json file.
#dataflow_service_option (optional): The Dataflow service option to use. If you are using Dataflow prime it should have the value:
#enable_prime: Enable Dataflow Prime.

###

reset="$(tput sgr 0)"
bold="$(tput bold)"
text_red="$(tput setaf 1)"
text_yellow="$(tput setaf 3)"
text_green="$(tput setaf 2)"


function show_message() {
    echo
    echo $1
    echo
}

function check_valid_parameters() {
    if [ -z "${BUCKET_NAME}" ] || [ -z "${LOCATION}" ] || [ -z "${REGION}" ] || [ -z "${BQ_OPS_DATASET_NAME}" ];then
        show_message "${bold}${text_red}ERROR: Some required parameters are missing. Please check your config.json file.${reset}"
        exit
    fi
    configs=0
    if [ ! -z "${SETUP_SHEET_ID}" ]; then
        configs=$((configs+1))
    fi
    if [ ! -z "${SETUP_JSON_URL}" ]; then
        configs=$((configs+1))
    fi
    if [ ! -z "${SETUP_FIRESTORE_COLLECTION}" ]; then
        configs=$((configs+1))
    fi
    # Check if no config has been provided
    if [ $configs -eq 0 ]; then
        show_message "${bold}${text_red}ERROR: Please provide at least one configuration (Sheet, JSON or Firestore Collection).${reset}"
        exit
    fi
    # Check that only 1 config has been provided
    if [ $configs -gt 1 ]; then
        show_message "${bold}${text_red}ERROR: Please provide only one configuration (Sheet, JSON or Firestore Collection).${reset}"
        exit
    fi
}

function init() {
    
    if [ -z "${GOOGLE_CLOUD_PROJECT}" ]; then
        GOOGLE_CLOUD_PROJECT="$(gcloud config get-value project)"
    fi
    BQ_OPS_DATASET_NAME="$(gcloud storage cat $1 | jq -r '.bq_ops_dataset')" 
    LOCATION="$(gcloud storage cat $1 | jq -r '.location')"
    GOOGLE_CLOUD_PROJECT="$(gcloud config get-value project)"
    REGION="$(gcloud storage cat $1 | jq -r '.region')"
    BUCKET_NAME="$(gcloud storage cat $1 | jq -r '.bucket_name')"
    DEVELOPER_TOKEN="$(gcloud storage cat $1 | jq -r '.developer_token')"
    CLIENT_ID="$(gcloud storage cat $1 | jq -r '.client_id')"
    CLIENT_SECRET="$(gcloud storage cat $1 | jq -r '.client_secret')"
    REFRESH_TOKEN="$(gcloud storage cat $1 | jq -r '.refresh_token')"
    ACCESS_TOKEN="$(gcloud storage cat $1 | jq -r '.access_token')"
    SETUP_SHEET_ID="$(gcloud storage cat $1 | jq -r '.setup_sheet_id')"
    SETUP_JSON_URL="$(gcloud storage cat $1 | jq -r '.setup_json_url')"
    SETUP_FIRESTORE_COLLECTION="$(gcloud storage cat $1 | jq -r '.setup_firestore_collection')"
    NOTIFY_ERRORS_BY_EMAIL="$(gcloud storage cat $1 | jq -r '.notify_errors_by_email')"
    ERRORS_DESTINATION_EMAILS="$(gcloud storage cat $1 | jq -r '.errors_destination_emails')"
    DATAFLOW_SERVICE_OPTION="$2"
    
    check_valid_parameters

    python3 main.py \
    --developer_token $DEVELOPER_TOKEN\
    --runner DirectRunner \
    --refresh_token $REFRESH_TOKEN \
    --setup_json_url $SETUP_JSON_URL \
    --access_token $ACCESS_TOKEN \
    --client_id $CLIENT_ID \
    --client_secret $CLIENT_SECRET \
    --notify_errors_by_email $NOTIFY_ERRORS_BY_EMAIL \
    --errors_destination_emails $ERRORS_DESTINATION_EMAILS\
    --bq_ops_dataset $BQ_OPS_DATASET_NAME \
    --bq_location $LOCATION\
    --project $GOOGLE_CLOUD_PROJECT \
    --region $REGION \
    --temp_location gs://$BUCKET_NAME/tmp\
    --dataflow_service_options=$DATAFLOW_SERVICE_OPTION
}

CONFIG_PATH=$1
if [[ -z "${CONFIG_PATH}" ]]; then
    show_message "${bold}${text_red}ERROR: the gsutil URI for the config.json file is required"
    exit
else
   init $CONFIG_PATH $2
fi