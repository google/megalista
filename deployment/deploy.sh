#!/bin/bash
# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

reset="$(tput sgr 0)"
bold="$(tput bold)"
text_red="$(tput setaf 1)"
text_yellow="$(tput setaf 3)"
text_green="$(tput setaf 2)"

function start_message() {
    echo "☑️  ${bold}${text_green}$1${reset}"
}

ask() {
    while true; do
    read -r -p "${BOLD}${1:-Continue?} : ${NOFORMAT}"
    case ${REPLY:0:1} in
        [yY]) return 0 ;;
        [nN]) return 1 ;;
        *) echo "Please answer yes or no."
    esac
    done
}

function enable_services() {
    start_message "Enabling services..."
    gcloud services enable sheets.googleapis.com
    gcloud services enable dataflow.googleapis.com
    gcloud services enable cloudscheduler.googleapis.com
    gcloud services enable analytics.googleapis.com
    gcloud services enable googleads.googleapis.com
    gcloud services enable dfareporting.googleapis.com
    echo
}

function create_bq_ops_dataset() {
    start_message "Creating dataset ${BQ_OPS_DATASET_NAME}..."
    bq --location=$LOCATION mk \
    --dataset \
    --description="Auxiliary BigQuery dataset for Megalista operations" \
    --label=name:$BQ_OPS_DATASET_NAME \
    $GOOGLE_CLOUD_PROJECT:$BQ_OPS_DATASET_NAME
    echo
}

function create_bucket() {
    start_message "Creating bucket ${BUCKET_NAME}..."
    gsutil mb -p $GOOGLE_CLOUD_PROJECT -l $LOCATION gs://$BUCKET_NAME
    gsutil uniformbucketlevelaccess set on gs://$BUCKET_NAME
    echo
}

function create_service_account() {
    start_message "Creating the service account ${SERVICE_ACCOUNT_NAME}..."
    gcloud iam service-accounts create $SERVICE_ACCOUNT_NAME \
    --display-name='Service Account for Megalista use'
    # Add Dataflow Worker role to the service account
    gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
    --member "serviceAccount:${SERVICE_ACCOUNT}" \
    --role "roles/dataflow.worker"
    # Add editor role to the service account
    gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
    --member "serviceAccount:${SERVICE_ACCOUNT}" \
    --role "roles/editor"
    # Add Storage Admin to the service account
    gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
    --member "serviceAccount:${SERVICE_ACCOUNT}" \
    --role "roles/storage.admin"
    # Add Scheduler Admin to the service account
    gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
    --member "serviceAccount:${SERVICE_ACCOUNT}" \
    --role "roles/cloudscheduler.admin"
    echo
}

function  create_scheduler() {
    start_message "Creating the Cloud Scheduler ${SCHEDULER_NAME}..."
    gcloud scheduler jobs create http $SCHEDULER_NAME \
    --schedule "0 0 * * *" \
    --uri "https://dataflow.googleapis.com/v1b3/projects/${GOOGLE_CLOUD_PROJECT}/locations/${LOCATION}/templates:launch?gcsPath=gs://${BUCKET_NAME}/templates/megalista" \
    --description "Daily Runner for Megalista" \
    --time-zone "Etc/UTC" \
    --location ${LOCATION} \
    --attempt-deadline 320s \
    --http-method POST \
    --oauth-service-account-email ${SERVICE_ACCOUNT} \
    --headers "Content-Type"="application/octet-stream" \
    --message-body "{
        \"jobName\": \"megalista_daily\",
        \"parameters\": {
            \"developer_token\": \"${DEVELOPER_TOKEN}\",
            \"client_id\": \"${CLIENT_ID}\",
            \"client_secret\": \"${CLIENT_SECRET}\",
            \"access_token\": \"${ACCESS_TOKEN}\",
            \"refresh_token\": \"${REFRESH_TOKEN}\",
            \"setup_sheet_id\": \"${SETUP_SHEET_ID}\",
            \"setup_json_url\": \"${SETUP_JSON_URL}\",
            \"bq_ops_dataset\": \"${BQ_OPS_DATASET_NAME}\",
            \"bq_location\": \"${LOCATION}\"
        },
        \"environment\": {
            \"tempLocation\": \"gs://${BUCKET_NAME}/tmp\"
        }
    }"
    echo
}

function grant_permissions_to_active_account() {
    active_account=$(gcloud config list account --format "value(core.account)")
    start_message "Granting permissions to the active account ${active_account}..."
    gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
    --member="user:${active_account}" \
    --role='roles/cloudscheduler.admin'
    echo
}

function init() {
    echo
    echo "${bold}┌──────────────────────────────────┐${reset}"
    echo "${bold}│      Megalista Deployment        │${reset}"
    echo "${bold}└──────────────────────────────────┘${reset}"
    echo
    echo "${bold}${text_red}This is not an officially supported Google product.${reset}"
    echo "${bold}Megalista will be deployed in the Google Cloud project ${text_green}${GOOGLE_CLOUD_PROJECT}${bold}${reset}"
    echo
    if [ -z "${CLOUD_SHELL}" ]; then
        echo "${bold}${text_yellow}WARNING! You are not running this script from the Google Cloud Shell environment.${reset}"
        echo
    fi
    if ask "Do you wish to proceed?"; then
        echo
        # Get Megalista parameters
        if [ -z "${GOOGLE_CLOUD_PROJECT}" ]; then
            GOOGLE_CLOUD_PROJECT="$(gcloud config get-value project)"
        fi
        SERVICE_ACCOUNT_NAME=$(cat config.json | jq -r '.service_account_name')
        SERVICE_ACCOUNT="${SERVICE_ACCOUNT_NAME}@${GOOGLE_CLOUD_PROJECT}.iam.gserviceaccount.com"
        BUCKET_NAME=$(cat config.json | jq -r '.bucket_name')
        SCHEDULER_NAME=$(cat config.json | jq -r '.scheduler_name')
        LOCATION=$(cat config.json | jq -r '.location')
        BQ_OPS_DATASET_NAME=$(cat config.json | jq -r '.bq_ops_dataset')
        DEVELOPER_TOKEN=$(cat config.json | jq -r '.developer_token')
        CLIENT_ID=$(cat config.json | jq -r '.client_id')
        CLIENT_SECRET=$(cat config.json | jq -r '.client_secret')
        ACCESS_TOKEN=$(cat config.json | jq -r '.access_token')
        REFRESH_TOKEN=$(cat config.json | jq -r '.refresh_token')
        SETUP_SHEET_ID=$(cat config.json | jq -r '.setup_sheet_id')
        SETUP_JSON_URL=$(cat config.json | jq -r '.setup_json_url')
        SETUP_FIRESTORE_COLLECTION=$(cat config.json | jq -r '.setup_firestore_collection')
        # Confirm details
        echo
        echo "${bold}${text_green}Settings${reset}"
        echo "${bold}${text_green}──────────────────────────────────────────${reset}"
        echo "${bold}${text_green}Project ID: ${GOOGLE_CLOUD_PROJECT}${reset}"
        echo "${bold}${text_green}Bucket: ${BUCKET_NAME}${reset}"
        echo "${bold}${text_green}Service Account: ${SERVICE_ACCOUNT}${reset}"
        echo "${bold}${text_green}Location: ${LOCATION}${reset}"
        echo
        if ask "Continue?"; then
            echo
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
            # Check that only 1 config has been provided
            if [ $configs -gt 1 ]; then
                echo
                echo ${text_red}"ERROR: Please provide only one configuration (Sheet, JSON or Firestore Collection)"${reset}
                echo
                exit
            fi
            # Resource Deployment
            enable_services
            grant_permissions_to_active_account
            EXISTING_BQ_OPS_DATASET=$(bq ls --filter labels."name:${BQ_OPS_DATASET_NAME}")
            echo 'Dataset' $EXISTING_BQ_OPS_DATASET
            if [ -z "${EXISTING_BQ_OPS_DATASET}" ]; then
               create_bq_ops_dataset
            else
                echo
                echo "${text_yellow}INFO: The dataset '${BQ_OPS_DATASET_NAME}' already exists."${reset}
                echo
            fi
            EXITING_BUCKET=$(gsutil ls -L -b gs://${BUCKET_NAME})
            if [ -z "${EXITING_BUCKET}" ]; then
                create_bucket
            else
                echo
                echo "${text_yellow}INFO: The bucket '${BUCKET_NAME}' already exists."${reset}
                echo
            fi
            EXISTING_SERVICE_ACCOUNT=$(gcloud iam service-accounts list --filter "email:${SERVICE_ACCOUNT_NAME}" --format="value(email)")
            if [ -z "${EXISTING_SERVICE_ACCOUNT}" ]; then
                create_service_account
            else
                echo
                echo "${text_yellow}INFO: Service account '${SERVICE_ACCOUNT_NAME}' already exists.${reset}"
                echo
            fi
            EXISTING_SCHEDULER=$(gcloud scheduler jobs list --location="${LOCATION}" --filter="${SCHEDULER_NAME}" --format="value(ID)")
            if [ -z "${EXISTING_SCHEDULER}" ]; then
                create_scheduler
            else
                echo
                echo "${text_yellow}INFO: Cloud Scheduler '${SCHEDULER_NAME}' already exists.${reset}"
                echo
            fi
            # Build metadata and copy it to Cloud Storage
            start_message "Building Dataflow metadata..."
            cd ..
            sh ./deployment/deploy_cloud.sh ${GOOGLE_CLOUD_PROJECT} ${BUCKET_NAME} ${LOCATION} ${SERVICE_ACCOUNT}
            echo

            echo "✅ ${bold}${text_green} Done!${reset}"
            echo
        fi
    fi
}

init