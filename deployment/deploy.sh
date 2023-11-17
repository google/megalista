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
    echo "✓ ${bold}${text_green}$1${reset}"
    echo
}

function show_message() {
    echo
    echo $1
    echo
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
    start_message "Creating the new Cloud Scheduler ${SCHEDULER_NAME}..."
    gcloud scheduler jobs create http $SCHEDULER_NAME \
    --schedule "0 0 * * *" \
    --uri "https://dataflow.googleapis.com/v1b3/projects/${GOOGLE_CLOUD_PROJECT}/locations/${REGION}/templates:launch?gcsPath=gs://${BUCKET_NAME}/templates/megalista" \
    --description "Daily Runner for Megalista" \
    --time-zone "Etc/UTC" \
    --location ${REGION} \
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
            \"setup_firestore_collection\": \"${SETUP_FIRESTORE_COLLECTION}\",
            \"bq_ops_dataset\": \"${BQ_OPS_DATASET_NAME}\",
            \"bq_location\": \"${LOCATION}\"
        },
        \"environment\": {
            \"tempLocation\": \"gs://${BUCKET_NAME}/tmp\"
        }
    }"
    echo
}

function pause_scheduler() {
    start_message "Pausing scheduler $1..."
    gcloud scheduler jobs pause --location=$REGION $1
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

function check_valid_parameters() {
    if [ -z "${SERVICE_ACCOUNT_NAME}" ] || [ -z "${BUCKET_NAME}" ] || [ -z "${SCHEDULER_NAME_PREFIX}" ] || [ -z "${LOCATION}" ] || [ -z "${REGION}" ] || [ -z "${BQ_OPS_DATASET_NAME}" ] || [ -z "${CLIENT_ID}" ] || [ -z "${CLIENT_SECRET}" ] || [ -z "${ACCESS_TOKEN}" ] || [ -z "${REFRESH_TOKEN}" ]; then
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
    if [ $configs == 0 ]; then
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
    echo
    echo "${bold}┌──────────────────────────────────┐${reset}"
    echo "${bold}│      Megalista Deployment        │${reset}"
    echo "${bold}└──────────────────────────────────┘${reset}"
    show_message "${bold}${text_red}This is not an officially supported Google product.${reset}"
    show_message "${bold}Megalista will be deployed in the Google Cloud project ${text_green}${GOOGLE_CLOUD_PROJECT}${reset}"

    if [ -z "${CLOUD_SHELL}" ]; then
        show_message "${bold}${text_yellow}WARNING! You are not running this script from the Google Cloud Shell environment.${reset}"
    fi
    if ask "Do you wish to proceed?"; then
        echo
        # Get Megalista parameters
        if [ -z "${GOOGLE_CLOUD_PROJECT}" ]; then
            GOOGLE_CLOUD_PROJECT="$(gcloud config get-value project)"
        fi
        CURRENT_DATE=$(date +"%Y-%m-%d_%H-%M-%S")
        SERVICE_ACCOUNT_NAME=$(cat config.json | jq -r '.service_account_name')
        SERVICE_ACCOUNT="${SERVICE_ACCOUNT_NAME}@${GOOGLE_CLOUD_PROJECT}.iam.gserviceaccount.com"
        BUCKET_NAME=$(cat config.json | jq -r '.bucket_name')
        SCHEDULER_NAME_PREFIX=$(cat config.json | jq -r '.scheduler_name')
        SCHEDULER_NAME=$SCHEDULER_NAME_PREFIX"_"$CURRENT_DATE
        LOCATION=$(cat config.json | jq -r '.location')
        REGION=$(cat config.json | jq -r '.region')
        BQ_OPS_DATASET_NAME=$(cat config.json | jq -r '.bq_ops_dataset')
        DEVELOPER_TOKEN=$(cat config.json | jq -r '.developer_token')
        CLIENT_ID=$(cat config.json | jq -r '.client_id')
        CLIENT_SECRET=$(cat config.json | jq -r '.client_secret')
        ACCESS_TOKEN=$(cat config.json | jq -r '.access_token')
        REFRESH_TOKEN=$(cat config.json | jq -r '.refresh_token')
        SETUP_SHEET_ID=$(cat config.json | jq -r '.setup_sheet_id')
        SETUP_JSON_URL=$(cat config.json | jq -r '.setup_json_url')
        SETUP_FIRESTORE_COLLECTION=$(cat config.json | jq -r '.setup_firestore_collection')

        echo -n "Optin to collect crash/usage stats to improve the solution. This helps us suporting the solution (E.g. true/false):"
        read -r SHARE_CRASH_USAGE_STATS
        echo
        # Validate params before deployment
        check_valid_parameters
        # Confirm details
        echo
        echo "${bold}${text_green}Settings${reset}"
        echo "${bold}${text_green}──────────────────────────────────────────${reset}"
        echo "${bold}${text_green}Project ID: ${GOOGLE_CLOUD_PROJECT}${reset}"
        echo "${bold}${text_green}Service Account: ${SERVICE_ACCOUNT}${reset}"
        echo "${bold}${text_green}Cloud Storage Bucket: ${BUCKET_NAME}${reset}"
        echo "${bold}${text_green}Cloud Scheduler: ${SCHEDULER_NAME}${reset}"
        echo "${bold}${text_green}BigQuery Dataset: ${BQ_OPS_DATASET_NAME}${reset}"
        echo "${bold}${text_green}Location: ${LOCATION}${reset}"
        echo "${bold}${text_green}Region: ${REGION}${reset}"
        echo
        if ask "Do you want to continue?"; then
            echo
            # Resource Deployment
            enable_services
            grant_permissions_to_active_account
            # Check existing bucket first since other components depend directly on this
            EXITING_BUCKET=$(gsutil ls -b -p ${GOOGLE_CLOUD_PROJECT} gs://${BUCKET_NAME} 2>&1)
            if [[ $EXITING_BUCKET == *"BucketNotFoundException: 404"* ]]; then
                create_bucket
            elif [[ $EXITING_BUCKET == "gs://${BUCKET_NAME}/" ]]; then
                show_message "${text_yellow}WARNING: The bucket '${BUCKET_NAME}' already exists. Please verify that the bucket exists in this Google Cloud project '${GOOGLE_CLOUD_PROJECT}'.${reset}"
            elif [[ $EXITING_BUCKET == *"AccessDeniedException: 403"* ]]; then
                show_message "${bold}${text_red}ERROR: The bucket '${BUCKET_NAME}' already exists and you don't have access to it. Try another name and execute the deployment script again.${reset}"
                exit
            else
                # Show any other message
                show_message "${text_yellow}Something unexpected happened during the Cloud Bucket creation: $EXITING_BUCKET. Please address the issue and execute the deployment script again.${reset}"
                exit
            fi
            EXISTING_BQ_OPS_DATASET=$(bq ls ${GOOGLE_CLOUD_PROJECT}:${BQ_OPS_DATASET_NAME} 2>&1)
            if [[ $EXISTING_BQ_OPS_DATASET == *"Not found"* ]]; then
               create_bq_ops_dataset
            else
                show_message "${text_yellow}WARNING: Skipping BigQuery dataset creation. The dataset '${BQ_OPS_DATASET_NAME}' already exists.${reset}"
            fi
            EXISTING_SERVICE_ACCOUNT=$(gcloud iam service-accounts list --filter "email:${SERVICE_ACCOUNT_NAME}" --format="value(email)")
            if [ -z "${EXISTING_SERVICE_ACCOUNT}" ]; then
                create_service_account
            else
                show_message "${text_yellow}WARNING: Skipping service account creation. Service account '${SERVICE_ACCOUNT_NAME}' already exists.${reset}"
            fi
            EXISTING_SCHEDULERS=$(gcloud scheduler jobs list --location="${REGION}" --filter="${SCHEDULER_NAME_PREFIX}" --format="value(ID)")
            if [ ! -z "${EXISTING_SCHEDULERS}" ]; then
                show_message "Some Cloud Schedulers with the prefix '${SCHEDULER_NAME_PREFIX}' already exist. Pausing existing Cloud Schedulers..."
                show_message "${text_yellow}WARNING: If the scheduler name or region changed, please make sure to disable the existing schedulers directly in the UI.${reset}"
                for scheduler in $(echo $EXISTING_SCHEDULERS | tr " " "\n")
                do
                    pause_scheduler $scheduler
                done
            fi
            # Create a new version of the Cloud Scheduler appending the timestamp
            #create_scheduler
            # Build metadata and copy it to Cloud Storage
            start_message "Building Dataflow metadata..."
            cd ..
            sh ./deployment/deploy_cloud.sh ${GOOGLE_CLOUD_PROJECT} ${BUCKET_NAME} ${REGION} ${SERVICE_ACCOUNT} ${SHARE_CRASH_USAGE_STATS}
            echo

            echo "✅ ${bold}${text_green} Done!${reset}"
            echo
        fi
    fi
}

init