# Megalista

Sample integration code for onboarding offline/CRM data from BigQuery as custom audiences or offline conversions in Google Ads, Google Analytics 360, Google Display & Video 360 and Google Campaign Manager.

**Disclaimer:** This is not an officially supported Google product.

## Supported integrations
- **Google Ads**
  - Contact Info **Customer Match** (email, phone, address) [[details]](https://support.google.com/google-ads/answer/6379332?&ref_topic=6296507)
  - Id Based **Customer Match** (device Id, user id)
  - Offline Conversions through **gclid** [[details]](https://support.google.com/google-ads/answer/2998031?)
  - Store Sales Direct **(SSD)** conversions [[details]](https://support.google.com/google-ads/answer/9995886?hl=en)

- **Google Analytics (Universal analytics)**
  - Custom segments through **Data Import** [[details]](https://support.google.com/analytics/answer/3191589?hl=en)
  - Measurement Protocol [[details]](https://developers.google.com/analytics/devguides/collection/protocol/v1#:~:text=Measurement%20Protocol%20Overview%20bookmark_border&text=The%20Google%20Analytics%20Measurement%20Protocol,directly%20to%20Google%20Analytics%20servers.)

- **Campaign Manager**
  - Offline Conversions API **(user id, device id, match id, gclid, dclid)** [[details]](https://developers.google.com/doubleclick-advertisers/guides/conversions_upload)

- **Google Analytics 4**
  - Measurement protocol (Web + App) [[details]](https://developers.google.com/analytics/devguides/collection/protocol/ga4)

- **Appsflyer**
  - S2S Offline events API (conversion upload), to be used for audience creation and in-app events with Google Ads and DV360 [[details]](https://support.appsflyer.com/hc/en-us/articles/207034486-API-de-eventos-de-servidor-para-servidor-S2S-mobile-para-mobile)

## How does it work
Megalista was design to separate the configuration of conversion/audience upload rules from the engine, giving more freedom for non-technical teams (i.e. Media and Business Inteligence) to setup multiple upload rules on their own.

The solution consists of #1 a configuration environment (either Google Sheet or JSON file, or a Google Cloud Firestore collection) in which all rules are defined by mapping a data source (BigQuery Table) to a destination (data upload endpoint) and #2, an Apache Beam workflow running on Google Dataflow, scheduled to upload the data in batch mode.

## Prerequisites

### Google Cloud Services
- **Google Cloud Platform** account
  - **Billing** enabled
  - **BigQuery** enabled
  - **Dataflow** enabled
  - **Cloud storage** enabled
  - **Cloud scheduler** enabled
- At least one of:
  - **Google Ads** API Access
  - **Campaign Manager** API Access
  - **Google Analytics** API Access
- **Python3**
- **Google Cloud SDK**

### Access Requirements
Those are the minimum roles necessary to deploy Megalista:
- OAuth Config Editor
- BigQuery User
- BigQuery Job User
- BigQuery Data Viewer
- Cloud Scheduler Admin
- Storage Admin
- Dataflow Admin
- Service Account Admin
- Logs Viewer
- Service Consumer

### APIs
Required APIs will depend on upload endpoints in use. We recomend you to enable all of them:
- Google Sheets (required if using Sheets configuration) [[link]](https://console.cloud.google.com/apis/library/sheets.googleapis.com)
- Google Analytics [[link]](https://console.cloud.google.com/apis/library/analytics.googleapis.com)
- Google Analytics Reporting [[link]](https://console.cloud.google.com/apis/library/analyticsreporting.googleapis.com)
- Google Ads [[link]](https://console.cloud.google.com/apis/library/googleads.googleapis.com)
- Campaign Manager [[link]](https://console.cloud.google.com/apis/library/dfareporting.googleapis.com)
- Google Cloud Firestore [[link]](https://console.cloud.google.com/apis/library/firestore.googleapis.com)


## Installation

### Configure Megalista
Megalista can be configured via Google Sheets, a JSON file or a Google Cloud Firestore collection. Expected data schemas (Sources) and metadata (Destinations) for each use case defined in [the Megalista Wiki](https://github.com/google/megalista/wiki).

To configure using Google Sheets:
 - Make a copy of the [Sheets template](https://docs.google.com/spreadsheets/d/1rP9x93h5CVu2IdUjP6yJ8yYEMcb9J_WPx2sBEEpLMJI)
 - In the "Intro" sheet provide Account IDs for Google Ads, Analytics, CM360, etc.
 - Configure source/input data in the "Sources" sheet
 - Configure destinations in the "Destinations" sheet
  + The "Help" sheet has a chart to document the required metadata for each use case.
 - Configure connections from Sources to Destinations in the "Connect" sheet
 - Make note of the Sheet's ID in the URL: `docs.google.com/spreadsheets/d/`**EXAMPLEIDHERE**`/edit`

 To configure using JSON:
 - Make a copy of the [JSON template](https://github.com/google/megalista/tree/main/cloud_config/configuration_sample.json)
 - Provide Account IDs for Google Ads, Analytics, CM360, etc.
 - Configure Sources and Destinations, separating entries with commas
 - Connect Sources to Destinations in the "Connections" field
 - Save and upload this JSON file to a Google Cloud Storage bucket (default bucket options are fine)
 - Make note of the files's Authenticated URL in the Cloud Storage UI: `storage.cloud.google.com/`**mybucketname/myconfig.json**

 To configure using Firestore (recommended for use with web interface - WIP):
 - Verify that the Firestore API is enabled on Google Cloud (see the APIs section)
 - Create a [collection](https://firebase.google.com/docs/firestore/data-model) in Firestore, with the name of your choice
 - Inside the collection, create a Firestore document named "account_config", and provide Account IDs for Google Ads, Analytics, CM360, etc. See schema below.
 - Inside the collection, create separate Firestore documents for each Source/Destination pair. See schema below.

#### Firestore document schemas

**account_config document schema:**

| Field name  | Description  |
|---|---|
| google_ads_id | Google Ads account ID (customer ID) used by default |
| mcc_trix  | TRUE/FALSE. Indicates whether the Google Ads account is an MCC (parent account) or not |
| google_analytics_account_id | Google Analytics account ID |
| campaign_manager_account_id | Google Campaign Manager account ID |
| app_id | Android/iOS app ID for Google Ads or Appsflyer used by default |

**Source+Destination document schema:**

Universal parameters (mandatory for any upload type):
| Field name  | Description  |
|---|---|
| active | yes/no. Enables or disables scheduled uploads for the Source/Destination pair |
| bq_dataset | Name of the souce Big Query dataset |
| bq_table | Name of the source Big Query table |
| source | BIG_QUERY. Data source for the uploads. |
| source_name | Display-only name for the source, shown on logs. Example: "Top spending customers" |
| destination_name | Display-only name for the destination, shown on logs. Example: "Customer match" |

Google Ads conversions
| Field name  | Description  |
|---|---|
| gads_conversion_name | Name of the Google Ads conversion registered on the platform |
| type | ADS_OFFLINE_CONVERSION |

Google Ads Store Sales Direct
| Field name  | Description  |
|---|---|
| gads_conversion_name | Name of the Google Ads conversion registered on the platform |
| gads_external_upload_id | External upload ID |
| type | ADS_SSD_UPLOAD |

Google Ads Customer Match - Contact
| Field name  | Description  |
|---|---|
| gads_audience_name | Name of the Google Ads audience registered on the platform |
| gads_operation | ADD/REMOVE. Indicates whether the user list should be added or removed from the audience |
| gads_hash | TRUE/FALSE. Enables hashing on data |
| gads_account | (Optional) Google Ads account ID (Customer ID). Overlaps the default account ID in the account_config document |
| type | ADS_CUSTOMER_MATCH_CONTACT_INFO_UPLOAD |

Google Ads Customer Match - Mobile
| Field name  | Description  |
|---|---|
| gads_audience_name | Name of the Google Ads audience registered on the platform |
| gads_operation | ADD/REMOVE. Indicates whether the user list should be added or removed from the audience |
| gads_account | (Optional) Google Ads account ID (Customer ID). Overlaps the default account ID in the account_config document |
| gads_app_id | (Optional) Android/iOS app ID for Google Ads. Overlaps the default app ID in the account_config document  |
| type | ADS_CUSTOMER_MATCH_MOBILE_DEVICE_ID_UPLOAD |

Google Ads Customer Match - User ID
| Field name  | Description  |
|---|---|
| gads_audience_name | Name of the Google Ads audience registered on the platform |
| gads_operation | ADD/REMOVE. Indicates whether the user list should be added or removed from the audience |
| gads_account | (Optional) Google Ads account ID (Customer ID). Overlaps the default account ID in the account_config document |
| gads_hash | TRUE/FALSE. Enables hashing on data |
| type | ADS_CUSTOMER_MATCH_USER_ID_UPLOAD |

Google Analytics - Measurement Protocol
| Field name  | Description  |
|---|---|
| google_analytics_property_id | Google Analytics property ID (UA) |
| google_analytics_non_interaction | 1/0. Indicates whether the event is a non-interaction hit |
| type | GA_MEASUREMENT_PROTOCOL |

Google Analytics - Data Import
| Field name  | Description  |
|---|---|
| google_analytics_property_id | Google Analytics property ID (UA) |
| google_analytics_data_import_name | Name of the data import set in Google Analytics |
| type | GA_DATA_IMPORT |

Google Analytics - User List
| Field name  | Description  |
|---|---|
| google_analytics_property_id | Google Analytics property ID (UA) |
| google_analytics_view_id | Google Analytics view ID in the property selected |
| google_analytics_data_import_name | Name of the data import set in Google Analytics |
| google_analytics_user_id_list_name | Name of the user ID list |
| google_analytics_user_id_custom_dim | User ID custom dimension |
| google_analytics_buyer_custom_dim | Buyer custom dimension |
| type | GA_USER_LIST_UPLOAD |

Campaign Manager
| Field name  | Description  |
|---|---|
| campaign_manager_floodlight_activity_id | Floodlight activity ID |
| campaign_manager_floodlight_configuration_id | Floodlight configuration ID |
| type | CM_OFFLINE_CONVERSION |

Appsflyer S2S events
| Field name  | Description  |
|---|---|
| appsflyer_app_id |  |
| type | APPSFLYER_S2S_EVENTS |



### Creating required access tokens
To access campaigns and user lists on Google's platforms, this dataflow will need OAuth tokens for a account that can authenticate in those systems.

In order to create it, follow these steps:
 - Access GCP console
 - Go to the **API & Services** section on the top-left menu.
 - On the **OAuth Consent Screen** and configure an *Application name*
 - Then, go to the **Credentials** and create an *OAuth client Id* with Application type set as *Desktop App*
 - This will generate a *Client Id* and a *Client secret*
 - Run the **generate_megalista_token.sh** script in this folder providing these two values and follow the instructions
   - Sample: `./generate_megalista_token.sh client_id client_secret`
 - This will generate the *Access Token* and the *Refresh token*

### Creating a bucket on Cloud Storage
This bucket will hold the deployed code for this solution. To create it, navigate to the *Storage* link on the top-left menu on GCP and click on *Create bucket*. You can use Regional location and Standard data type for this bucket.

## Running Megalista

We recommend first running it locally and make sure that everything works.
Make some sample tables on BigQuery for one of the uploaders and make sure that the data is getting correctly to the destination.
After that is done, upload the Dataflow template to GCP and try running it manually via the UI to make sure it works.
Lastly, configure the Cloud Scheduler to run Megalista in the frequency desired and you'll have a fully functional data integration pipeline.

### Running locally
Only set one configuration parameter (setup_sheet_id, setup_json_url or setup_firestore_collection)
```bash
python3 megalista_dataflow/main.py \
  --runner DirectRunner \
  --developer_token ${GOOGLE_ADS_DEVELOPER_TOKEN} \
  --setup_sheet_id ${CONFIGURATION_SHEET_ID} \
  --setup_json_url ${CONFIGURATION_JSON_URL} \
  --setup_firestore_collection ${CONFIGURATION_FIRESTORE_COLLECTION}
  --refresh_token ${REFRESH_TOKEN} \
  --access_token ${ACCESS_TOKEN} \
  --client_id ${CLIENT_ID} \
  --client_secret ${CLIENT_SECRET} \
  --project ${GCP_PROJECT_ID} \
  --region us-central1 \
  --temp_location gs://{$GCS_BUCKET}/tmp
```

### Deploying Pipeline
To deploy the full Megalista pipeline, use the following command from the root folder:
`./terraform_deploy.sh`

#### Manually executing pipeline using Dataflow UI
To execute the pipeline, use the following steps:
- Go to **Dataflow** on GCP console
- Click on *Create job from template*
- On the template selection dropdown, select *Custom template*
- Find the *megalista* file on the bucket you've created, on the templates folder
- Fill in the parameters required and execute

### Scheduling pipeline
To schedule daily/hourly runs, go to **Cloud Scheduler**:
- Click on *create job*
- Add a name and frequency as desired
- For *target* set as HTTP
- Configure a *POST* for url: https://dataflow.googleapis.com/v1b3/projects/${YOUR_PROJECT_ID}/locations/${LOCATION}/templates:launch?gcsPath=gs://${BUCKET_NAME}/templates/megalista, replacing the params with the actual values
- For a sample on the *body* of the request, check **cloud_config/scheduler_sample.json**
- Add OAuth Headers
- Scope: https://www.googleapis.com/auth/cloud-platform

#### Creating a Service Account
It's recommended to create a new Service Account to be used with the Cloud Scheduler
- Go to IAM & Admin > Service Accounts
- Create a new Service Account with the following roles:
    - Cloud Dataflow Service Agent
    - Dataflow Admin
    - Storage Objects Viewer


## Usage
Every upload method expects as source a BigQuery data with specific fields, in addition to specific configuration metadata. For details on how to setup your upload routines, refer to the [Megalista Wiki](https://github.com/google/megalista/wiki) or the [Megalista user guide](https://github.com/google/megalista/blob/main/documentation/Megalista%20-%20Technical%20User%20Guide%20-%20EXTERNAL.pdf).