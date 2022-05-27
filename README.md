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
  - Offline Conversions API **(user id, device id, match id, gclid, dclid, value, quantity, and customVariables)** [[details]](https://developers.google.com/doubleclick-advertisers/guides/conversions_upload)

- **Google Analytics 4**
  - Measurement protocol (Web + App) [[details]](https://developers.google.com/analytics/devguides/collection/protocol/ga4)

- **Display & Video**
  - Contact Info **Customer Match** (email, phone, address) [[details]](https://support.google.com/displayvideo/answer/9539301?hl=en)
  - Id Based **Customer Match** (device Id)

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
  - **App Engine** enabled
- At least one of:
  - **Google Ads** API Access
  - **Campaign Manager** API Access
  - **Google Analytics** API Access
  - **Display & Video** API Access
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
Required APIs will depend on upload endpoints in use.
- Google Sheets (required if using Sheets configuration) [[link]](https://console.cloud.google.com/apis/library/sheets.googleapis.com)
- Google Analytics [[link]](https://console.cloud.google.com/apis/library/analytics.googleapis.com)
- Google Analytics Reporting [[link]](https://console.cloud.google.com/apis/library/analyticsreporting.googleapis.com)
- Google Ads [[link]](https://console.cloud.google.com/apis/library/googleads.googleapis.com)
- Campaign Manager [[link]](https://console.cloud.google.com/apis/library/dfareporting.googleapis.com)
- Google Cloud Firestore [[link]](https://console.cloud.google.com/apis/library/firestore.googleapis.com)
- Display & Video [[link]](https://console.cloud.google.com/apis/library/displayvideo.googleapis.com)

## Configure Megalista
Megalista can be configured via Google Sheets, a JSON file or a Google Cloud Firestore collection. Expected data schemas (Sources) and metadata (Destinations) for each use case defined in [the Megalista Wiki](https://github.com/google/megalista/wiki).

Instructions for each configuration method method can be found in the Megalista wiki
- [Google Sheets] (https://github.com/google/megalista/wiki/Google-Sheets)
- [JSON] (https://github.com/google/megalista/wiki/JSON)
- [Firestore] (https://github.com/google/megalista/wiki/Firestore)

## Deployment

These guide assumes it'll be followed inside Google Cloud Platform Console.

### Creating required access tokens
To access campaigns and user lists on Google's platforms, this dataflow will need OAuth tokens for an account that can authenticate in those systems.

In order to create it, follow these steps:
 - Access GCP console
 - Go to the **API & Services** section on the top-left menu.
 - On the **OAuth Consent Screen** and configure an *Internal Consent Screen*
 - Then, go to the **Credentials** and create an *OAuth client Id* with Application type set as *Desktop App*
 - This will generate a *Client Id* and a *Client secret*. Save these values as they are required during the deployment
 - Run the **generate_megalista_token.sh** script in this folder providing these two values and follow the instructions
   - Sample: `./generate_megalista_token.sh client_id client_secret`
 - This will generate the *Access Token* and the *Refresh token*
   -  The user who opened the generated link and clicked on *Allow* must have access to the platforms that Megalista will integrate, including the configuration Sheet, if this is the chosen method for configuration.

### Deploying Pipeline
To deploy the full Megalista pipeline, use the following command from the root folder:
`./terraform_deploy.sh`
The script will required some parameters, between them:
- Auxliary bigquery dataset for Megalista operations to create
  - This dataset will be used for storing operational data and will be created by Terraform
- Google Cloud Storage Bucket to create
  - This Cloud Storage Bucket will be used to store Megalista compiled binary, metadata and temp files and will be created by Terraform.
- *Setup Firestore collection*, *URL for JSON configuration* and *Setup Sheet Id*
  - Only one of these three should be filled and the other should be left black accordingly to the chosen configuration method.

### Updating the Binary
to update the binary whitouth redoing the whole deployment process, run:
- ./terraform/scripts/deploy_cloud.sh *gcp_project_id* *bucket_name* *region*

## Usage
Every upload method expects as source a BigQuery data with specific fields, in addition to specific configuration metadata. For details on how to setup your upload routines, refer to the [Megalista Wiki](https://github.com/google/megalista/wiki).

## Errors notifications by email
To have uploaders errors captured and sent by email, do the following:  
In Cloud Scheduler, in the `parameters` section of the request body, add `notify_errors_by_email` parameter as `true` and `errors_destination_emails` with a list of emails divided by comma (`a@gmail.com,b@gmail.com` etc).  
This parameters should be add the same list of pre-configured ones, such as `client_id`, `client_secret` etc.  

If the access tokens being used were generated prior to version `v4.4`, new access and refresh tokens must be generated to activate this feature. This is necessary because old token don't have the `gmail.send` scope.



## Note about Google Ads API access
Calls to the Google Ads API will fail if the user that generated the OAuth2 credentials (Access Token and Refresh Token) doesn't have direct access to the Google Ads account which the calls are being directed to. It's not enough for the user to have access to a MCC above this account and being able to access the account through the interface, it's required that the user has permissions on the account itself.
