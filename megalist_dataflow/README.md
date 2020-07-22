# Megalist

Sample integration code for onboarding data from BigQuery to Google Ads, Google Analytics 360, DV360 and DCM.

**Disclaimer:** This is not an officially supported Google product.

## Prerequisites

- **Google Cloud Platform** account
  - **Billing** enabled
  - **BigQuery** enabled
  - **Dataflow** enabled
  - **Datastore** enabled
  - **Cloud storage** enabled
  - **Cloud scheduler** enabled
- At least one of:
  - **Google Ads** API Access
  - **Campaign Manager** API Access
  - **Google Analytics** API Access
  - **AppsFlyer** Dev token for S2S events
- **Python3**
- **Google Cloud SDK**

## First Steps

### Creating required access tokens
To access campaigns and user lists on Google's platforms, this dataflow will need OAuth tokens for a account that can authenticate in those systems.

In order to create it, follow these steps:
 - Access GCP console
 - Go to the **API & Services** section on the top-left menu.
 - On the **OAuth Consent Screen** and configure an *Application name*
 - Then, go to the **Credentials** and create an *OAuth client Id* with Application type set as *Other*
 - This will generate a *Client Id* and a *Client secret*
 - Run the **generate_megalist_token.sh** script in this folder providing these two values and follow the instructions
   - Sample: `./generate_megalist_token.sh client_id client_secret`
 - This will generate the *Access Token* and the *Refresh token*

### Creating a bucket on Cloud Storage
This bucket will hold the deployed code for this solution. To create it, navigate to the *Storage* link on the top-left menu on GCP and click on *Create bucket*. You can use Regional location and Standard data type for this bucket.

## Deploying Pipeline
To deploy, use the following command:
`./deploy_cloud.sh project_id bucket_name`

## Manually executing pipeline
To execute the pipeline, use the following steps: 
- Go to **Dataflow** on GCP console
- Click on *Create job from template*
- On the template selection dropdown, select *Custom template* 
- Find the *megalist* file on the bucket you've created, on the templates folder
- Fill in the parameters required and execute

## Scheduling pipeline
To schedule daily/hourly runs, go to **Cloud Scheduler**:
- Click on *create job* 
- Add a name and frequency as desired
- For *target* set as HTTP
- Configure a *POST* for url: https://dataflow.googleapis.com/v1b3/projects/${YOUR_PROJECT_ID}/templates:launch?gcsPath=gs://${BUCKET_NAME}/templates/megalist, replacing the params with the actual values
- For a sample on the *body* of the request, check **cloud_config/scheduler.json** 