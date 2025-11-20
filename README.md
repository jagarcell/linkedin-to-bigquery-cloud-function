# Linkedin Metrics to BigQuery Integration Cloud Function
This is the function that will run in Google Cloud to pull the metrics from Linkedin and ingest them into BigQuery.

Once the metrics are in BigQuery they can be rearranged as desired by creting the SQL queries needed.

# Prepare you local development environment
You will need to install Python in your environment and make sure that is added to your PATH.

Install Google Cloud SDK in your local

# Run this commands in your local
gcloud auth login  (authenticate your local with Google Cloud)

gcloud config set project <google_cloud_project_id>  (Select your project, can be other than this one)

# Instructions
This has been developed to be used with any project that lives here:

https://console.cloud.google.com/

Create a Linkedin App or use an existing one at:

https://www.linkedin.com/developers/apps

# Useful local executable Python scripts
roles.py (Will list the roles associated to the ACCESS_TOKEN_SECRET)

deploy.py (Use this to deply the function when you make updates to the code)

main_local.py (Use this script to pull the metrics and ingest them to BigQuery from your local)

# Links of interest
Linkedin API documentation:

https://learn.microsoft.com/en-us/linkedin/

BigQuery documentation:

https://docs.cloud.google.com/bigquery/docs