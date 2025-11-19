This is the fucntion that will run in Google Cloud to pull the metrics from Linkedin and ingest them into BigQuery.
Once the metrics are in BigQuery they can be rearranged as desired by creting the SQL queries needed.

You will need to install Python in your environment and make sure that is added to your PATH.

Install Google Cloud SDK in your local

# Run this commands in your local
gcloud auth login  (authenticate your local with Google Cloud)
gcloud config set project <google_cloud_project_id>  (Select your project, can be other than this one)

This has been developed to be used with any project that lives here:
https://console.cloud.google.com/

Create a Linkedin App or use an existing one at:
https://www.linkedin.com/developers/apps

Linkedin API documentation:
https://learn.microsoft.com/en-us/linkedin/
