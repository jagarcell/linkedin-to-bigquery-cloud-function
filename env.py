GCP_PROJECT_ID='<your-gcp-project-id>'
LINKEDIN_CLIENT_ID='<your-linkedin-client-id>'
LINKEDIN_CLIENT_SECRET='<your-linkedin-client-secret>'
LINKEDIN_ACCOUNT_ID='<your-linkedin-account-id>'
ACCESS_TOKEN_SECRET='<your-access-token-secret>'
REFRESH_TOKEN_SECRET='<your-refresh-token-secret>'
BIGQUERY_DATASET='<your-bigquery-dataset>'

# LinkedIn app client credentials (store securely as env vars or in Secret Manager)
CLIENT_ID = LINKEDIN_CLIENT_ID # this value will be used as a key for the environment variable in google cloud functions
CLIENT_SECRET = LINKEDIN_CLIENT_SECRET # this value will be used as a key for the environment variable in google cloud functions

# Email settings
EMAIL_USER = '<email_user>'
EMAIL_PASS = '<email_password>'
SMTP_SERVER = '<smtp_server>'
SMTP_PORT = '<smtp_port>'
EMAIL_RECIPIENT = '<email_recipient>'
