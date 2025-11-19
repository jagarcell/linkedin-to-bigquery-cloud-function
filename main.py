import os
from flask import jsonify
import pandas as pd
import requests
import smtplib

from datetime import datetime, timedelta, timezone
from google.cloud import bigquery, secretmanager
import metrics
from google.api_core.exceptions import NotFound
from email.mime.text import MIMEText


# ======================================================================
# Config from env
# ======================================================================
PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET_ID = os.environ.get("BIGQUERY_DATASET", "linkedin")
TABLE_IDS = metrics.BIGQUERY_TABLES
ACCOUNT_ID = os.environ["LINKEDIN_ACCOUNT_ID"]
ACCOUNT_NAME = None

# Secret names (store these in Secret Manager beforehand)
ACCESS_TOKEN_SECRET = os.environ.get("LINKEDIN_ACCESS_TOKEN", "LINKEDIN_ACCESS_TOKEN")
REFRESH_TOKEN_SECRET = os.environ.get("LINKEDIN_REFRESH_TOKEN", "LINKEDIN_REFRESH_TOKEN")

# LinkedIn app client credentials (store securely as env vars or in Secret Manager)
CLIENT_ID = os.environ["LINKEDIN_CLIENT_ID"]
CLIENT_SECRET = os.environ["LINKEDIN_CLIENT_SECRET"]

PIVOTS = metrics.PIVOTS

# Email settings
EMAIL_USER = os.environ.get("EMAIL_USER")
EMAIL_PASS = os.environ.get("EMAIL_PASS")
SMTP_SERVER = os.environ.get("SMTP_SERVER")
SMTP_PORT = os.environ.get("SMTP_PORT")
EMAIL_RECIPIENT = os.environ.get("EMAIL_RECIPIENT")


# ======================================================================
# Email helpers
# ======================================================================
def send_email(recipient, subject, body):
    msg = MIMEText(body)
    msg["From"] = EMAIL_USER
    msg["To"] = recipient
    msg["Subject"] = subject

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_USER, recipient, msg.as_string())

# ======================================================================
# Get Account Name
# ======================================================================
def getAccountName(account_id, access_token):
    url = f"https://api.linkedin.com/rest/adAccounts/{account_id}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "LinkedIn-Version": "202510",
        "Content-Type": "application/json",
        "X-Restli-Protocol-Version": "2.0.0"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        return data.get("name", "N/A")
    return "N/A"

# ======================================================================
# Secret Manager helpers
# ======================================================================
sm_client = secretmanager.SecretManagerServiceClient()

def access_secret(secret_name):
    name = f"projects/{PROJECT_ID}/secrets/{secret_name}/versions/latest"
    try:
        resp = sm_client.access_secret_version(name=name)
    except Exception as e:
        # Let caller handle missing secret
        raise RuntimeError(f"Could not access secret {secret_name}: {e}")
    return resp.payload.data.decode("utf-8")

# ======================================================================
# Add new secret version
# ======================================================================
def add_secret_version(secret_name, secret_value):
    parent = f"projects/{PROJECT_ID}/secrets/{secret_name}"
    # secretmanager.SecretPayload expects bytes in .data
    payload = {"data": secret_value.encode("utf-8")}
    # Using the client method - it accepts a dict-like payload
    sm_client.add_secret_version(parent=parent, payload=payload)

# ======================================================================
# LinkedIn token helpers
# ======================================================================
def refresh_access_token_using_refresh_token(refresh_token):
    url = "https://www.linkedin.com/oauth/v2/accessToken"
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }
    resp = requests.post(url, data=data)
    resp.raise_for_status()
    return resp.json()

# ======================================================================
# Test access token validity
# ======================================================================
def test_access_token(token):
    # lightweight test; will return True if token works, False if 401
    url = "https://api.linkedin.com/v2/me"
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(url, headers=headers)
    return r.status_code == 200

# ======================================================================
# Get valid access token (refresh if needed)
# ======================================================================
def get_valid_access_token():
    """
    Returns a valid access token string. Uses access token from Secret Manager
    if still valid. Otherwise uses refresh token to obtain a new access token,
    updates secrets and returns the new token.
    """
    # Try reading existing access token (may not exist)
    access_token = None
    try:
        access_token = access_secret(ACCESS_TOKEN_SECRET)
    except Exception:
        access_token = None

    if access_token and test_access_token(access_token):
        return access_token

    # Need to refresh using refresh token
    try:
        refresh_token = access_secret(REFRESH_TOKEN_SECRET)
    except Exception as e:
        raise RuntimeError("Refresh token missing in Secret Manager. Please add it.") from e

    token_response = refresh_access_token_using_refresh_token(refresh_token)
    # token_response example: { "access_token": "...", "expires_in": 5184000, "refresh_token": "...", "refresh_token_expires_in": 31535999 }
    new_access_token = token_response.get("access_token")
    if not new_access_token:
        raise RuntimeError("LinkedIn did not return access_token when refreshing.")

    # Update access token secret by adding a new version
    add_secret_version(ACCESS_TOKEN_SECRET, new_access_token)

    # If LinkedIn returned a new refresh_token (some flows do), update it too
    new_refresh_token = token_response.get("refresh_token")
    if new_refresh_token:
        add_secret_version(REFRESH_TOKEN_SECRET, new_refresh_token)

    return new_access_token

# ======================================================================
# BigQuery helpers
# ======================================================================
bq_client = bigquery.Client(project=PROJECT_ID)

# ======================================================================
# Ensure dataset and tables exist
# ======================================================================
def ensure_dataset_and_table():
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    try:
        bq_client.get_dataset(dataset_ref)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        print(f"Couldn't find dataset: {DATASET_ID}")
        return jsonify({"status": "error", "reason": f"Couldn't find dataset: {DATASET_ID}"}), 404

    for table_info in TABLE_IDS:
        for TABLE_ID, table_config in table_info.items():
            table_ref = bq_client.dataset(DATASET_ID).table(TABLE_ID)
            try:
                bq_client.get_table(table_ref)
            except NotFound:
                print(f"Couldn't find table: {TABLE_ID}")
                return jsonify({"status": "error", "reason": f"Couldn't find table: {TABLE_ID}"}), 404

# ======================================================================
# LinkedIn data fetch + flatten
# ======================================================================
def get_yesterday_date_parts():
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    return yesterday.year, yesterday.month, yesterday.day, yesterday.isoformat()

# ======================================================================
# LinkedIn data fetch
# ======================================================================
def fetch_linkedin_analytics(access_token, date):
    date = datetime.strptime(date, "%Y-%m-%d").date()
    start_date = date
    end_date = date
    url = (
        "https://api.linkedin.com/rest/adAnalytics"
        "?q=statistics"
        "&timeGranularity=DAILY"
        f"&accounts=List(urn%3Ali%3AsponsoredAccount%3A{ACCOUNT_ID})"
        "&pivots=List(CAMPAIGN,CAMPAIGN_GROUP)"
        f"&dateRange=(start:(day:{start_date.day},month:{start_date.month},year:{start_date.year}),end:(day:{end_date.day},month:{end_date.month},year:{end_date.year}))"
        "&fields=pivotValues,impressions,clicks,costInUsd,oneClickLeads,externalWebsiteConversions,sends,opens,validWorkEmailLeads,oneClickLeadFormOpens,shares,comments,reactions,totalEngagements,cardImpressions,cardClicks"
    )

    headers = {
        "Authorization": f"Bearer {access_token}",
        "LinkedIn-Version": "202510",
        "Content-Type": "application/json",
        "X-Restli-Protocol-Version": "2.0.0"
    }

    print(f"📡 Requesting LinkedIn Ad Analytics for {date} ...")
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()

# ======================================================================
# Data flattening and insertion
# ======================================================================
def flatten_linkedin_response(access_token, json_data, date):
    rows = []
    date = datetime.strptime(date, "%Y-%m-%d").date()

    headers = {
        "Authorization": f"Bearer {access_token}",
        "LinkedIn-Version": "202510",
        "Content-Type": "application/json",
        "X-Restli-Protocol-Version": "2.0.0"
    }

    elements = json_data.get("elements", [])

    for element in elements:
        campaign_group_id = "N/A"
        campaign_id = "N/A"
        campaign_group_name = "N/A"
        campaign_name = "N/A"
        campaign_type = "N/A"
        campaign_status = "N/A"

        try:
            pivot_values = element.get("pivotValues", [])
            for urn in pivot_values:
                if "CampaignGroup" in urn:
                    campaign_group_id = urn.split(":")[-1]
                    url = f"https://api.linkedin.com/rest/adAccounts/{ACCOUNT_ID}/adCampaignGroups/{campaign_group_id}"
                    resp = requests.get(url, headers=headers).json()
                    campaign_group_name = resp.get("name", "N/A")
                elif "Campaign" in urn:
                    campaign_id = urn.split(":")[-1]
                    url = f"https://api.linkedin.com/rest/adAccounts/{ACCOUNT_ID}/adCampaigns/{campaign_id}"
                    resp = requests.get(url, headers=headers).json()
                    campaign_name = resp.get("name", "N/A")
                    campaign_type = resp.get("type", "N/A")
                    campaign_status = resp.get("status", "N/A")
        except Exception as e:
            print(f"Error processing pivotValues: {e}")

        # remove pivotValues safely
        element.pop("pivotValues", None)

        # build the row
        row = {
            "date": date.strftime("%Y-%m-%d"),
            "account_name": ACCOUNT_NAME,
            "account_id": ACCOUNT_ID,
            "campaign_group_name": campaign_group_name,
            "campaign_group_id": campaign_group_id,
            "campaign_name": campaign_name,
            "campaign_id": campaign_id,
            "campaign_type": campaign_type,
            "campaign_status": campaign_status,
        }

        # merge the remaining metrics
        row.update(element)
        rows.append(row)

    return rows

# =========================================================================
# BigQuery insert helpers
# =========================================================================
def insert_rows_into_bq(rows, table_id):
    if not rows:
        print("No rows to insert.")
        return 0
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"
    job = bq_client.load_table_from_json(rows, table_ref)
    try:
        job.result()  # Wait for the job to complete
    except Exception as e:
        print("Load Job Failed")
        if job.errors:
            print("BigQuery insert errors:", job.errors)
            raise RuntimeError(f"BigQuery insert errors: {job.errors}")
        else:
            raise RuntimeError(f"BigQuery load job failed: {e}")
    print(f"Inserted {job.output_rows} rows into {table_ref}")
    return job.output_rows

# ======================================================================
# Delete existing records in date range to avoid duplicates
# ======================================================================
def delete_records_in_date_range(start_date, end_date, table_id):
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"
    query = f"""
        DELETE FROM `{table_ref}`
        WHERE date >= @start_date AND date <= @end_date
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
        ]
    )
    query_job = bq_client.query(query, job_config=job_config)
    query_job.result()  # Wait for job to complete
    print(f"Deleted records from {start_date} to {end_date} in {table_ref}")
    return query_job.num_dml_affected_rows

# ======================================================================
# LinkedIn data fetch for specific date with metrics and pivots
# ======================================================================
def get_linkedin_analytics_for_date(access_token, date, metrics=[], pivots=[]):
    print(f"Fetching LinkedIn analytics for {date} with metrics: {metrics} and pivots: {pivots}")
    date = datetime.strptime(date, "%Y-%m-%d").date()
    start_date = date
    end_date = date

    impressionsRequested = "impressions" in metrics

    headers = {
        "Authorization": f"Bearer {access_token}",
        "LinkedIn-Version": "202510",
        "Content-Type": "application/json",
        "X-Restli-Protocol-Version": "2.0.0"
    }

    q = "statistics"
    qPivots = ""
    # Ensure pivotValues is always requested
    metrics.insert(0, "pivotValues")
    # Ensure impressions are always requested, this is needed for LinkedIn to always return other metrics
    # Not documented by LinkedIn but observed behavior.
    metrics.insert(1, "impressions")
    if len(pivots) == 0:
        qPivots = "&pivots=List(CAMPAIGN,CAMPAIGN_GROUP)"
    else:
        if len(pivots) < 2:
            q="analytics"
            qPivots = f"&pivot={pivots[0]}"
        else:
            qPivots = f"&pivots=List({','.join(pivots)})"


    url = (
        "https://api.linkedin.com/rest/adAnalytics"
        f"?q={q}"
        "&timeGranularity=DAILY"
        f"&accounts=List(urn%3Ali%3AsponsoredAccount%3A{ACCOUNT_ID})"
        f"&dateRange=(start:(day:{start_date.day},month:{start_date.month},year:{start_date.year}),end:(day:{end_date.day},month:{end_date.month},year:{end_date.year}))"
        f"{qPivots}"
        "&fields="
        f"{','.join(metrics)}"
    )
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    retval = r.json()
    if not impressionsRequested:
        metrics.remove("impressions")
        # Removing impressions from response as it was not requested
        for element in retval.get("elements", []):
            element.pop("impressions", None)
    for element in retval.get("elements", []):
        for metric in metrics:
            if metric not in element:
                element[metric] = 0
    return retval

# ======================================================================
# Test data fetch
# ======================================================================
def get_linkedin_metrics(access_token, date, metrics=[], pivots=[]):
    r = get_linkedin_analytics_for_date(
            access_token,
            date,
            metrics,
            pivots
        )
    rows = flatten_linkedin_response(access_token, r, date)
    return rows

# ======================================================================
# Cloud Function entrypoint
# ======================================================================
def jc_linkedin_to_bq(request):
    try:
        print("Starting LinkedIn to BigQuery data ingestion...")

        # Ensure we have a valid access token (refresh if needed)
        valid_access_token = get_valid_access_token()

        global ACCOUNT_NAME
        ACCOUNT_NAME = getAccountName(ACCOUNT_ID, valid_access_token)
        print(f"Using LinkedIn Account Name: {ACCOUNT_NAME}")

        # Ensure BigQuery dataset and table exist
        ensure_dataset_and_table()
        _, _, _, date = get_yesterday_date_parts()

        emailLogs = []

        # Set the number of tables to process
        nTables = len(TABLE_IDS)
        print(f"Number of tables to process: {nTables}")
        nTableProcessing = 0

        for table_info in TABLE_IDS:
            for TABLE_ID, table_config in table_info.items():
                nTableProcessing += 1
                print(f"Processing table: {nTableProcessing} of {nTables} - {TABLE_ID}")
                # Delete existing records for that date to avoid duplicates
                delete_records_in_date_range(date, date, TABLE_ID)

                n_rows = 0
                metrics = table_config.get("metrics", [])
                for date_to_process in pd.date_range(date, date):
                    print(f"Processing date: {date_to_process}")
                    # Fetch LinkedIn analytics for yesterday
                    date_str = date_to_process.strftime("%Y-%m-%d")
                    print(f"Fetching data for date: {date_str}")
                    rows = get_linkedin_metrics(valid_access_token, date_str, metrics=metrics, pivots=PIVOTS)
                    inserted_rows = insert_rows_into_bq(rows, TABLE_ID)
                    n_rows += inserted_rows
                    emailLogs.append("=" * 50)
                    emailLogs.append(f"Inserted {inserted_rows} rows into table ({TABLE_ID}) of BigQuery dataset {DATASET_ID} for date {date_str}")

        send_email(
            EMAIL_RECIPIENT, 
            "LinkedIn Data Ingestion", 
            (
                f"Date processed: {date}\n"
                f"Account Name: {ACCOUNT_NAME}\n"
                f"Dataset: {DATASET_ID}\n"
                f"{chr(10).join(emailLogs)}\n"
            )
        )
        return (f"Inserted {n_rows} rows.", 200)
    except Exception as e:
        send_email(
            EMAIL_RECIPIENT, 
            "LinkedIn Data Ingestion Error", 
            (
                f"Error: {e}\n"
                f"Date processed: {date}\n"
                f"Account Name: {ACCOUNT_NAME}\n"
                f"Dataset: {DATASET_ID}\n"
            )
        )
        return (f"Error: {e}", 500)

