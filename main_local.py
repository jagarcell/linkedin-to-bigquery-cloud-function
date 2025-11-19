import os
import re
import sys
import pandas as pd
import requests
import smtplib

from datetime import date, datetime, timedelta, timezone
from google.cloud import bigquery, secretmanager
import env
import metrics
from google.api_core.exceptions import NotFound
from email.mime.text import MIMEText

# ======================================================================
# Project config
# ======================================================================
PROJECT_ID = env.GCP_PROJECT_ID
DATASET_ID = env.BIGQUERY_DATASET
TABLE_IDS = metrics.BIGQUERY_TABLES
ACCOUNT_ID = env.LINKEDIN_ACCOUNT_ID
ACCESS_TOKEN_SECRET = env.ACCESS_TOKEN_SECRET
REFRESH_TOKEN_SECRET = env.REFRESH_TOKEN_SECRET
CLIENT_ID = env.LINKEDIN_CLIENT_ID
CLIENT_SECRET = env.LINKEDIN_CLIENT_SECRET

PIVOTS = metrics.PIVOTS

# Email settings
EMAIL_USER = env.EMAIL_USER
EMAIL_PASS = env.EMAIL_PASS
SMTP_SERVER = env.SMTP_SERVER
SMTP_PORT = env.SMTP_PORT
EMAIL_RECIPIENT = env.EMAIL_RECIPIENT

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

sm_client = secretmanager.SecretManagerServiceClient()

# ======================================================================
# LinkedIn data fetch + flatten
# ======================================================================
def get_yesterday_date_parts():
    date = datetime.now(timezone.utc).date()
    yesterday = date - timedelta(days=1)
    return yesterday.year, yesterday.month, yesterday.day, yesterday.isoformat()

START_DATE = get_yesterday_date_parts()[3]
END_DATE = START_DATE

valid_table_names = [list(t.keys())[0] for t in TABLE_IDS]

print("\nYou may enter the following arguments when executing this script:\n")
print("  --start-date YYYY-MM-DD : The start date for data fetching (default: yesterday)\n")
print("  --end-date YYYY-MM-DD   : The end date for data fetching (default: yesterday)\n")
print("  --table TABLE_NAME      : The specific table to fetch data for (defaults to all tables)\n")

# If no argument was specified prompt the user
if len(sys.argv) == 1:
    print("No arguments specified, using default values.\n")
    print(f"Using START_DATE: {START_DATE}")
    print(f"Using END_DATE: {END_DATE}\n")
    print(f"Using all tables: {', '.join(valid_table_names)}\n")
    # Ask if exit or continue
    user_input = input("Press Enter to continue with these settings, or type 'exit' to quit: ")
    if user_input.lower() == 'exit':
        sys.exit(0)

# Validate date format from command line args if provided
date_validator = r'^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$'

# Find if --start-date is among the arguments and return its position
if '--start-date' in sys.argv:
    start_date_index = sys.argv.index('--start-date') + 1
    if start_date_index < len(sys.argv):
        START_DATE = sys.argv[start_date_index]
    else:
        print("Error: --start-date argument provided but no date found.")
        sys.exit(1)
    # Validate date format
    if (not re.match(date_validator, sys.argv[start_date_index])):
        print(f"Invalid --start-date format, must be YYYY-MM-DD")
        sys.exit(1)

# Find if --end-date is among the arguments and return its position    
if '--end-date' in sys.argv:
    end_date_index = sys.argv.index('--end-date') + 1
    if end_date_index < len(sys.argv):
        END_DATE = sys.argv[end_date_index]
    else:
        print("Error: --end-date argument provided but no date found.")
        sys.exit(1)
    # Validate date format
    if (not re.match(date_validator, sys.argv[end_date_index])):
        print(f"Invalid --end-date format, must be YYYY-MM-DD")
        sys.exit(1)

# Check if START_DATE is before END_DATE
if (START_DATE > END_DATE):
    print("Error: --start-date must be before or equal to --end-date")
    sys.exit(1)

# Check if --table is among the arguments
if '--table' in sys.argv:
    table_index = sys.argv.index('--table') + 1
    if table_index < len(sys.argv):
        table_name = sys.argv[table_index]
        # Get the valid table names from TABLE_IDS
        if table_name not in valid_table_names:
            print(f"Error: --table argument provided but table '{table_name}' is not valid.")
            print(f"Valid tables are: {', '.join(valid_table_names)}")
            sys.exit(1)
        # Filter TABLE_IDS to only include the specified table
        TABLE_IDS = [t for t in TABLE_IDS if list(t.keys())[0] == table_name]
    else:
        print("Error: --table argument provided but no table name found.")
        sys.exit(1)
    print(f"--table found, using table: {table_name}")

# ======================================================================
# Secret Manager helpers
# ======================================================================
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
# BigQuery helpers
# ======================================================================
bq_client = bigquery.Client(project=PROJECT_ID)

# ======================================================================
# Ensure dataset and table exist
# ======================================================================
def ensure_dataset_and_table():
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    try:
        bq_client.get_dataset(dataset_ref)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        print(f"Couldn't find dataset: {DATASET_ID}")
        exit(1)

    for table_info in TABLE_IDS:
        for TABLE_ID, table_config in table_info.items():
            table_ref = bq_client.dataset(DATASET_ID).table(TABLE_ID)
            try:
                bq_client.get_table(table_ref)
            except NotFound:
                print(f"Couldn't find table: {TABLE_ID}")
                exit(1)

# ======================================================================
# LinkedIn data fetch
# ======================================================================
def fetch_linkedin_analytics(date, metrics=[], pivots=[]):
    print(f"Fetching LinkedIn analytics for {date} with metrics: {metrics} and pivots: {pivots}")
    date = datetime.strptime(date, "%Y-%m-%d").date()
    start_date = date
    end_date = date

    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN_SECRET}",
        "LinkedIn-Version": "202510",
        "Content-Type": "application/json",
        "X-Restli-Protocol-Version": "2.0.0"
    }

    q = "statistics"
    qPivots = ""
    metrics.insert(0, "pivotValues")
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
    return r.json()

# ======================================================================
# Data flattening and insertion
# ======================================================================
def flatten_linkedin_response(json_data, date):
    rows = []
    date = datetime.strptime(date, "%Y-%m-%d").date()

    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN_SECRET}",
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

# ======================================================================
# Insert rows into BigQuery
# ======================================================================
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
# Delete records in date range
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
# Cloud Function entrypoint
# ======================================================================
def jc_linkedin_to_bq(request):
    try:
        # Ensure BigQuery dataset and table exist
        ensure_dataset_and_table()

        # Ensure we have a valid access token (refresh if needed)
        # token = get_valid_access_token()

        # Delete existing records for that date to avoid duplicates
        # delete_records_in_date_range(START_DATE, END_DATE)
        _, _, _, date = get_yesterday_date_parts()

        # Delete existing records for that date to avoid duplicates
        delete_records_in_date_range(START_DATE, END_DATE)

        n_rows = 0
        for date_to_process in pd.date_range(START_DATE, END_DATE):
            # Fetch LinkedIn analytics for yesterday
            date_str = date_to_process.strftime("%Y-%m-%d")
            data = fetch_linkedin_analytics(ACCESS_TOKEN_SECRET, date_str)
            # Flatten and insert
            rows = flatten_linkedin_response(data, date_to_process)
            insert_rows_into_bq(rows)
            n_rows += len(rows)

        send_email(
            EMAIL_RECIPIENT, 
            "LinkedIn Data Ingestion", 
            (
                f"Inserted {n_rows} rows into BigQuery.\n"
                f"Date processed: {date}\n"
                f"Account Name: {ACCOUNT_NAME}\n"
                f"Dataset: {DATASET_ID}\n"
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

# ======================================================================
# LinkedIn API call for specific date
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
# Debugging helpers
# ======================================================================
def print_linkedin_response(json_data):
    for element in json_data.get("elements", []):
        # print("Pivot Values:", element.get("pivotValues"))
        # print("Impressions:", element.get("impressions"))
        for key, value in element.items():
            if key != "pivotValues":
                print(f"{key}: {value}")
        print("-" * 40)

# ======================================================================
# Get LinkedIn metrics for a date
# ======================================================================
def get_linkedin_metrics(access_token, date, metrics=[], pivots=[]):
    r = get_linkedin_analytics_for_date(
            access_token,
            date,
            metrics,
            pivots
        )
    rows = flatten_linkedin_response(r, date)
    return rows

# ======================================================================
# Cloud Function entrypoint for local execution
# ======================================================================
def local_linkedin_to_bq(request):
    try:
        # Ensure BigQuery dataset and table exist
        ensure_dataset_and_table()

        # Ensure we have a valid access token (refresh if needed)
        # token = get_valid_access_token()
        valid_access_token = ACCESS_TOKEN_SECRET

        global ACCOUNT_NAME
        ACCOUNT_NAME = getAccountName(ACCOUNT_ID, valid_access_token)
        print(f"Using LinkedIn Account Name: {ACCOUNT_NAME}")

        # Delete existing records for that date to avoid duplicates
        # delete_records_in_date_range(START_DATE, END_DATE)
        _, _, _, date = get_yesterday_date_parts()

        emailLogs = []

        # Set the number of tables to process
        nTables = len(TABLE_IDS)
        print(f"Number of tables to process: {nTables}")
        nTableProcessing = 0

        for table_info in TABLE_IDS:
            for TABLE_ID, table_config in table_info.items():
                nTableProcessing += 1
                print("="*40)
                print(f"Processing table: {nTableProcessing} of {nTables} - {TABLE_ID}")
                # Delete existing records for that date to avoid duplicates
                delete_records_in_date_range(START_DATE, END_DATE, TABLE_ID)

                n_rows = 0
                metrics = table_config.get("metrics", [])
                for date_to_process in pd.date_range(START_DATE, END_DATE):
                    # Fetch LinkedIn analytics for yesterday
                    date_str = date_to_process.strftime("%Y-%m-%d")
                    rows = get_linkedin_metrics(valid_access_token, date_str, metrics=metrics, pivots=PIVOTS)
                    inserted_rows = insert_rows_into_bq(rows, TABLE_ID)

                    n_rows += inserted_rows
                    emailLogs.append("=" * 50)
                    emailLogs.append(f"Inserted {inserted_rows} rows into table ({TABLE_ID}) of BigQuery dataset {DATASET_ID} for date {date_str}")

        send_email(
            EMAIL_RECIPIENT, 
            "LinkedIn Data Ingestion", 
            (
                "Manual run\n"
                f"Dates processed: {START_DATE} to {END_DATE}\n"
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
                f"Dates processed: {START_DATE} to {END_DATE}\n"
                f"Account Name: {ACCOUNT_NAME}\n"
                f"Dataset: {DATASET_ID}\n"
            )
        )
        return (f"Error: {e}", 500)

# Start here for local execution
local_linkedin_to_bq('')