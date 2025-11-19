import subprocess
import sys
import shutil
import env

# ======================================================================
# Deploy Google Cloud Function
# ======================================================================
def deploy_application():
    # Try to locate gcloud automatically
    gcloud_path = shutil.which("gcloud")
    if not gcloud_path:
        print("Error: 'gcloud' command not found. Make sure Google Cloud SDK is installed and in your PATH.")
        sys.exit(1)

    # Construct the deploy command as a list
    command = [
        gcloud_path,
        "functions",
        "deploy",
        "jc_linkedin_to_bq",
        "--runtime", "python311",
        "--trigger-http",
        "--timeout", "540s",
        "--allow-unauthenticated",
        "--region", "us-east1",
        "--entry-point", "jc_linkedin_to_bq",
        "--set-env-vars",
        f"GCP_PROJECT_ID={env.GCP_PROJECT_ID},"
        f"LINKEDIN_CLIENT_ID={env.LINKEDIN_CLIENT_ID},"
        f"LINKEDIN_CLIENT_SECRET={env.LINKEDIN_CLIENT_SECRET},"
        f"LINKEDIN_ACCOUNT_ID={env.LINKEDIN_ACCOUNT_ID},"
        f"ACCESS_TOKEN_SECRET={env.ACCESS_TOKEN_SECRET},"
        f"REFRESH_TOKEN_SECRET={env.REFRESH_TOKEN_SECRET},"
        f"BIGQUERY_DATASET={env.BIGQUERY_DATASET},"
        f"EMAIL_USER={env.EMAIL_USER},"
        f"EMAIL_PASS={env.EMAIL_PASS},"
        f"SMTP_SERVER={env.SMTP_SERVER},"
        f"SMTP_PORT={env.SMTP_PORT},"
        f"EMAIL_RECIPIENT={env.EMAIL_RECIPIENT}"

    ]

    print("Deploying Google Cloud Function...")
    try:
        # Run the command
        subprocess.run(command, check=True)
        print("Deployment completed successfully!")
    except subprocess.CalledProcessError as e:
        print(f"Deployment failed with exit code {e.returncode}")
        sys.exit(e.returncode)

if __name__ == "__main__":
    deploy_application()
