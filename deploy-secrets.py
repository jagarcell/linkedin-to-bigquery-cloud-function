import subprocess
import sys
import env  # import your environment variables
import shutil

def deploy_secrets():
        # Try to locate gcloud automatically
    gcloud_path = shutil.which("gcloud")
    if not gcloud_path:
        print("Error: 'gcloud' command not found. Make sure Google Cloud SDK is installed and in your PATH.")
        sys.exit(1)

    # List of secrets to add from env.py
    secrets = {
        "LINKEDIN_ACCESS_TOKEN": env.ACCESS_TOKEN_SECRET,
        "LINKEDIN_REFRESH_TOKEN": env.REFRESH_TOKEN_SECRET,
    }

    for name, value in secrets.items():
        # Create the secret if it doesn't already exist
        subprocess.run(
            [
                gcloud_path,
                "secrets", 
                "create", 
                name,
                "--replication-policy=automatic"
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False  # ignore if it already exists
        )

        # Add a new version with the value from env.py
        subprocess.run(
            [
                gcloud_path, 
                "secrets", 
                "versions", 
                "add", 
                name, 
                "--data-file=-"
            ],
            input=value.encode("utf-8"),
            check=True
        )

        print(f"✅ Added version for secret: {name}")
    print("🎉 All secrets have been deployed successfully.")

if __name__ == "__main__":
    deploy_secrets()
