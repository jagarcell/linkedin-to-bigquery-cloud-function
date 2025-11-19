import requests
import json

# ==========================================
# 🔐 INSERT YOUR ACCESS TOKEN HERE
# ==========================================
ACCESS_TOKEN = "AQWMzHN325c5mJ4mRGe1Q15UWqLRz_-PfmxukHAUNCN5xlk530xCIKFIT6MAcRJfhX0Vy7_774TQtj1wMTI6mUdIsoy1RgPeROzbPXt2GozCxcfDL_aevfOthny8h9ucdbtFb0-dtksRrdvfxv6Ux9CF6mMFfDQUrwVMAxB8BSjHu0RNLoD405krYXxhHAYT-RyXF2lFcb8Vw4PaDmsPBlKi6oKZX-tGq35YOxRE1wE7Pd_gwmc-mgI_Pu3LLYm0PSiWDbmcihI6O6PkNb-3ULwf1hnewD7pnG_KCq4VwzHklDE7fa5jdJtczTkpKRAN_xxwCgGVFRjy2T52NDEm_KA8zNDk2w"

headers = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "LinkedIn-Version": "202510",
    "Content-Type": "application/json",
    "X-Restli-Protocol-Version": "2.0.0"
}

# LinkedIn endpoint to fetch available ad accounts
url = "https://api.linkedin.com/rest/adAccounts?q=search"

print("🔍 Checking accessible Ad Accounts...")

response = requests.get(url, headers=headers)

print("\n📡 Status Code:", response.status_code)

if response.status_code != 200:
    print("\n❗Error:")
    print(response.text)
else:
    data = response.json()

    if "elements" in data and len(data["elements"]) > 0:
        print("\n🧾 Accessible Ad Accounts:")
        for acc in data["elements"]:
            print("-" * 50)
            print("🆔 Account ID:", acc.get("id"))
            print("🏢 Name:", acc.get("name"))
            print("📌 Status:", acc.get("status"))
    else:
        print("\n🚫 No Ad Accounts found for this user/token.")
        print("   ➤ This user likely has NO admin/campaign roles on any ad accounts.")
