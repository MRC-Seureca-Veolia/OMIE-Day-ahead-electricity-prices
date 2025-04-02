import os
import requests
from datetime import datetime, timedelta

# 🌍 Create a folder to store data
data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

# 📅 Target: tomorrow's date
tomorrow = datetime.utcnow().date() + timedelta(days=1)
yyyymmdd = tomorrow.strftime("%Y%m%d")

# 🔄 Try versions .1 and .2
for version in ["1", "2"]:
    filename = f"marginalpdbc_{yyyymmdd}.{version}"
    filepath = os.path.join(data_dir, filename)

    # Skip if already downloaded
    if os.path.exists(filepath):
        print(f"✅ Already exists: {filename}")
        break

    # 🌐 Download attempt
    url = f"https://www.omie.es/es/file-download?parents=marginalpdbc&filename={filename}"
    response = requests.get(url)

    if response.status_code == 200:
        with open(filepath, "wb") as f:
            f.write(response.content)
        print(f"✅ Downloaded: {filename}")
        break
    else:
        print(f"❌ Not available: {filename} — {response.status_code}")
