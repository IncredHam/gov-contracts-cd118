"""
# DoD Historical Contract Archive Downloader

## Purpose
Downloads historical Department of Defense (DoD) prime contract transaction
archives from USAspending.gov for FY2008 through the current fiscal year.

The script uses the USAspending.gov bulk download archive endpoint to locate
pre-generated annual contract ZIP files and downloads them locally. These files
contain the complete DoD contract transaction history available through the
USAspending.gov archive.

The downloaded archives are intended to serve as the raw data layer for later
processing, filtering, geocoding, congressional district attribution, and
funding analysis.

## Data Source
USAspending.gov Award Data Archive:
https://www.usaspending.gov/download_center/award_data_archive

API endpoint used to identify archive files:
https://api.usaspending.gov/api/v2/bulk_download/list_monthly_files/


## Output
Downloaded ZIP files are saved to:

    ./dod_contract_archives/

Example output file: FY2008_097_Contracts_Full_YYYYMMDD.zip

## Dependencies

Python >= 3.9 recommended

Required packages:

    pip install requests

## Usage

Run:

    python master_contracts_download.py

The script will:
1. Determine the current fiscal year.
2. Query USAspending.gov for each fiscal year archive.
3. Identify the DoD full contract archive ZIP.
4. Download missing files.
5. Skip files that already exist locally.
6. Retry failed API requests and file downloads automatically.

## Download Reliability Features

Because large archive files can exceed several GB, the script includes:
- API request retries for temporary USAspending.gov failures.
- Download retries for interrupted transfers.
- Temporary ".part" files to prevent incomplete ZIP files from appearing
  as completed downloads.

## Notes
Raw archive files can require significant storage space.
Ensure sufficient disk capacity before running.
"""

import os
import requests
from datetime import date
import time

# USAspending endpoint
API_URL = "https://api.usaspending.gov/api/v2/bulk_download/list_monthly_files/"

# DoD top-tier agency ID, taken using inspect element from this website: https://www.usaspending.gov/download_center/award_data_archive
DOD_ID = 126

# Where to save files
OUT_DIR = "dod_contract_archives"
os.makedirs(OUT_DIR, exist_ok=True)


def current_fiscal_year():
    today = date.today()

    # FY changes on October 1
    if today.month >= 10:
        return today.year + 1
    else:
        return today.year


def download_file(url, filepath):

    #FY2025 kept timing out so part file allows progress to be saved:
    temp_path = filepath + ".part"

    print(f"Downloading {os.path.basename(filepath)}")

    for attempt in range(5):

        try:
            with requests.get(
                url,
                stream=True,
                timeout=(30, 600)
            ) as r:

                r.raise_for_status()

                with open(temp_path, "wb") as f:
                    for chunk in r.iter_content(
                        chunk_size=1024 * 1024
                    ):
                        if chunk:
                            f.write(chunk)

            os.rename(temp_path, filepath)

            print("Finished")
            return

        except requests.exceptions.RequestException as e:

            print(
                f"Download attempt {attempt+1}/5 failed: {e}"
            )

            if os.path.exists(temp_path):
                os.remove(temp_path)

            if attempt < 4:
                time.sleep(30)

            else:
                raise


start_fy = 2008 #earliest data available on usaspending.gov
end_fy = current_fiscal_year()

print(f"Downloading FY{start_fy}-FY{end_fy}")


for fy in range(start_fy, end_fy + 1):

    print(f"\nChecking FY{fy}...")

    payload = {
        "agency": DOD_ID,
        "fiscal_year": fy,
        "type": "contracts"
    }

    #response = requests.post(API_URL, json=payload)
    #response.raise_for_status()
    #Code above ^ was failing if requests were run too close together.
    for attempt in range(5):
        try:
            response = requests.post(
                API_URL,
                json=payload,
                timeout=60
            )
            response.raise_for_status()
            break

        except requests.exceptions.RequestException as e:
            print(f"API attempt {attempt + 1}/5 failed: {e}")

            if attempt == 4:
                raise

            time.sleep(10)

    files = response.json()["monthly_files"]

    # Find the full contracts archive
    full_files = [
        f for f in files
        if "Contracts_Full" in f["file_name"]
    ]

    if not full_files:
        print(f"No full contract file found for FY{fy}")
        continue

    file_info = full_files[0]

    filename = file_info["file_name"]
    url = file_info["url"]

    filepath = os.path.join(
        OUT_DIR,
        filename
    )

    if os.path.exists(filepath):
        print("Already downloaded:", filename)
        continue

    download_file(
        url,
        filepath
    )

print("\nAll downloads complete!")