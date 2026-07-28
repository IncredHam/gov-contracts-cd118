"""
Batch-geocode addresses via the Census Bureau's free batch geocoder.

Splits a large address CSV into <=10,000-row chunks, submits each to
the Census locations/addressbatch endpoint, and reassembles the
results into one CSV with matched lon/lat.

Requirements:
    pip install os time pandas requests

Usage:
    1. Edit INPUT_CSV below to point at your 125k unique-address file.
       It must have columns you can map to: id, street, city, state, zip
    2. Run: python census_batch_geocode.py
    3. Output: geocoded_addresses_full.csv

Notes:
- Resumable: if the script dies partway through, rerun it. It skips
  any batch that already has a saved result file.
- Test with a tiny sample (~10 rows) first to confirm your column
  mapping and that the endpoint is reachable from your machine.
- This uses the LOCATIONS endpoint (address -> lon/lat only), not the
  GEOGRAPHIES endpoint, on purpose. Assigning to congressional
  district is done afterward in QGIS via a spatial join against your
  own CD layer, which avoids relying on Census's internal layer-ID
  numbering for congressional districts.
"""

import os
import time
import pandas as pd
import requests

# ---- CONFIG -----------------------------------------------------------
INPUT_CSV = "unique_addresses.csv"     # address file
COLUMN_MAP = {                         # map your column names -> required names
    "location_id": "id",
    "recipient_address_line_1": "street",
    "recipient_city_name": "city",
    "recipient_state_code": "state",
    "zip5": "zip",
}
CHUNK_SIZE =10000
BENCHMARK = "Public_AR_Current"
CENSUS_URL = "https://geocoding.geo.census.gov/geocoder/locations/addressbatch"
BATCH_DIR = "geocode_batches"
RESULT_DIR = "geocode_results"
OUTPUT_CSV = "geocoded_addresses.csv"
POLITE_DELAY_SEC = 5                   # pause between submissions
MAX_RETRIES = 3
TIMEOUT_SEC = 900                      # 15 min per batch
# ------------------------------------------------------------------------

RESULT_COLS = [
    "id", "input_address", "match", "match_type",
    "matched_address", "lonlat", "tiger_line_id", "side",
]


def load_and_prepare(input_csv: str) -> pd.DataFrame:
    df = pd.read_csv(input_csv, dtype=str, encoding="utf-8")
    df = df.rename(columns=COLUMN_MAP)
    required = ["id", "street", "city", "state", "zip"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"Missing columns after mapping: {missing}. "
            f"Update COLUMN_MAP to match your file's headers."
        )
    return df[required]


def split_addresses(df: pd.DataFrame) -> list[str]:
    os.makedirs(BATCH_DIR, exist_ok=True)
    paths = []
    for i, start in enumerate(range(0, len(df), CHUNK_SIZE)):
        chunk = df.iloc[start:start + CHUNK_SIZE]
        path = os.path.join(BATCH_DIR, f"batch_{i:03d}.csv")
        chunk.to_csv(path, index=False, header=False, encoding = "utf-8")
        paths.append(path)
    print(f"Split {len(df)} addresses into {len(paths)} batch file(s).")
    return paths


def submit_batch(path: str) -> str:
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with open(path, "rb") as f:
                resp = requests.post(
                    CENSUS_URL,
                    files={"addressFile": (os.path.basename(path), f, "text/csv")},
                    data={"benchmark": BENCHMARK},
                    timeout=TIMEOUT_SEC,
                )
            resp.raise_for_status()
            return resp.text
        except requests.exceptions.RequestException as e:
            last_err = e
            print(f"  attempt {attempt}/{MAX_RETRIES} failed: {e}")
            time.sleep(30)
    raise RuntimeError(f"Failed to geocode {path}: {last_err}")


def reassemble(batch_paths: list[str]) -> pd.DataFrame:
    os.makedirs(RESULT_DIR, exist_ok=True)
    frames = []
    for i, path in enumerate(batch_paths):
        result_path = os.path.join(RESULT_DIR, f"result_{i:03d}.csv")
        if os.path.exists(result_path):
            print(f"[{i+1}/{len(batch_paths)}] already have {result_path}, skipping submit")
        else:
            print(f"[{i+1}/{len(batch_paths)}] submitting {path} ...")
            text = submit_batch(path)
            with open(result_path, "w", newline="", encoding="utf-8") as f:
                f.write(text)
            time.sleep(POLITE_DELAY_SEC)

        df = pd.read_csv(result_path, header=None, names=RESULT_COLS, dtype=str)
        frames.append(df)

    full = pd.concat(frames, ignore_index=True)
    lonlat = full["lonlat"].str.split(",", n=1, expand=True)
    full["lon"] = lonlat[0]
    full["lat"] = lonlat[1]
    full = full.drop(columns=["lonlat"])
    full.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")

    matched = (full["match"] == "Match").sum()
    print(f"\nDone: {len(full)} addresses processed, {matched} matched "
          f"({matched/len(full):.1%}), saved to {OUTPUT_CSV}")
    return full


if __name__ == "__main__":
    df = load_and_prepare(INPUT_CSV)
    batch_paths = split_addresses(df)
    reassemble(batch_paths)
