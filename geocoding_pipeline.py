# -----------------------------------------------------------------------------
# MASTER CONTRACT GEOCODING PIPELINE
#
# Updates master_geocoded.parquet with geocoded locations from master.parquet.
# The pipeline:
#   1. Identifies new unique addresses in master.parquet that have not previously
#      been geocoded.
#   2. Geocodes those addresses using the Census geocoder.
#   3. Combines new geocoding results with existing geocoding results.
#   4. Fills remaining unmatched locations using a hierarchy of existing
#      coordinates:
#         UEI + Address → UEI + Zip → UEI + City → UEI + CD → UEI + State
#         → CD + Zip → CD + City → Zip → ZCTA centroid → County centroid
#         → CD
#   5. Gives newly discovered higher-priority matches precedence over older
#      lower-priority matches.
#   6. Joins the resulting latitude/longitude and match method back to the
#      full transaction-level master.parquet and saves the result as
#      master_geocoded.parquet.
#
# master.parquet is the source of truth for all contract transactions;
# master_geocoded.parquet is the geographic/enriched version of that data.
# -----------------------------------------------------------------------------

import duckdb
import os
from pathlib import Path
import pandas as pd
from census_batch_geocode import geocode_df
import re
import unicodedata
import glob
import zipfile
import requests

MASTER_PATH = Path("master.parquet")
GEOCODED_PATH = Path("master_geocoded.parquet")
COUNTY_GAZETTEER_URL = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2025_Gazetteer/2025_Gaz_counties_national.zip"
COUNTY_GAZETTEER_ZIP = "2025_Gaz_counties_national.zip"
ZIP_GAZETTEER_URL = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2025_Gazetteer/2025_Gaz_zcta_national.zip"
ZIP_GAZETTEER = "2025_Gaz_zcta_national.txt"

def get_addresses_to_geocode():
    new_addresses = duckdb.sql(f"""
        SELECT DISTINCT
            clean_address AS address,
            clean_city AS city,
            clean_state AS state,
            clean_zip AS zip
        FROM '{MASTER_PATH}'
    """).df()

    if GEOCODED_PATH.exists():
        old_addresses = duckdb.sql(f"""
            SELECT DISTINCT
                clean_address AS address,
                clean_city AS city,
                clean_state AS state,
                clean_zip AS zip
            FROM '{GEOCODED_PATH}'
        """).df()
    else:
        new_addresses = new_addresses.reset_index(drop=True)
        new_addresses["address_id"] = new_addresses.index + 1
        return new_addresses

    # Find addresses that are in new_addresses but not in old_addresses
    addresses_to_geocode = pd.merge(
        new_addresses,
        old_addresses,
        on=["address", "city", "state", "zip"],
        how="left",
        indicator=True
    ).query('_merge == "left_only"').drop(columns=['_merge'])

    addresses_to_geocode = addresses_to_geocode.reset_index(drop=True)
    addresses_to_geocode["id"] = addresses_to_geocode.index + 1
    return addresses_to_geocode
    
def geocode_addresses(addresses_df):
    geocoded_results = geocode_df(
        addresses_df,
        id_col="id",
        street_col="address",
        city_col="city",
        state_col="state",
        zip_col="zip"
    )
    geocoded_df = pd.merge(
        addresses_df,
        geocoded_results,
        on=["id"],
        how="left"
    )
    return geocoded_df

def merge_geocoded_with_master(geocoded_df):
    master = duckdb.sql(f"""
            SELECT DISTINCT
                clean_uei AS uei,
                clean_name AS name,
                clean_address AS address,
                clean_city AS city,
                clean_state AS state,
                clean_zip AS zip,
                prime_award_transaction_recipient_cd_current AS cd,
                recipient_county_name AS county
            FROM '{MASTER_PATH}'
        """).df()

    master = pd.merge(
        master,
        geocoded_df[["address", "city", "state", "zip", "lat", "lon", "match"]],
        on=["address", "city", "state", "zip"],
        how = "left"
    )
    if GEOCODED_PATH.exists():
        old_geocoded = duckdb.sql(f"""
            SELECT DISTINCT
                clean_address AS address,
                clean_city AS city,
                clean_state AS state,
                clean_zip AS zip,
                lat,
                lon,
                match
            FROM '{GEOCODED_PATH}'
            WHERE match = 'Match'
        """).df()

        # Merge old geocoding onto current master
        master = master.merge(
            old_geocoded,
            on=["address","city","state","zip"],
            how="left",
            suffixes=("", "_old")
        )

        # Fill ONLY missing values
        null_lat = master["lat"].isna()
        master.loc[null_lat, ['lat', 'lon', 'match']] = master.loc[null_lat, ['lat_old', 'lon_old', 'match_old']].values
        master = master.drop(columns=["lat_old", "lon_old", "match_old"]) # Remove temporary columns

    return master

def normalize_county(name):
    if pd.isna(name):
        return None
    name = str(name).upper().strip()

    # Remove accents
    name = unicodedata.normalize("NFKD", name)
    name = "".join(
        c for c in name
        if not unicodedata.combining(c)
    )

    # Remove geographic suffix
    name = name.replace("(", " ").replace(")", " ")
    name = re.sub(
        r"\s+(COUNTY|MUNICIPIO|BOROUGH|PARISH|CENSUS AREA|MUNICIPALITY|CITY AND BOROUGH|PLANNING REGION|CITY)$",
        "",
        name
    )

    # Remove spaces and punctuation
    name = re.sub(r"[^A-Z0-9]", "", name)

    return name

def county_zcta_download():
    if not os.path.exists(COUNTY_GAZETTEER_ZIP):
        r = requests.get(COUNTY_GAZETTEER_URL)
        r.raise_for_status()

        with open(COUNTY_GAZETTEER_ZIP, "wb") as f:
            f.write(r.content)
    # Extract
    os.makedirs("county_gazetteer", exist_ok=True)

    if not os.listdir("county_gazetteer"):
        with zipfile.ZipFile(COUNTY_GAZETTEER_ZIP) as z:
            z.extractall("county_gazetteer")

    county_gaz_file = glob.glob("county_gazetteer/*.txt")[0]
    county_gaz = pd.read_csv(county_gaz_file, sep="|", dtype=str)
    county_gaz["county_match"] = county_gaz["NAME"].apply(normalize_county)
    county_gaz["county_key"] = (
        county_gaz["USPS"].str.upper().str.strip()
        + "|"
        + county_gaz["county_match"]
    )
    county_lookup = county_gaz[["county_key", "INTPTLAT", "INTPTLONG"]].copy()
    county_lookup["INTPTLAT"] = pd.to_numeric(county_lookup["INTPTLAT"], errors="coerce")
    county_lookup["INTPTLONG"] = pd.to_numeric(county_lookup["INTPTLONG"], errors="coerce")
    county_lookup = county_lookup.rename(columns={"INTPTLAT": "lat", "INTPTLONG": "lon"})

    if not os.path.exists(ZIP_GAZETTEER):
        r = requests.get(ZIP_GAZETTEER_URL)
        r.raise_for_status()

        with open(ZIP_GAZETTEER, "wb") as f:
            f.write(r.content)

    os.makedirs("zcta_gazetteer", exist_ok=True)

    if not os.listdir("zcta_gazetteer"):
        with zipfile.ZipFile(ZIP_GAZETTEER) as z:
            z.extractall("zcta_gazetteer")

    zip_gaz_file = glob.glob("zcta_gazetteer/*.txt")[0]
    zcta = pd.read_csv(zip_gaz_file, sep="|", dtype=str)
    zcta_lookup = zcta[["GEOID", "INTPTLAT", "INTPTLONG"]].copy()
    zcta_lookup = zcta_lookup.rename(columns={"GEOID": "zip", "INTPTLAT": "lat", "INTPTLONG": "lon"})

    return county_lookup, zcta_lookup

def fill_missing_lat_lon(master_df):
    county_lookup, zcta_lookup = county_zcta_download()
    known = master_df[master_df["lat"].notna()].copy()
    master = master_df.copy()
    def make_lookup(keys):
        return known.dropna(subset=keys).groupby(keys, as_index = False)[["lat","lon"]].first()
    def apply_match(keys, label):
        nonlocal master
        lookup = make_lookup(keys)
        master = master.merge(lookup, on = keys, how = "left", suffixes=("","_new"))
        to_update = master["lat"].isna() & master["lat_new"].notna()
        master.loc[to_update, ['lat', 'lon']] = master.loc[to_update, ['lat_new', 'lon_new']].values
        master.loc[to_update,"match"]=label
        master = master.drop(columns=["lat_new", "lon_new"]) # Remove temporary columns

    apply_match(["uei", "address", "city", "state"], "UEI + Address")
    apply_match(["uei", "zip"], "UEI + Zip")
    apply_match(["uei", "city", "state"], "UEI + City")
    apply_match(["uei", "cd"], "UEI + CD")
    apply_match(["uei", "state"], "UEI + State")
    apply_match(["cd", "zip"], "CD + Zip")
    apply_match(["cd", "city"], "CD + City")
    apply_match(["zip"], "Zip Only")

    # ZCTA Matches
    master = master.merge(
        zcta_lookup,
        on="zip",
        how="left",
        suffixes=("", "_zcta")
    )
    to_update = (master["lat"].isna() & master["lat_zcta"].notna())
    master.loc[to_update, ['lat', 'lon']] = master.loc[to_update, ['lat_zcta', 'lon_zcta']].values
    master.loc[to_update,"match"]="ZCTA Centroid"
    master = master.drop(columns=["lat_zcta", "lon_zcta"]) # Remove temporary columns

    # County Matches
    master["county_match"] = (master["county"].apply(normalize_county))
    master["county_key"] = (
        master["state"].str.upper().str.strip()
        + "|"
        + master["county_match"]
    )
    master = master.merge(
        county_lookup,
        on="county_key",
        how="left",
        suffixes=("", "_county")
    )

    to_update = (master["lat"].isna() & master["lat_county"].notna())
    master.loc[to_update, ['lat', 'lon']] = master.loc[to_update, ['lat_county', 'lon_county']].values
    master.loc[to_update,"match"]="County Centroid"
    master = master.drop(columns=["county_match","county_key","lat_county", "lon_county"]) # Remove temporary columns

    apply_match(["cd"], "CD Only") # CD only

    return master

def save_master_geocoded(master):
    con = duckdb.connect()
    con.register("new_geocoding", master)

    if GEOCODED_PATH.exists():
        old_geocoding = duckdb.sql(f"""
            SELECT
                clean_address AS address,
                clean_city AS city,
                clean_state AS state,
                clean_zip AS zip,
                ANY_VALUE(lat) AS lat,
                ANY_VALUE(lon) AS lon,
                ANY_VALUE(match) AS match
            FROM '{GEOCODED_PATH}'
            WHERE match != 'Match'
                AND lat is NOT NULL
            GROUP BY
                clean_address,
                clean_city,
                clean_state,
                clean_zip
        """).df()
    else: old_geocoding = pd.DataFrame(columns = ["address","city","state","zip","lat","lon","match"])
    con.register("old_geocoding", old_geocoding)

    con.execute(f"""
        COPY (
            SELECT
                m.*,
                COALESCE(n.lat,o.lat) AS lat,
                COALESCE(n.lon, o.lon) AS lon,
                CASE
                    WHEN n.lat IS NOT NULL THEN n.match
                    WHEN o.lat IS NOT NULL THEN o.match
                    ELSE NULL
                END AS match
            FROM '{MASTER_PATH}' m
            
            LEFT JOIN new_geocoding n
                ON m.clean_uei = n.uei
                AND m.clean_name = n.name
                AND m.clean_address = n.address
                AND m.clean_city = n.city
                AND m.clean_state = n.state
                AND m.clean_zip = n.zip
                AND m.prime_award_transaction_recipient_cd_current = n.cd
                AND m.recipient_county_name = n.county

            LEFT JOIN old_geocoding o
                ON m.clean_address = o.address
                AND m.clean_city = o.city
                AND m.clean_state = o.state
                AND m.clean_zip = o.zip
        )
        TO '{GEOCODED_PATH}'
        (FORMAT PARQUET)
    """)
    con.close()


def main():
    addresses_df = get_addresses_to_geocode()
    geocoded_df = geocode_addresses(addresses_df)
    master_df = merge_geocoded_with_master(geocoded_df)
    master = fill_missing_lat_lon(master_df)

    save_master_geocoded(master)
    matched = master["match"].notna().sum()
    print(f"\nDone: {len(master)} addresses processed, {matched} matched "
                f"({matched/len(master):.1%}), saved to master_geocoded.parquet")


if __name__ == "__main__":
    main()