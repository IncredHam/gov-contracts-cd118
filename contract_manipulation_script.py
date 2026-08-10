#columns to keep:
keep_cols = [
    "contract_transaction_unique_key",
    "contract_award_unique_key",
    "award_id_piid",
    "modification_number",

    "federal_action_obligation",
    "total_dollars_obligated",
    "total_outlayed_amount_for_overall_award",
    "current_total_value_of_award",
    "potential_total_value_of_award",

    "action_date",
    "action_date_fiscal_year",
    "period_of_performance_start_date",
    "period_of_performance_current_end_date",

    "awarding_agency_code",
    "awarding_agency_name",
    "awarding_sub_agency_code",
    "awarding_sub_agency_name",
    "awarding_office_code",
    "awarding_office_name",

    "funding_agency_code",
    "funding_agency_name",
    "funding_sub_agency_code",
    "funding_sub_agency_name",

    "recipient_uei",
    "recipient_duns",
    "recipient_name",
    "recipient_name_raw",

    "recipient_parent_uei",
    "recipient_parent_duns",
    "recipient_parent_name",

    "recipient_country_code",
    "recipient_country_name",
    "recipient_address_line_1",
    "recipient_city_name"
    "recipient_county_name",
    "recipient_state_code",
    "recipient_state_name",
    "recipient_zip_4_code",

    "prime_award_transaction_recipient_cd_original",
    "prime_award_transaction_recipient_cd_current",

    "award_or_idv_flag",
    "award_type_code",
    "award_type",

    "product_or_service_code",
    "product_or_service_code_description",

    "naics_code",
    "naics_description",
    "transaction_description",
    "prime_award_base_transaction_description",
    "action_type_code",
    "action_type",
]

import duckdb

con = duckdb.connect()

con.execute("""
COPY (
    SELECT DISTINCT
        TRIM(recipient_uei) AS uei,
        TRIM(recipient_name) AS name,

        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    REGEXP_REPLACE(
                                        REGEXP_REPLACE(
                                            REGEXP_REPLACE(
                                                REGEXP_REPLACE(
                                                    REGEXP_REPLACE(
                                                        UPPER(TRIM(recipient_address_line_1)),
                                                    '[,.]',
                                                        '',
                                                        'g'
                                                ),
                                                    '\\s+(STE|SUITE|UNIT|APT|APARTMENT|BLDG|BLDNG)\\b.*$', 
                                                    ''
                                                ),
                                                '\\bSTREET\\b', 'ST'
                                            ),
                                            '\\bAVENUE\\b', 'AVE'
                                    ),
                                        '\\bHIGHWAY\\b', 'HWY'
                                    ),
                                    '\\bROAD\\b', 'RD'
                                ),
                                '\\bPLACE\\b', 'PL'
                            ),
                            '\\bBOULEVARD\\b', 'BLVD'
                        ),
                        '\\bDRIVE\\b', 'DR'
                    ),
                    '\\bLANE\\b', 'LN'
                ),
                '\\bCIRCLE\\b', 'CIR'
            ),
            '\\s*#.*$', ''
        ) AS address,

        UPPER(TRIM(recipient_city_name)) AS city,
        UPPER(TRIM(recipient_state_code)) AS state,
        LEFT(TRIM(recipient_zip_4_code), 5) AS zip

    FROM 'master.parquet'
)
TO 'unique_recipient_addresses_script.csv'
(FORMAT CSV, HEADER TRUE);
""")

con.close()

import pandas as pd

df = pd.read_csv("unique_recipient_addresses_script.csv",
                 dtype={"zip":"string"}
            )

# Create normalized address
df["geo_address"] = (
    df["address"]
    .str.upper()
    .str.replace(r"\bWEST\b", "W", regex=True)
    .str.replace(r"\bEAST\b", "E", regex=True)
    .str.replace(r"\bNORTH\b", "N", regex=True)
    .str.replace(r"\bSOUTH\b", "S", regex=True)
)

# Unique physical addresses
addresses = (
    df[["geo_address", "city", "state", "zip"]]
    .drop_duplicates()
    .reset_index(drop=True)
)

addresses["address_id"] = addresses.index + 1

df = df.merge(
    addresses,
    on=["geo_address", "city", "state", "zip"],
    how="left"
)

df.to_csv("uei_name_lookup.csv", index=False)

addresses.to_csv("census_geocoder_input_from_script.csv", index=False)



## latitude and longitude

import re
import unicodedata

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
    name = re.sub(
        r"\s+(COUNTY|MUNICIPIO|BOROUGH|PARISH|CENSUS AREA|MUNICIPALITY|CITY AND BOROUGH|PLANNING REGION)$",
        "",
        name
    )

    # Remove spaces and punctuation
    name = re.sub(r"[^A-Z0-9]", "", name)

    return name

county_gaz["county_match"] = county_gaz["NAME"].apply(normalize_county)

master_counties = con.execute("""
    SELECT DISTINCT
        clean_state,
        recipient_county_name
    FROM 'master_geocoded_tmp.parquet'
    WHERE recipient_county_name IS NOT NULL
""").fetchdf()

master_counties["county_match"] = (
    master_counties["recipient_county_name"]
    .apply(normalize_county)
)

# Create a state + county matching key
master_counties["county_key"] = (
    master_counties["clean_state"].str.upper()
    + "|"
    + master_counties["county_match"]
)

county_gaz["county_key"] = (
    county_gaz["USPS"].str.upper()
    + "|"
    + county_gaz["county_match"]
)

# Compare your county names against the Census Gazetteer
matched = master_counties.merge(
    county_gaz[["county_key", "NAME", "GEOID"]],
    on="county_key",
    how="left"
)

# Show counties that DID NOT match
unmatched_counties = (
    matched[matched["GEOID"].isna()]
    [["clean_state", "recipient_county_name"]]
    .drop_duplicates()
    .sort_values(["clean_state", "recipient_county_name"])
)

print(f"Total unique counties in your data: {len(master_counties)}")
print(f"Matched: {matched['GEOID'].notna().sum()}")
print(f"Unmatched: {matched['GEOID'].isna().sum()}")

print("\nUnmatched counties:")
print(unmatched_counties.to_string(index=False))

# ------------------------------------------------------------
# Prepare ZCTA lookup
# ------------------------------------------------------------

gaz_file = "2025_Gaz_zcta_national.txt"

zcta = con.execute(f"""
SELECT DISTINCT
    TRIM(GEOID) AS clean_zip,
    CAST(INTPTLAT AS DOUBLE) AS zip_lat,
    CAST(INTPTLONG AS DOUBLE) AS zip_lon
FROM read_csv(
    '{gaz_file}',
    delim='|',
    header=true
)
WHERE GEOID IS NOT NULL
""").fetchdf()

zcta.to_parquet(
    "zcta_lookup.parquet",
    index=False
)


# ------------------------------------------------------------
# Prepare county lookup
# ------------------------------------------------------------

county_gaz["county_match"] = (
    county_gaz["NAME"].apply(normalize_county)
)

county_gaz["county_key"] = (
    county_gaz["USPS"].astype(str).str.upper().str.strip()
    + "|"
    + county_gaz["county_match"].astype(str)
)

county_lookup = county_gaz[
    [
        "county_key",
        "GEOID",
        "INTPTLAT",
        "INTPTLONG"
    ]
].copy()

county_lookup["INTPTLAT"] = pd.to_numeric(
    county_lookup["INTPTLAT"],
    errors="coerce"
)

county_lookup["INTPTLONG"] = pd.to_numeric(
    county_lookup["INTPTLONG"],
    errors="coerce"
)

county_lookup.to_parquet(
    "county_lookup.parquet",
    index=False
)


# ------------------------------------------------------------
# Prepare normalized county names from master data
# ------------------------------------------------------------

master_counties = con.execute("""
SELECT DISTINCT
    clean_state,
    recipient_county_name
FROM 'master_geocoded.parquet'
WHERE recipient_county_name IS NOT NULL
""").fetchdf()

master_counties["county_match"] = (
    master_counties["recipient_county_name"]
    .apply(normalize_county)
)

master_counties["county_key"] = (
    master_counties["clean_state"].astype(str).str.upper().str.strip()
    + "|"
    + master_counties["county_match"].astype(str)
)

master_counties.to_parquet(
    "master_county_lookup.parquet",
    index=False
)


print("ZCTA lookup:", len(zcta))
print("County lookup:", len(county_lookup))
print("Master county lookup:", len(master_counties))

import duckdb

con = duckdb.connect()

con.execute("""
COPY (

WITH

-- Highest confidence
uei_address AS (
    SELECT
        clean_uei,
        clean_address,
        clean_city,
        clean_state,
        ANY_VALUE(lat) AS lat,
        ANY_VALUE(lon) AS lon
    FROM 'master_geocoded.parquet'
    WHERE
        lat IS NOT NULL
        AND clean_address IS NOT NULL
        AND clean_city IS NOT NULL
        AND clean_state IS NOT NULL
    GROUP BY clean_uei, clean_address, clean_city, clean_state
),

uei_zip AS (
    SELECT
        clean_uei,
        clean_zip,
        ANY_VALUE(lat) AS lat,
        ANY_VALUE(lon) AS lon
    FROM 'master_geocoded.parquet'
    WHERE
        lat IS NOT NULL
        AND clean_zip IS NOT NULL
    GROUP BY clean_uei, clean_zip
),

uei_city AS (
    SELECT
        clean_uei,
        clean_city,
        clean_state,
        ANY_VALUE(lat) AS lat,
        ANY_VALUE(lon) AS lon
    FROM 'master_geocoded.parquet'
    WHERE
        lat IS NOT NULL
        AND clean_city IS NOT NULL
        AND clean_state IS NOT NULL
    GROUP BY clean_uei, clean_city, clean_state
),

uei_cd AS (
    SELECT
        clean_uei,
        prime_award_transaction_recipient_cd_current AS current_cd,
        ANY_VALUE(lat) AS lat,
        ANY_VALUE(lon) AS lon
    FROM 'master_geocoded.parquet'
    WHERE
        lat IS NOT NULL
        AND prime_award_transaction_recipient_cd_current IS NOT NULL
    GROUP BY clean_uei, current_cd
),

uei_state AS (
    SELECT
        clean_uei,
        clean_state,
        ANY_VALUE(lat) AS lat,
        ANY_VALUE(lon) AS lon
    FROM 'master_geocoded.parquet'
    WHERE
        lat IS NOT NULL
        AND clean_state IS NOT NULL
    GROUP BY clean_uei, clean_state
),

cd_zip AS (
    SELECT
        prime_award_transaction_recipient_cd_current AS current_cd,
        clean_zip,
        ANY_VALUE(lat) AS lat,
        ANY_VALUE(lon) AS lon
    FROM 'master_geocoded.parquet'
    WHERE
        lat IS NOT NULL
        AND clean_zip IS NOT NULL
        AND prime_award_transaction_recipient_cd_current IS NOT NULL
    GROUP BY current_cd, clean_zip
),

cd_city AS (
    SELECT
        prime_award_transaction_recipient_cd_current AS current_cd,
        clean_city,
        ANY_VALUE(lat) AS lat,
        ANY_VALUE(lon) AS lon
    FROM 'master_geocoded.parquet'
    WHERE
        lat IS NOT NULL
        AND clean_city IS NOT NULL
        AND prime_award_transaction_recipient_cd_current IS NOT NULL
    GROUP BY current_cd, clean_city
),

zip AS (
    SELECT
        clean_zip,
        ANY_VALUE(lat) AS lat,
        ANY_VALUE(lon) AS lon
    FROM 'master_geocoded.parquet'
    WHERE
        lat IS NOT NULL
        AND clean_zip IS NOT NULL
    GROUP BY clean_zip
),

cd AS (
    SELECT
        prime_award_transaction_recipient_cd_current AS current_cd,
        ANY_VALUE(lat) AS lat,
        ANY_VALUE(lon) AS lon
    FROM 'master_geocoded.parquet'
    WHERE
        lat IS NOT NULL
        AND prime_award_transaction_recipient_cd_current IS NOT NULL
    GROUP BY current_cd
)

SELECT

    m.* EXCLUDE(lat, lon, match),

    -- ========================================================
    -- LATITUDE
    -- Existing priorities first.
    -- ZCTA and CD + County are LAST.
    -- ========================================================

    COALESCE(
        m.lat,
        ua.lat,          -- UEI + Address
        uz.lat,          -- UEI + Zip
        uc.lat,          -- UEI + City
        ucd.lat,         -- UEI + CD
        us.lat,          -- UEI + State
        cz.lat,          -- CD + Zip
        cc.lat,          -- CD + City
        z.lat,           -- Zip Only
        zcta.zip_lat,    -- ZCTA centroid
        c.INTPTLAT,      -- County centroid
        cd.lat           -- CD Only
    ) AS lat,

    -- ========================================================
    -- LONGITUDE
    -- Same priority order.
    -- ========================================================

    COALESCE(
        m.lon,
        ua.lon,
        uz.lon,
        uc.lon,
        ucd.lon,
        us.lon,
        cz.lon,
        cc.lon,
        z.lon,
        zcta.zip_lon,
        c.INTPTLONG,
        cd.lon
    ) AS lon,

    -- ========================================================
    -- MATCH TYPE
    -- ========================================================

    CASE
        WHEN m.lat IS NOT NULL THEN m.match
        WHEN ua.lat IS NOT NULL THEN 'UEI + Address'
        WHEN uz.lat IS NOT NULL THEN 'UEI + Zip'
        WHEN uc.lat IS NOT NULL THEN 'UEI + City'
        WHEN ucd.lat IS NOT NULL THEN 'UEI + CD'
        WHEN us.lat IS NOT NULL THEN 'UEI + State'
        WHEN cz.lat IS NOT NULL THEN 'CD + Zip'
        WHEN cc.lat IS NOT NULL THEN 'CD + City'
        WHEN z.lat IS NOT NULL THEN 'Zip Only'
        WHEN zcta.zip_lat IS NOT NULL THEN 'ZCTA Centroid'
        WHEN c.INTPTLAT IS NOT NULL THEN 'County'
        WHEN cd.lat IS NOT NULL THEN 'CD Only'
        ELSE NULL
    END AS match

FROM 'master_geocoded.parquet' m

-- ============================================================
-- EXISTING MATCHES
-- ============================================================

LEFT JOIN uei_address ua
    ON m.clean_uei = ua.clean_uei
    AND m.clean_address = ua.clean_address
    AND m.clean_city = ua.clean_city
    AND m.clean_state = ua.clean_state

LEFT JOIN uei_zip uz
    ON m.clean_uei = uz.clean_uei
    AND m.clean_zip = uz.clean_zip

LEFT JOIN uei_city uc
    ON m.clean_uei = uc.clean_uei
    AND m.clean_city = uc.clean_city
    AND m.clean_state = uc.clean_state

LEFT JOIN uei_cd ucd
    ON m.clean_uei = ucd.clean_uei
    AND m.prime_award_transaction_recipient_cd_current = ucd.current_cd

LEFT JOIN uei_state us
    ON m.clean_uei = us.clean_uei
    AND m.clean_state = us.clean_state

LEFT JOIN cd_zip cz
    ON m.prime_award_transaction_recipient_cd_current = cz.current_cd
    AND m.clean_zip = cz.clean_zip

LEFT JOIN cd_city cc
    ON m.prime_award_transaction_recipient_cd_current = cc.current_cd
    AND m.clean_city = cc.clean_city

LEFT JOIN zip z
    ON m.clean_zip = z.clean_zip

LEFT JOIN 'zcta_lookup.parquet' zcta
    ON m.clean_zip = zcta.clean_zip

LEFT JOIN 'master_county_lookup.parquet' mc
    ON m.clean_state = mc.clean_state
    AND m.recipient_county_name = mc.recipient_county_name

LEFT JOIN 'county_lookup.parquet' c
    ON mc.county_key = c.county_key

LEFT JOIN cd
    ON m.prime_award_transaction_recipient_cd_current = cd.current_cd

-- ============================================================
-- FILTER
-- ============================================================

WHERE
    m.clean_zip IS NOT NULL

    OR m.prime_award_transaction_recipient_cd_current IS NOT NULL

    OR COALESCE(
        m.lat,
        ua.lat,
        uz.lat,
        uc.lat,
        ucd.lat,
        us.lat,
        cz.lat,
        cc.lat,
        z.lat,
        zcta.zip_lat,
        c.INTPTLAT,
        cd.lat
    ) IS NOT NULL

)
TO 'master_geocoded_tmp.parquet'
(FORMAT PARQUET)
""")