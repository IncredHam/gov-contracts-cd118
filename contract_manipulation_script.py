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