#!/usr/bin/env python3

import re
import zipfile
from datetime import datetime
from pathlib import Path
import pandas as pd
import duckdb

KEEP_COLS = [
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
    "recipient_city_name",
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

US_COUNTRY_VALUES = {
    "USA",
    "US",
    "UNITED STATES",
    "UNITED STATES OF AMERICA",
}

SOURCE_DIR = Path("dod_contract_archives")
TARGET_PATH = Path("master.parquet")


def normalize_text(series):
    return series.astype(str).str.strip().str.upper()

def filter_us_rows(df):
    if df.empty:
        return df
    mask = pd.Series(False, index=df.index)
    if "recipient_country_code" in df.columns:
        mask |= normalize_text(df["recipient_country_code"]).isin(US_COUNTRY_VALUES)
    if "recipient_country_name" in df.columns:
        mask |= normalize_text(df["recipient_country_name"]).isin(US_COUNTRY_VALUES)
    return df.loc[mask]

ZIP_DATE_PATTERN = re.compile(r"(\d{8})\.zip$", re.IGNORECASE)

def parse_zip_date(zip_path):
    match = ZIP_DATE_PATTERN.search(zip_path.name)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y%m%d").date()
    except ValueError:
        return None

def get_last_modified_date(zip_paths):
    dates = [
        date
        for date in (parse_zip_date(path) for path in zip_paths)
        if date is not None
    ]
    return max(dates) if dates else None

def read_parquet_last_modified_date(target_path):
    try:
        import pyarrow.parquet as pq
    except ImportError:
        return None
    try:
        metadata = pq.read_metadata(target_path).metadata
    except Exception:
        return None
    if not metadata:
        return None
    value = metadata.get(b"last_modified_date")
    if value is None:
        return None
    return value.decode("utf-8", errors="ignore")

def read_zip_csv_chunks(zip_path, keep_cols):
    with zipfile.ZipFile(zip_path, "r") as archive:
        for name in archive.namelist():
            if not name.lower().endswith(".csv"):
                continue
            with archive.open(name) as source_file:
                try:
                    header = pd.read_csv(source_file, nrows=0)
                except Exception:
                    continue
            available_columns = [col for col in keep_cols if col in header.columns]
            if not available_columns:
                continue
            with archive.open(name) as source_file:
                for chunk in pd.read_csv(
                    source_file,
                    usecols=available_columns,
                    dtype=str,
                    chunksize=100_000,
                    low_memory=False,
                ):
                    chunk = chunk.reindex(columns=keep_cols)
                    chunk = filter_us_rows(chunk)
                    if not chunk.empty:
                        yield chunk

def write_parquet(chunks, target_path, last_modified_date=None):
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        raise RuntimeError("pyarrow is required to write parquet output.")

    writer = None
    any_written = False
    for chunk in chunks:
        table = pa.Table.from_pandas(chunk, preserve_index=False)
        if writer is None:
            schema = table.schema
            if last_modified_date is not None:
                schema = schema.with_metadata(
                    {b"last_modified_date": last_modified_date.encode("utf-8")}
                )
            writer = pq.ParquetWriter(target_path, schema, compression="snappy")
        writer.write_table(table)
        any_written = True
    if writer is not None:
        writer.close()
    elif not target_path.exists():
        empty = pd.DataFrame(columns=KEEP_COLS)
        table = pa.Table.from_pandas(empty, preserve_index=False)
        if last_modified_date is not None:
            table = table.replace_schema_metadata(
                {b"last_modified_date": last_modified_date.encode("utf-8")}
            )
        pq.write_table(table, target_path, compression="snappy")

def main():
    if TARGET_PATH.exists():
        last_modified_date = read_parquet_last_modified_date(TARGET_PATH)
        if last_modified_date:
            print(f"{TARGET_PATH} exists. last modified date: {last_modified_date}")
        else:
            print(f"{TARGET_PATH} exists.")
        return

    if not SOURCE_DIR.exists():
        raise SystemExit(f"source directory {SOURCE_DIR} does not exist")

    zip_files = sorted(SOURCE_DIR.rglob("*.zip"))
    if not zip_files:
        raise SystemExit(f"no zip files found in {SOURCE_DIR}")

    last_modified_date = get_last_modified_date(zip_files)

    def chunk_generator():
        for zip_path in zip_files:
            print(f"processing {zip_path}")
            yield from read_zip_csv_chunks(zip_path, KEEP_COLS)

    try:
        write_parquet(chunk_generator(), TARGET_PATH, last_modified_date=last_modified_date.isoformat())
    except RuntimeError as exc:
        raise SystemExit(str(exc))

    print(f"wrote {TARGET_PATH}")

    con = duckdb.connect()
    con.execute("""
    COPY (
        SELECT 
            *,

            TRIM(recipient_uei) AS clean_uei,
            TRIM(recipient_name) AS clean_name,

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
                                                            REGEXP_REPLACE(
                                                                REGEXP_REPLACE(
                                                                    REGEXP_REPLACE(
                                                                        REGEXP_REPLACE(
                                                                            UPPER(TRIM(recipient_address_line_1)),'[,.]','','g'
                                                                        ),
                                                                        '\\s+(STE|SUITE|UNIT|APT|APARTMENT|BLDG|BLDNG)\\b.*$', ''
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
                            ),
                            '\\bNORTH\\b','N'
                        ),
                        '\\bSOUTH\\b','S'
                    ),
                    '\\bEAST\\b','E'
                ),
                '\\bWEST\\b','W'
            ) AS clean_address,

            UPPER(TRIM(recipient_city_name)) AS clean_city,
            UPPER(TRIM(recipient_state_code)) AS clean_state,
            LEFT(TRIM(recipient_zip_4_code), 5) AS clean_zip

        FROM 'master.parquet'
    )
    TO 'master.parquet'
    (FORMAT PARQUET);
    """)

    con.close()
    print("Wrote cleaned data to master.parquet")

if __name__ == "__main__":
    main()