## Script to download and clean current congressional district data


import requests
import geopandas as gpd
import pandas as pd
from shapely.geometry import shape
from pathlib import Path
from shapely.validation import make_valid

# ArcGIS Feature Service endpoint
url = "https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/NTAD_Congressional_Districts/FeatureServer/0/query"

# Query parameters
params = {
    "where": "1=1",
    "outFields": "*",
    "f": "json",
    "returnGeometry": "true",
    "outSR": 4269 # NAD83 (Census/TIGER standard)
}

print("Downloading congressional district data...")

# Get data from ArcGIS API
r = requests.get(url, params=params)
r.raise_for_status()

data = r.json()

print(f"Records returned from API: {len(data['features'])}")

# Convert ArcGIS JSON to GeoDataFrame
features = []
for feature in data["features"]:
    geom = feature.get("geometry")
    row = feature["attributes"]
    row["geometry"] = shape({
        "type": "Polygon",
        "coordinates": geom["rings"]
    })
    features.append(row)

# Create GeoDataFrame
districts = gpd.GeoDataFrame(
    features,
    geometry="geometry",
    crs="EPSG:4269"   # NAD83 (Census/TIGER standard)
)

# Fix invalid geometries
districts["geometry"] = districts["geometry"].apply(make_valid)

print("Invalid geometries remaining:")
print((~districts.geometry.is_valid).sum())

#Create readable representative name column
districts["rep_name"] = (
    districts["FIRSTNAME"].fillna("") + " " +
    districts["MIDDLENAME"].fillna("") + " " +
    districts["LASTNAME"].fillna("")
).str.split().str.join(" ")

#The ZZ districts are those that are not assigned to a specific congressional district 
# and therefore do not have a congressman assigned to them. We will remove these from the dataset for our purposes.
districts = districts[~districts["GEOID"].str.endswith("ZZ", na=False)]

def ordinal(n):
    if 10 <= n % 100 <= 20:
        return f"{n}th"
    return f"{n}{['th', 'st', 'nd', 'rd', 'th', 'th', 'th', 'th', 'th', 'th'][n % 10]}"

# Load state lookup table from Census Bureau
# In 2030, change this to the 2030 version of the file
state_url = "https://www2.census.gov/geo/docs/reference/codes2020/national_state2020.txt"
states = pd.read_csv(state_url, sep="|", dtype={"STATEFP": str})
state_lookup = dict(zip(states["STATEFP"], states["STATE_NAME"]))

districts["state_name"] = districts["STATEFP"].map(state_lookup)

def make_district_pretty(row):
    state_name = row["state_name"]
    district_num = row["OFFICE_ID"][2:]

    if district_num == "00":
        return state_name
    else:
        return f"{state_name} {ordinal(int(district_num))}"

districts["district_pretty"] = districts.apply(make_district_pretty, axis=1)

# Keep only the columns needed for the final layer
districts = districts[
    [
        "GEOID",
        "CDSESSN",        # congressional session (119 means 119th Congress)
        "rep_name",
        "district_pretty",
        "PARTY",
        "BIOGUIDE_ID",    # unique id for each congressperson
        "geometry",       # what is actually used for QGIS mapping
        "COMMITTEE_ASSIGNMENTS"
    ]
]

#Pivot the committee assignments into dummy variables
committee_dummies = (
    districts["COMMITTEE_ASSIGNMENTS"]
    .str.get_dummies(sep=";")
)
committee_dummies.columns = (
    committee_dummies.columns
    .str.lower()
    .str.replace(r"[^a-z0-9]+", "_", regex=True)
    .str.strip("_")
)
districts = pd.concat(
    [
        districts.drop(columns=["COMMITTEE_ASSIGNMENTS"]),
        committee_dummies
    ],
    axis=1
)

print("Finished!")
print("Shape:", districts.shape)

print("\nColumns:")
print(districts.columns.tolist())

print("\nFirst rows:")
print(districts.head())

#output
output_dir = Path("outputs")
output_dir.mkdir(exist_ok=True)
districts.to_file(
    output_dir / "congressional_districts.gpkg",
    driver="GPKG"
)
districts.to_file(
    output_dir / "congressional_districts.geojson",
    driver="GeoJSON"
)
districts.drop(columns="geometry").to_csv(
    output_dir / "congressional_districts.csv",
    index=False
)
print(f"\nSaved files to {output_dir.resolve()}")

#Sanity checks
print(f"Districts: {len(districts)}")
print(f"Unique GEOIDs: {districts['GEOID'].nunique()}")
print(f"Representatives: {districts['rep_name'].nunique()}")
