import pandas as pd
from sqlalchemy import create_engine, String, Float, MetaData, Table, Column

# --- Database connection ---
db_name = "kaggle_test"
db_user = "kaggle_user"
db_password = "TEMPORARYPASSWORD"
db_host = "localhost"
db_port = 5432

engine = create_engine(
    f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}",
    echo=True
)

# --- Extract from worldbank_countries ---
query = """
SELECT
    id AS code,
    "iso2Code" AS iso2_code,
    name,
    "capitalCity" AS capital,
    longitude,
    latitude,
    "region.id" AS region_code,
    "adminregion.id" AS administrative_region_code,
    "incomeLevel.id" AS income_level_code
FROM worldbank_countries
WHERE "capitalCity" IS NOT NULL AND "capitalCity" != '';
"""
df_country = pd.read_sql(query, engine)
print(f"📊 Extracted {len(df_country)} rows for dsp_dm_country")

# --- Clean data ---
df_country["longitude"] = pd.to_numeric(df_country["longitude"], errors="coerce")
df_country["latitude"] = pd.to_numeric(df_country["latitude"], errors="coerce")

# --- Save cleaned data ---
df_country.to_sql(
    "dsb_dm_country",
    engine,
    if_exists="replace",
    index=False,
    dtype={
        "code": String,
        "iso2_code": String(2),
        "name": String,
        "capital": String,
        "longitude": Float,
        "latitude": Float,
        "region_code": String,
        "administrative_region_code": String,
        "income_level_code": String,
    }
)

# --- Extract from worldbank_countries ---
query2 = """
SELECT
    id AS code,
    "iso2Code" AS iso2_code,
    name
FROM worldbank_countries
WHERE "capitalCity" IS NULL OR "capitalCity" = '';
"""
df_subregion = pd.read_sql(query2, engine)
print(f"📊 Extracted {len(df_subregion)} rows for dsp_ft_country")