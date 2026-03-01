import pandas as pd
from sqlalchemy import create_engine, String, Float, MetaData, Table, Column, Integer

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
    indicator_id AS indicator_code,
    value AS value,
    obs_status AS obs_status,
    decimal AS decimal,
    country_id AS country_code
FROM worldbank_debt;
"""
df_debt = pd.read_sql(query, engine)
print(f"📊 Extracted {len(df_debt)} rows for dsp_ft_debt")

# --- Save cleaned data ---
df_debt.to_sql(
    "dsp_ft_debt",
    engine,
    if_exists="replace",
    index=False,
    dtype={
        "indicator_code": String,
        "value": Float,
        "obs_status": String,
        "decimal": Integer,
        "country_code": String,
    }
)