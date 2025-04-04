import pandas as pd
import os
from glob import glob
import duckdb

# Setup
raw_folder = "data"
output_folder = "output"
os.makedirs(output_folder, exist_ok=True)

# Cleaning function
def clean_omie_file(filepath):
    df = pd.read_csv(filepath, sep=";", skiprows=1, header=None)
    df = df[~df.apply(lambda x: x.astype(str).str.contains('\*').any(), axis=1)]
    df = df.drop(columns=[6])
    df.columns = ["Year", "Month", "Day", "Hour", "Price1", "Price2"]
    df["Datetime"] = pd.to_datetime(df[["Year", "Month", "Day"]]) + pd.to_timedelta(df["Hour"] - 1, unit="h")
    df["Country"] = "Spain" if filepath.endswith(".1") else "Portugal"
    return df

# Clean and combine
files = glob(os.path.join(raw_folder, "*.1")) + glob(os.path.join(raw_folder, "*.2"))
all_dfs = [clean_omie_file(f) for f in files]
df = pd.concat(all_dfs).sort_values(["Datetime", "Country"]).reset_index(drop=True)

# Save
df.to_csv(f"{output_folder}/all_omie_prices.csv", index=False)
df.to_parquet(f"{output_folder}/all_omie_prices.parquet", index=False)

con = duckdb.connect(f"{output_folder}/omie_prices.duckdb")
con.execute("CREATE OR REPLACE TABLE prices AS SELECT * FROM df")
con.close()
