import pandas as pd
import duckdb
import os

new_file = "data/MARGINALPDBC.1"  # replace with actual new file name
output_folder = "output"

def clean_file(filepath):
    df = pd.read_csv(filepath, sep=";", skiprows=1, header=None)
    df = df[~df.apply(lambda x: x.astype(str).str.contains('\*').any(), axis=1)]
    df = df.drop(columns=[6])
    df.columns = ["Year", "Month", "Day", "Hour", "Price1", "Price2"]
    df["Datetime"] = pd.to_datetime(df[["Year", "Month", "Day"]]) + pd.to_timedelta(df["Hour"] - 1, unit="h")
    df["Country"] = "Spain" if filepath.endswith(".1") else "Portugal"
    return df

# Clean new file
new_df = clean_file(new_file)

# Append to DuckDB
con = duckdb.connect(f"{output_folder}/omie_prices.duckdb")
con.execute("INSERT INTO prices SELECT * FROM new_df")
con.close()

# Append to Parquet
existing = pd.read_parquet(f"{output_folder}/all_omie_prices.parquet")
combined = pd.concat([existing, new_df]).sort_values(["Datetime", "Country"])
combined.to_parquet(f"{output_folder}/all_omie_prices.parquet", index=False)
