import os
import sys
import pandas as pd
import duckdb
from glob import glob

# 📁 Paths
data_folder = "data"
output_folder = "processed"
os.makedirs(output_folder, exist_ok=True)

print("🚀 Starting OMIE append script...")

# 🔍 Find the newest .1 or .2 file (most recent first)
files = sorted(
    glob(os.path.join(data_folder, "marginalpdbc_*.1")) +
    glob(os.path.join(data_folder, "marginalpdbc_*.2")),
    reverse=True
)

if not files:
    print("⚠️ No OMIE files found in 'data/'. Exiting.")
    sys.exit(0)

# ✅ Pick the newest one
new_file = files[0]
print(f"📄 Selected OMIE file: {new_file}")

# 🧼 Clean it
def clean_file(filepath):
    df = pd.read_csv(filepath, sep=";", skiprows=1, header=None)
    df = df[~df.apply(lambda x: x.astype(str).str.contains(r"\*").any(), axis=1)]
    df = df.drop(columns=[6])
    df.columns = ["Year", "Month", "Day", "Hour", "Price1", "Price2"]
    df["Datetime"] = pd.to_datetime(df[["Year", "Month", "Day"]]) + pd.to_timedelta(df["Hour"] - 1, unit="h")
    df["Country"] = "Spain" if filepath.endswith(".1") else "Portugal"
    return df

new_df = clean_file(new_file)

# 🧠 Merge with Parquet
parquet_path = os.path.join(output_folder, "all_omie_prices.parquet")
if os.path.exists(parquet_path):
    existing = pd.read_parquet(parquet_path)
    combined = pd.concat([existing, new_df])
    combined = combined.drop_duplicates(subset=["Datetime", "Country"]).sort_values(["Datetime", "Country"])
else:
    combined = new_df

# 💾 Save CSV and Parquet
combined.to_parquet(parquet_path, index=False)
combined.to_csv(os.path.join(output_folder, "all_omie_prices.csv"), index=False)

# 🦆 DuckDB
duckdb_path = os.path.join(output_folder, "omie_prices.duckdb")
con = duckdb.connect(duckdb_path)
con.register("new_data", new_df)

# Ensure table exists
con.execute("""
    CREATE TABLE IF NOT EXISTS prices AS 
    SELECT * FROM new_data LIMIT 0
""")

# Insert only new rows
con.execute("""
    INSERT INTO prices
    SELECT * FROM new_data
    EXCEPT
    SELECT * FROM prices
""")

con.close()
print("✅ Done: OMIE data appended to all formats.")


