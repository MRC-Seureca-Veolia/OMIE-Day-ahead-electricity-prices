import os
import sys
import pandas as pd
import duckdb
from glob import glob

# 📂 Folders
data_folder = "data"
output_folder = "processed"
os.makedirs(output_folder, exist_ok=True)

print("🚀 Starting OMIE append script...")

# 🔍 Find latest file (newest marginalpdbc_*.1 or *.2)
files = sorted(
    glob(os.path.join(data_folder, "marginalpdbc_*.1")) +
    glob(os.path.join(data_folder, "marginalpdbc_*.2")),
    reverse=True
)

if not files:
    print("⚠️ No OMIE files found in 'data/'. Exiting.")
    sys.exit(0)

# ✅ Pick newest
new_file = files[0]
print(f"📄 Using latest file: {new_file}")

# 🧼 Clean file function
def clean_file(filepath):
    df = pd.read_csv(filepath, sep=";", skiprows=1, header=None)
    df = df[~df.apply(lambda x: x.astype(str).str.contains(r"\*").any(), axis=1)]
    df = df.drop(columns=[6])
    df.columns = ["Year", "Month", "Day", "Hour", "Price1", "Price2"]
    df["Datetime"] = pd.to_datetime(df[["Year", "Month", "Day"]]) + pd.to_timedelta(df["Hour"] - 1, unit="h")
    df["Country"] = "Spain" if filepath.endswith(".1") else "Portugal"
    return df

# 📊 Clean new data
new_df = clean_file(new_file)

# 🧠 Merge with Parquet
parquet_path = os.path.join(output_folder, "all_omie_prices.parquet")
if os.path.exists(parquet_path):
    existing = pd.read_parquet(parquet_path)
    combined = pd.concat([existing, new_df])
    combined = combined.drop_duplicates(subset=["Datetime", "Country"]).sort_values(["Datetime", "Country"])
else:
    combined = new_df

# 💾 Save all formats
combined.to_parquet(parquet_path, index=False)
combined.to_csv(os.path.join(output_folder, "all_omie_prices.csv"), index=False)

# 🦆 Save to DuckDB
duckdb_path = os.path.join(output_folder, "omie_prices.duckdb")
con = duckdb.connect(duckdb_path)
con.register("df", combined)

# Rebuild DuckDB table
con.execute("DROP TABLE IF EXISTS prices")
con.execute("CREATE TABLE prices AS SELECT * FROM df")
con.close()

print("✅ Parquet, CSV, and DuckDB updated!")
print(f"🕒 Latest date: {combined['Datetime'].max()}")



