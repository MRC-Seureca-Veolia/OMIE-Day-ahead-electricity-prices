import os
import sys
import pandas as pd
import duckdb

# 🔧 Define paths
new_file = "data/MARGINALPDBC.1"
output_folder = "data"  # or wherever your parquet/duckdb are stored

# 🛑 Check if the new file exists
if not os.path.exists(new_file):
    print(f"⚠️ No new file found at {new_file}. Skipping append.")
    sys.exit(0)

# 🧼 Cleaning function
def clean_file(filepath):
    df = pd.read_csv(filepath, sep=";", skiprows=1, header=None)
    df = df[~df.apply(lambda x: x.astype(str).str.contains(r'\*').any(), axis=1)]
    df = df.drop(columns=[6])
    df.columns = ["Year", "Month", "Day", "Hour", "Price1", "Price2"]
    df["Datetime"] = pd.to_datetime(df[["Year", "Month", "Day"]]) + pd.to_timedelta(df["Hour"] - 1, unit="h")
    df["Country"] = "Spain" if filepath.endswith(".1") else "Portugal"
    return df

# ✅ Clean new file
new_df = clean_file(new_file)

# 📁 Load existing Parquet data
parquet_path = os.path.join(output_folder, "all_omie_prices.parquet")
if os.path.exists(parquet_path):
    existing = pd.read_parquet(parquet_path)
    combined = pd.concat([existing, new_df]).drop_duplicates(subset=["Datetime", "Country"]).sort_values(["Datetime", "Country"])
else:
    combined = new_df  # If no existing file

# 💾 Save to Parquet
combined.to_parquet(parquet_path, index=False)

# 🐤 Save to DuckDB
duckdb_path = os.path.join(output_folder, "omie_prices.duckdb")
con = duckdb.connect(duckdb_path)

# Create table if not exists
con.execute("""
CREATE TABLE IF NOT EXISTS prices AS SELECT * FROM combined LIMIT 0
""")
# Append the new data
con.execute("INSERT INTO prices SELECT * FROM combined")
con.close()

print("✅ Successfully appended new data to Parquet and DuckDB.")
