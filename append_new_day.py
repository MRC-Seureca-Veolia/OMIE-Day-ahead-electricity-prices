import os
import sys
import pandas as pd
import duckdb
from glob import glob

data_folder = "data"
output_folder = "processed"
os.makedirs(output_folder, exist_ok=True)

print("🚀 Running append_new_day.py")

# 🔍 Find newest .1 or .2 file
files = sorted(
    glob(os.path.join(data_folder, "marginalpdbc_*.1")) +
    glob(os.path.join(data_folder, "marginalpdbc_*.2")),
    reverse=True
)

if not files:
    print("❌ No new OMIE files found in data/")
    sys.exit(0)

new_file = files[0]
print(f"📄 Found file: {new_file}")

# 🧼 Cleaning function
def clean_file(filepath):
    df = pd.read_csv(filepath, sep=";", skiprows=1, header=None)
    df = df[~df.apply(lambda x: x.astype(str).str.contains(r"\*").any(), axis=1)]
    df = df.drop(columns=[6])
    df.columns = ["Year", "Month", "Day", "Hour", "Price1", "Price2"]
    df["Datetime"] = pd.to_datetime(df[["Year", "Month", "Day"]]) + pd.to_timedelta(df["Hour"] - 1, unit="h")
    df["Country"] = "Spain" if filepath.endswith(".1") else "Portugal"
    return df

# 🆕 Cleaned new data
new_df = clean_file(new_file)
print(f"🆕 New rows: {len(new_df)} | Date range: {new_df['Datetime'].min()} → {new_df['Datetime'].max()}")

# 📦 Parquet merging
parquet_path = os.path.join(output_folder, "all_omie_prices.parquet")
if os.path.exists(parquet_path):
    existing_df = pd.read_parquet(parquet_path)
    print(f"📦 Loaded existing DB: {len(existing_df)} rows")
    combined = pd.concat([existing_df, new_df], ignore_index=True)
    combined = combined.drop_duplicates(subset=["Datetime", "Country"]).sort_values(["Datetime", "Country"])
else:
    combined = new_df.copy()

print(f"📈 Total after merge: {len(combined)} rows")

# 💾 Save all formats
combined.to_csv(os.path.join(output_folder, "all_omie_prices.csv"), index=False)
combined.to_parquet(parquet_path, index=False)

# 🦆 Update DuckDB (replace entire table for accuracy)
duckdb_path = os.path.join(output_folder, "omie_prices.duckdb")
con = duckdb.connect(duckdb_path)
con.execute("DROP TABLE IF EXISTS prices")
con.register("df", combined)
con.execute("CREATE TABLE prices AS SELECT * FROM df")
con.close()

# ✅ Check latest entry
print(f"✅ Latest date in database: {combined['Datetime'].max()}")

# 🔍 Sanity check for continuity
expected = pd.date_range(combined["Datetime"].min(), combined["Datetime"].max(), freq="H")
missing = expected.difference(combined["Datetime"].sort_values().drop_duplicates())

if missing.empty:
    print("✅ No gaps. Timestamps are continuous.")
else:
    print("⚠️ Missing timestamps:")
    for ts in missing:
        print(f"   ⏳ {ts}")


