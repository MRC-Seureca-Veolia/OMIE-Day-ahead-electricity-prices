import os
import sys
import pandas as pd
import duckdb
from glob import glob

# 📂 Paths
output_folder = "processed"
data_folder = "data"
os.makedirs(output_folder, exist_ok=True)

print("🚀 Starting OMIE append script...")

# 🔍 Find latest .1 or .2 file
files = sorted(glob(os.path.join(data_folder, "marginalpdbc_*.1")) + glob(os.path.join(data_folder, "marginalpdbc_*.2")), reverse=True)

if not files:
    print("⚠️ No OMIE data files found. Skipping.")
    sys.exit(0)

new_file = files[0]
print(f"📄 Using latest file: {new_file}")

# 🧼 Clean function
def clean_file(filepath):
    df = pd.read_csv(filepath, sep=";", skiprows=1, header=None)
    df = df[~df.apply(lambda x: x.astype(str).str.contains(r'\*').any(), axis=1)]
    df = df.drop(columns=[6])
    df.columns = ["Year", "Month", "Day", "Hour", "Price1", "Price2"]
    df["Datetime"] = pd.to_datetime(df[["Year", "Month", "Day"]]) + pd.to_timedelta(df["Hour"] - 1, unit="h")
    df["Country"] = "Spain" if filepath.endswith(".1") else "Portugal"
    return df

# ✅ Clean and prepare
new_df = clean_file(new_file)

# 📚 Merge with existing parquet
parquet_path = os.path.join(output_folder, "all_omie_prices.parquet")
if os.path.exists(parquet_path):
    existing = pd.read_parquet(parquet_path)
    combined = pd.concat([existing, new_df])
    combined = combined.drop_duplicates(subset=["Datetime", "Country"]).sort_values(["Datetime", "Country"])
else:
    combined = new_df

# 💾 Save to Parquet
combined.to_parquet(parquet_path, index=False)
print(f"✅ Parquet updated: {parquet_path}")

# 💾 Save to CSV
csv_path = os.path.join(output_folder, "all_omie_prices.csv")
combined.to_csv(csv_path, index=False)
print(f"✅ CSV updated: {csv_path}")

# 🦆 Save to DuckDB
duckdb_path = os.path.join(output_folder, "omie_prices.duckdb")
con = duckdb.connect(duckdb_path)

con.register("new_data", new_df)

con.execute("""
    CREATE TABLE IF NOT EXISTS prices AS 
    SELECT * FROM new_data LIMIT 0
""")

con.execute("""
    INSERT INTO prices
    SELECT * FROM new_data
    EXCEPT
    SELECT * FROM prices
""")

con.close()
print(f"✅ DuckDB updated: {duckdb_path}")
print("🎉 Done: CSV + Parquet + DuckDB updated!")




# ✅ Summary
print(f"🎉 OMIE data appended and exported to Parquet, CSV, and DuckDB!")
print(f"🕒 Latest timestamp: {combined['Datetime'].max()}")


