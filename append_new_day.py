import os
import sys
import pandas as pd
import duckdb

# 📂 Paths
new_file = "data/MARGINALPDBC.1"
output_folder = "processed"
os.makedirs(output_folder, exist_ok=True)

print("🚀 Starting OMIE append script...")

# 🚫 Skip if no new file
if not os.path.exists(new_file):
    print(f"⚠️ No new file found at {new_file}. Skipping append.")
    sys.exit(0)

# 🧼 Clean function
def clean_file(filepath):
    print(f"📂 Cleaning new OMIE file: {filepath}")
    df = pd.read_csv(filepath, sep=";", skiprows=1, header=None)
    df = df[~df.apply(lambda x: x.astype(str).str.contains(r'\*').any(), axis=1)]
    df = df.drop(columns=[6])
    df.columns = ["Year", "Month", "Day", "Hour", "Price1", "Price2"]
    df["Datetime"] = pd.to_datetime(df[["Year", "Month", "Day"]]) + pd.to_timedelta(df["Hour"] - 1, unit="h")
    df["Country"] = "Spain" if filepath.endswith(".1") else "Portugal"
    return df

# ✅ Clean and prepare
new_df = clean_file(new_file)

# 🧠 Combine with existing Parquet data
parquet_path = os.path.join(output_folder, "all_omie_prices.parquet")
if os.path.exists(parquet_path):
    existing = pd.read_parquet(parquet_path)
    combined = pd.concat([existing, new_df])
    combined = combined.drop_duplicates(subset=["Datetime", "Country"]).sort_values(["Datetime", "Country"])
else:
    combined = new_df

# 💾 Save updated Parquet
combined.to_parquet(parquet_path, index=False)
print(f"✅ Parquet updated: {parquet_path}")

# 💾 Also save updated CSV
csv_path = os.path.join(output_folder, "all_omie_prices.csv")
combined.to_csv(csv_path, index=False)
print(f"✅ CSV updated: {csv_path}")

# 🦆 DuckDB update
duckdb_path = os.path.join(output_folder, "omie_prices.duckdb")
con = duckdb.connect(duckdb_path)

# Register updated dataframe
con.register("updated", combined)

# Rebuild entire DuckDB table for safety
con.execute("DROP TABLE IF EXISTS prices")
con.execute("CREATE TABLE prices AS SELECT * FROM updated")
con.close()
print(f"✅ DuckDB rebuilt: {duckdb_path}")

# ✅ Summary
print(f"🎉 OMIE data appended and exported to Parquet, CSV, and DuckDB!")
print(f"🕒 Latest timestamp: {combined['Datetime'].max()}")


