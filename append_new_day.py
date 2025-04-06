import os
import pandas as pd
import duckdb
from glob import glob

# 📁 Paths
DATA_DIR = "data"
OUTPUT_DIR = "processed"
os.makedirs(OUTPUT_DIR, exist_ok=True)

parquet_path = os.path.join(OUTPUT_DIR, "all_omie_prices.parquet")
csv_path = os.path.join(OUTPUT_DIR, "all_omie_prices.csv")
duckdb_path = os.path.join(OUTPUT_DIR, "omie_prices.duckdb")

# 🧼 Cleaning function
def clean_file(filepath):
    df = pd.read_csv(filepath, sep=";", skiprows=1, header=None)
    df = df[~df.apply(lambda x: x.astype(str).str.contains(r'\*').any(), axis=1)]
    df = df.drop(columns=[6])
    df.columns = ["Year", "Month", "Day", "Hour", "Price1", "Price2"]
    df["Datetime"] = pd.to_datetime(df[["Year", "Month", "Day"]]) + pd.to_timedelta(df["Hour"] - 1, unit="h")
    df["Country"] = "Spain" if filepath.endswith(".1") else "Portugal"
    return df

# 🧠 Load existing DB (if any)
if os.path.exists(parquet_path):
    existing = pd.read_parquet(parquet_path)
    existing_dates = set(existing["Datetime"].dt.date)
else:
    existing = pd.DataFrame()
    existing_dates = set()

# 🔍 Look for new files
files = sorted(glob(os.path.join(DATA_DIR, "marginalpdbc_*.1")) + glob(os.path.join(DATA_DIR, "marginalpdbc_*.2")))
new_data = []

for file in files:
    try:
        df = clean_file(file)
        file_dates = set(df["Datetime"].dt.date)
        if file_dates.isdisjoint(existing_dates):
            print(f"✅ Adding: {os.path.basename(file)}")
            new_data.append(df)
        else:
            print(f"⏭️ Skipping (already in DB): {os.path.basename(file)}")
    except Exception as e:
        print(f"⚠️ Error processing {file}: {e}")

# 🧱 Merge and save
if new_data:
    all_new = pd.concat(new_data)
    combined = pd.concat([existing, all_new])
    combined = combined.drop_duplicates(subset=["Datetime", "Country"]).sort_values(["Datetime", "Country"])
    
    # 💾 Save everything
    combined.to_csv(csv_path, index=False)
    combined.to_parquet(parquet_path, index=False)
    
    con = duckdb.connect(duckdb_path)
    con.execute("DROP TABLE IF EXISTS prices")
    con.register("df", combined)
    con.execute("CREATE TABLE prices AS SELECT * FROM df")
    con.close()

    print("🎉 Database updated with new data!")
else:
    print("🟰 No new data to add. All up to date!")



