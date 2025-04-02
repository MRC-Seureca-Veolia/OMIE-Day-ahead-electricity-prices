import os
import pandas as pd
import duckdb

# 📁 Folders
data_folder = "data"
processed_folder = "processed"
os.makedirs(processed_folder, exist_ok=True)

# 📦 Master DataFrame
all_data = []

# 🔁 Loop through each file in /data/
for filename in sorted(os.listdir(data_folder)):
    if filename.endswith(".1") or filename.endswith(".2"):
        file_path = os.path.join(data_folder, filename)
        try:
            df = pd.read_csv(file_path, sep=";", encoding="latin1")
            df["source_file"] = filename  # traceability
            all_data.append(df)
        except Exception as e:
            print(f"⚠️ Failed to read {filename}: {e}")

# 📊 Combine all data
if all_data:
    combined_df = pd.concat(all_data, ignore_index=True)

    # 💾 Save to Parquet
    parquet_path = os.path.join(processed_folder, "prices.parquet")
    combined_df.to_parquet(parquet_path, index=False)
    print(f"✅ Parquet saved: {parquet_path}")

    # 🦆 Save to DuckDB
    duckdb_path = os.path.join(processed_folder, "prices.duckdb")
    con = duckdb.connect(duckdb_path)
    con.execute("DROP TABLE IF EXISTS prices")
    con.execute("CREATE TABLE prices AS SELECT * FROM combined_df")
    con.close()
    print(f"✅ DuckDB saved: {duckdb_path}")
else:
    print("🚫 No data found to process.")
