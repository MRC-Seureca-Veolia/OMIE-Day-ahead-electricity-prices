import os
import pandas as pd
import duckdb

# 📁 Folders
data_folder = "data"
processed_folder = "processed"
os.makedirs(processed_folder, exist_ok=True)

# 📦 Master DataFrame
all_data = []

# 🔁 Loop through each file
for filename in sorted(os.listdir(data_folder)):
    if filename.endswith(".1") or filename.endswith(".2"):
        file_path = os.path.join(data_folder, filename)
        try:
            df_raw = pd.read_csv(file_path, sep=";", encoding="latin1", skiprows=1)
            
            # 🧼 Clean + rename
            df_raw.columns = [col.strip().lower().replace(" ", "_") for col in df_raw.columns]
            if "fecha" in df_raw.columns and "hora" in df_raw.columns and "precio_(€/mwh)" in df_raw.columns:
                df = df_raw[["fecha", "hora", "precio_(€/mwh)"]].copy()
                df.rename(columns={
                    "fecha": "date",
                    "hora": "hour",
                    "precio_(€/mwh)": "price_eur_mwh"
                }, inplace=True)
                
                # ⏰ Standardize time format
                df["hour"] = df["hour"].astype(str).str.zfill(2)
                df["source_file"] = filename
                df["zone"] = "ES"  # Spain
                
                all_data.append(df)
            else:
                print(f"⚠️ Unexpected format in {filename}")
        except Exception as e:
            print(f"❌ Failed to process {filename}: {e}")

# 📊 Combine & export
if all_data:
    combined = pd.concat(all_data, ignore_index=True)

    # 💾 Save to Parquet
    parquet_path = os.path.join(processed_folder, "prices_tidy.parquet")
    combined.to_parquet(parquet_path, index=False)
    print(f"✅ Tidy Parquet saved: {parquet_path}")

    # 🦆 Save to DuckDB
    duckdb_path = os.path.join(processed_folder, "prices_tidy.duckdb")
    con = duckdb.connect(duckdb_path)
    con.execute("DROP TABLE IF EXISTS prices_tidy")
    con.execute("CREATE TABLE prices_tidy AS SELECT * FROM combined")
    con.close()
    print(f"✅ Tidy DuckDB saved: {duckdb_path}")
else:
    print("🚫 No valid data to process.")

