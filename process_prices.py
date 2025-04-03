import os
import pandas as pd
import duckdb
from datetime import datetime

data_folder = "data"
processed_folder = "processed"
os.makedirs(processed_folder, exist_ok=True)

all_data = []

for filename in sorted(os.listdir(data_folder)):
    if filename.endswith(".1") or filename.endswith(".2"):
        file_path = os.path.join(data_folder, filename)

        try:
            # Skip header row and read with semicolon separator
            df = pd.read_csv(file_path, sep=";", header=None, skiprows=1, encoding="latin1")

            if df.shape[1] < 6:
                print(f"⚠️ Skipping {filename} - unexpected format")
                continue

            df.columns = ["year", "month", "day", "hour", "price", "price_dup"]
            df = df[["year", "month", "day", "hour", "price"]]  # Drop duplicate price

            # Create a proper date column
            df["date"] = pd.to_datetime(df[["year", "month", "day"]])

            # Reorder columns
            df = df[["date", "hour", "price"]]
            df["source_file"] = filename

            all_data.append(df)

        except Exception as e:
            print(f"⚠️ Failed to parse {filename}: {e}")

# Combine and export
if all_data:
    combined_df = pd.concat(all_data, ignore_index=True)

    # Save Parquet
    combined_df.to_parquet(os.path.join(processed_folder, "prices.parquet"), index=False)

    # Save DuckDB
    con = duckdb.connect(os.path.join(processed_folder, "prices.duckdb"))
    con.execute("DROP TABLE IF EXISTS prices")
    con.execute("CREATE TABLE prices AS SELECT * FROM combined_df")
    con.close()

    print("✅ Done! Clean and tidy prices saved.")
else:
    print("🚫 No data processed.")
