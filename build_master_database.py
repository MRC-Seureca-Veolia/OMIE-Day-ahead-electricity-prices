import os
import pandas as pd
import duckdb
from glob import glob

# Paths
input_folder = "data"
output_folder = "process"
os.makedirs(output_folder, exist_ok=True)

# Function to clean a file
def clean_file(filepath):
    df = pd.read_csv(filepath, sep=";", skiprows=1, header=None)
    df = df[~df.apply(lambda x: x.astype(str).str.contains('\*').any(), axis=1)]
    df = df.drop(columns=[6])  # Drop duplicate price column
    df.columns = ["Year", "Month", "Day", "Hour", "Price1", "Price2"]
    df["Datetime"] = pd.to_datetime(df[["Year", "Month", "Day"]]) + pd.to_timedelta(df["Hour"] - 1, unit="h")
    df["Country"] = "Spain" if filepath.endswith(".1") else "Portugal"
    return df

# Collect and clean all files
files = glob(os.path.join(input_folder, "*.1")) + glob(os.path.join(input_folder, "*.2"))
all_data = []

for file in files:
    try:
        cleaned = clean_file(file)
        all_data.append(cleaned)
    except Exception as e:
        print(f"⚠️ Error in {file}: {e}")

# Combine all cleaned data
if all_data:
    full_df = pd.concat(all_data, ignore_index=True)
    full_df = full_df.sort_values(["Datetime", "Country"]).reset_index(drop=True)

    # Save to CSV
    full_df.to_csv(os.path.join(output_folder, "all_omie_prices.csv"), index=False)

    # Save to Parquet
    full_df.to_parquet(os.path.join(output_folder, "all_omie_prices.parquet"), index=False)

    # Save to DuckDB
    con = duckdb.connect(os.path.join(output_folder, "omie_prices.duckdb"))
    con.execute("DROP TABLE IF EXISTS prices")
    con.execute("CREATE TABLE prices AS SELECT * FROM full_df")
    con.close()

    print("✅ Historical database built successfully!")

else:
    print("⚠️ No valid OMIE files found.")
