import os
import pandas as pd
import duckdb
from glob import glob

# Paths
input_folder = "data"
output_folder = "processed"
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
    csv_path = os.path.join(output_folder, "all_omie_prices.csv")
    full_df.to_csv(csv_path, index=False)
    print(f"✅ CSV saved to {csv_path}")

    # Save to Parquet
    parquet_path = os.path.join(output_folder, "all_omie_prices.parquet")
    full_df.to_parquet(parquet_path, index=False)
    print(f"✅ Parquet saved to {parquet_path}")

    # Save to DuckDB
    duckdb_path = os.path.join(output_folder, "omie_prices.duckdb")
    con = duckdb.connect(duckdb_path)
    con.register("df_view", full_df)  # Register dataframe so DuckDB can access it
    con.execute("DROP TABLE IF EXISTS prices")
    con.execute("CREATE TABLE prices AS SELECT * FROM df_view")
    con.close()
    print(f"✅ DuckDB saved to {duckdb_path}")

    print("✅ Historical database built successfully!")

else:
    print("⚠️ No valid OMIE files found.")
