import pandas as pd
import os
from glob import glob

# 💾 Set the folder where all your .1 price files are stored
folder_path = "path_to_your_files"  # ← replace this with your folder path

# 🔍 Find all .1 files (daily price files from OMIE)
files = glob(os.path.join(folder_path, "*.1"))

# 🧹 Function to clean one file
def clean_omie_file(filepath):
    try:
        df = pd.read_csv(filepath, sep=";", skiprows=1, header=None)
        df = df.drop(columns=[6])  # remove last column
        df.columns = ["Year", "Month", "Day", "Hour", "Price1", "Price2"]
        df["Datetime"] = pd.to_datetime(df[["Year", "Month", "Day"]]) + pd.to_timedelta(df["Hour"] - 1, unit="h")
        df["Filename"] = os.path.basename(filepath)
        return df
    except Exception as e:
        print(f"❌ Error in file {filepath}: {e}")
        return None

# 📦 Load and clean all files
all_dfs = []
for f in files:
    cleaned_df = clean_omie_file(f)
    if cleaned_df is not None:
        all_dfs.append(cleaned_df)

# 🧬 Combine into one big DataFrame
if all_dfs:
    full_df = pd.concat(all_dfs, ignore_index=True)
    full_df = full_df.sort_values("Datetime").reset_index(drop=True)
    print("✅ Data loaded successfully!")
    print(full_df.head())

    # 💾 Optional: save to CSV or Parquet
    full_df.to_csv("all_omie_prices.csv", index=False)
    full_df.to_parquet("all_omie_prices.parquet", index=False)
else:
    print("⚠️ No vali
