import pandas as pd
import os
from glob import glob

# 💾 Folder with your OMIE .1 files (Spain)
folder_path = "path_to_your_files"  # 👈 Replace with your actual folder

# 🔍 Find all .1 files (Spain)
files = glob(os.path.join(folder_path, "*.1"))

# 🧹 Clean each file
def clean_omie_file(filepath):
    try:
        df = pd.read_csv(filepath, sep=";", skiprows=1, header=None)
        df = df.drop(columns=[6])  # Remove last column
        df.columns = ["Year", "Month", "Day", "Hour", "Price1", "Price2"]
        df["Datetime"] = pd.to_datetime(df[["Year", "Month", "Day"]]) + pd.to_timedelta(df["Hour"] - 1, unit="h")
        df["Filename"] = os.path.basename(filepath)
        df["Price"] = df["Price1"]  # 👈 Spain price
        return df[["Datetime", "Price", "Filename"]]
    except Exception as e:
        print(f"❌ Error in file {filepath}: {e}")
        return None

# 📦 Load all files
all_dfs = []
for f in files:
    df = clean_omie_file(f)
    if df is not None:
        all_dfs.append(df)

# 🧬 Combine and export
if all_dfs:
    full_df = pd.concat(all_dfs, ignore_index=True).sort_values("Datetime")
    print("✅ Spain data loaded!")
    print(full_df.head())

    # 💾 Save
    full_df.to_csv("spain_prices.csv", index=False)
    full_df.to_parquet("spain_prices.parquet", index=False)
else:
    print("⚠️ No valid Spain data files found.")
