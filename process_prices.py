import pandas as pd
import os
from glob import glob

# 📁 Folder where all OMIE files (.1 for Spain, .2 for Portugal) are stored
folder_path = "data"  # update this if needed

# 🗂️ Find all relevant files
files = glob(os.path.join(folder_path, "*.1")) + glob(os.path.join(folder_path, "*.2"))

# 🧹 Function to clean one file and add 'Country'
def clean_omie_file(filepath):
    try:
        df = pd.read_csv(filepath, sep=";", skiprows=1, header=None)
        
        # Remove rows where any column contains '*'
        df = df[~df.apply(lambda x: x.astype(str).str.contains('\*').any(), axis=1)]
        
        df = df.drop(columns=[6])  # drop last column with '*'
        df.columns = ["Year", "Month", "Day", "Hour", "Price1", "Price2"]
        df["Datetime"] = pd.to_datetime(df[["Year", "Month", "Day"]]) + pd.to_timedelta(df["Hour"] - 1, unit="h")
        df["Filename"] = os.path.basename(filepath)
        df["Country"] = "Spain" if filepath.endswith(".1") else "Portugal"
        return df
    except Exception as e:
        print(f"❌ Error in file {filepath}: {e}")
        return None

# 🚀 Process all files
all_dfs = []
for f in files:
    cleaned = clean_omie_file(f)
    if cleaned is not None:
        all_dfs.append(cleaned)

# 🧬 Merge all together
if all_dfs:
    full_df = pd.concat(all_dfs, ignore_index=True)
    full_df = full_df.sort_values(["Datetime", "Country"]).reset_index(drop=True)

    print("✅ All data processed successfully!")
    print(full_df.head())

    # 💾 Save it
    full_df.to_csv("all_omie_prices.csv", index=False)
    full_df.to_parquet("all_omie_prices.parquet", index=False)

else:
    print("⚠️ No valid OMIE files found. Check your folder 'data/' and file extensions.")
