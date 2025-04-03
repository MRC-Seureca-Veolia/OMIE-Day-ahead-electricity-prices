import pandas as pd
import os
from glob import glob

# 📁 Set your folder path
folder_path = "path_to_your_files"  # ⬅️ Change this!

# 📂 Find all .1 and .2 files
files = glob(os.path.join(folder_path, "*.1")) + glob(os.path.join(folder_path, "*.2"))

# 🧹 Cleaning function
def clean_omie_file(filepath):
    try:
        df = pd.read_csv(filepath, sep=";", skiprows=1, header=None)
        df = df.drop(columns=[6])  # drop last column
        df.columns = ["Year", "Month", "Day", "Hour", "Price1", "Price2"]
        df["Datetime"] = pd.to_datetime(df[["Year", "Month", "Day"]]) + pd.to_timedelta(df["Hour"] - 1, unit="h")
        df["Filename"] = os.path.basename(filepath)
        df["Country"] = "Spain" if filepath.endswith(".1") else "Portugal"
        df["Price"] = df["Price1"]  # we use Price1 for consistency
        return df[["Datetime", "Price", "Country", "Filename"]]
    except Exception as e:
        print(f"❌ Error in {filepath}: {e}")
        return None

# 📦 Load all
all_dfs = []
for f in files:
    cleaned = clean_omie_file(f)
    if cleaned is not None:
        all_dfs.append(cleaned)

# 🧬 Final dataset
if all_dfs:
    df_all = pd.concat(all_dfs).sort_values("Datetime").reset_index(drop=True)
    df_spain = df_all[df_all["Country"] == "Spain"]  # 🇪🇸 filter Spain

    print("✅ Spain data loaded!")
    print(df_spain.head())

    # 💾 Save
    df_spain.to_csv("spain_prices.csv", index=False)
    df_spain.to_parquet("spain_prices.parquet", index=False)
else:
    print("⚠️ No valid data found.")
