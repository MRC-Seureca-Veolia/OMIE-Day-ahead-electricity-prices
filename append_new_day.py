# 💾 Save updated Parquet
combined.to_parquet(parquet_path, index=False)
print(f"✅ Parquet updated: {parquet_path}")
print(f"🕒 Latest timestamp: {combined['Datetime'].max()}")

# 💾 Also save CSV
csv_path = os.path.join(output_folder, "all_omie_prices.csv")
combined.to_csv(csv_path, index=False)
print(f"✅ CSV updated: {csv_path}")

# 🦆 DuckDB update
duckdb_path = os.path.join(output_folder, "omie_prices.duckdb")
con = duckdb.connect(duckdb_path)

# Register new data as a DuckDB view
con.register("new_data", new_df)

# Create table if not exists
con.execute("""
    CREATE TABLE IF NOT EXISTS prices AS 
    SELECT * FROM new_data LIMIT 0
""")

# Insert only new rows
con.execute("""
    INSERT INTO prices
    SELECT * FROM new_data
    EXCEPT
    SELECT * FROM prices
""")

con.close()
print(f"✅ DuckDB updated: {duckdb_path}")
print("🎉 New data appended to Parquet, DuckDB, and CSV.")



# ✅ Summary
print(f"🎉 OMIE data appended and exported to Parquet, CSV, and DuckDB!")
print(f"🕒 Latest timestamp: {combined['Datetime'].max()}")


