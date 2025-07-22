from utils.database import get_database_manager

db = get_database_manager()

# Check row count in SalesData table
row_count = db.get_table_row_count("SalesData")
print(f"SalesData row count: {row_count}")

# Optionally, preview a few rows
df = db.query_data("SELECT TOP 5 * FROM SalesData")
print(df)