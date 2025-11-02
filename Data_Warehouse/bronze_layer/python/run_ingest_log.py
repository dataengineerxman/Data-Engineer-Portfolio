import pyodbc

conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=DataWarehouse;"
    "Trusted_Connection=yes;"
)

with pyodbc.connect(conn_str, autocommit=True) as conn:
    cursor = conn.cursor()
    cursor.execute("EXEC bronze.ingest_data;")
    print("✅ Ingestion executed successfully.")

# Optional: show latest run log
cursor.execute("""
SELECT TOP 1 run_ts, status, duration_seconds
FROM bronze.ingest_data_run_history
ORDER BY run_ts DESC;
""")
print(cursor.fetchone())

