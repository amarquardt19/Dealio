"""
Pull mart_label_mismatches from Databricks and push to Azure SQL.

Usage:
    python3 scripts/push_to_azure.py

Requires:
    pip3 install databricks-sql-connector pymssql pandas
"""

import pandas as pd
import pymssql
from databricks import sql as dbsql


# ── Databricks connection ──────────────────────────────────────────
DB_HOST = "dbc-7b1446b4-fd6c.cloud.databricks.com"
DB_HTTP_PATH = "/sql/1.0/warehouses/827f355ef744ab16"
DB_CATALOG = "workspace"
DB_SCHEMA = "dbt_amarquardt"
DB_TABLE = "mart_label_mismatches"

# ── Azure SQL connection ───────────────────────────────────────────
AZ_HOST = "brokerblasts1.database.windows.net"
AZ_USER = "CloudSAf21060b2"
AZ_PASS = "Shp808bhs!"
AZ_DB = "DealSifta"
AZ_TABLE = "dbo.mart_label_mismatches"


def main():
    # 1. Pull from Databricks
    print("Connecting to Databricks (OAuth — browser may open)…")
    with dbsql.connect(
        server_hostname=DB_HOST,
        http_path=DB_HTTP_PATH,
        auth_type="databricks-oauth",
        catalog=DB_CATALOG,
        schema=DB_SCHEMA,
    ) as conn:
        query = f"SELECT * FROM {DB_CATALOG}.{DB_SCHEMA}.{DB_TABLE}"
        print(f"Running: {query}")
        df = pd.read_sql(query, conn)
    print(f"Got {len(df)} rows from Databricks.")

    # 2. Push to Azure SQL
    print(f"Connecting to Azure SQL ({AZ_HOST})…")
    az = pymssql.connect(
        server=AZ_HOST,
        user=AZ_USER,
        password=AZ_PASS,
        database=AZ_DB,
        tds_version="7.3",
    )
    cursor = az.cursor()

    # Drop and recreate the table
    cursor.execute(f"IF OBJECT_ID('{AZ_TABLE}', 'U') IS NOT NULL DROP TABLE {AZ_TABLE}")

    # Build CREATE TABLE from dataframe dtypes
    col_defs = []
    for col in df.columns:
        dtype = df[col].dtype
        if "float" in str(dtype):
            sql_type = "FLOAT"
        elif "int" in str(dtype):
            sql_type = "INT"
        else:
            sql_type = "NVARCHAR(MAX)"
        col_defs.append(f"[{col}] {sql_type}")

    create_sql = f"CREATE TABLE {AZ_TABLE} ({', '.join(col_defs)})"
    cursor.execute(create_sql)
    print("Created table in Azure SQL.")

    # Insert rows
    placeholders = ", ".join(["%s"] * len(df.columns))
    insert_sql = f"INSERT INTO {AZ_TABLE} VALUES ({placeholders})"

    for _, row in df.iterrows():
        values = tuple(None if pd.isna(v) else v for v in row)
        cursor.execute(insert_sql, values)

    az.commit()
    cursor.close()
    az.close()

    print(f"Pushed {len(df)} rows to Azure SQL → {AZ_TABLE}")
    print("Done!")


if __name__ == "__main__":
    main()
