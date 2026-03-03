"""
Pull mart_label_mismatches from Databricks, push to Azure SQL,
and commit a CSV to the repo so Claude can always access it via GitHub.

Usage:
    python3 scripts/sync_mart.py

Requires:
    pip3 install databricks-sql-connector pymssql pandas
"""

import os
import subprocess
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

# ── Paths ──────────────────────────────────────────────────────────
REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")
CSV_PATH = os.path.join(REPO_ROOT, "data", "mart_label_mismatches.csv")


def pull_from_databricks():
    print("1/3  Pulling from Databricks (OAuth — browser may open)…")
    with dbsql.connect(
        server_hostname=DB_HOST,
        http_path=DB_HTTP_PATH,
        auth_type="databricks-oauth",
        catalog=DB_CATALOG,
        schema=DB_SCHEMA,
    ) as conn:
        query = f"SELECT * FROM {DB_CATALOG}.{DB_SCHEMA}.{DB_TABLE}"
        print(f"     Running: {query}")
        df = pd.read_sql(query, conn)
    print(f"     Got {len(df)} rows.")
    return df


def push_to_azure(df):
    print(f"2/3  Pushing to Azure SQL ({AZ_HOST})…")
    az = pymssql.connect(
        server=AZ_HOST, user=AZ_USER, password=AZ_PASS,
        database=AZ_DB, tds_version="7.3",
    )
    cursor = az.cursor()

    cursor.execute(f"IF OBJECT_ID('{AZ_TABLE}', 'U') IS NOT NULL DROP TABLE {AZ_TABLE}")

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

    cursor.execute(f"CREATE TABLE {AZ_TABLE} ({', '.join(col_defs)})")

    placeholders = ", ".join(["%s"] * len(df.columns))
    insert_sql = f"INSERT INTO {AZ_TABLE} VALUES ({placeholders})"
    for _, row in df.iterrows():
        values = tuple(None if pd.isna(v) else v for v in row)
        cursor.execute(insert_sql, values)

    az.commit()
    cursor.close()
    az.close()
    print(f"     Pushed {len(df)} rows.")


def commit_csv(df):
    print("3/3  Saving CSV + pushing to GitHub…")
    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
    df.to_csv(CSV_PATH, index=False)
    print(f"     Saved → {CSV_PATH}")

    subprocess.run(["git", "add", CSV_PATH], cwd=REPO_ROOT)
    result = subprocess.run(
        ["git", "diff", "--cached", "--quiet"],
        cwd=REPO_ROOT,
    )
    if result.returncode != 0:
        subprocess.run(
            ["git", "commit", "-m", "chore: update mart_label_mismatches data"],
            cwd=REPO_ROOT,
        )
        subprocess.run(["git", "push"], cwd=REPO_ROOT)
        print("     Committed + pushed to GitHub.")
    else:
        print("     No changes — skipping commit.")


def main():
    df = pull_from_databricks()
    push_to_azure(df)
    commit_csv(df)
    print("\nDone! Mart synced to Azure SQL + GitHub.")


if __name__ == "__main__":
    main()
