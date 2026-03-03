"""
Pull mart_label_mismatches from Databricks and save as Excel.

Usage:
    python scripts/pull_mismatches.py

Requires:
    pip install databricks-sql-connector openpyxl pandas
"""

import os
import subprocess
import pandas as pd
from databricks import sql


# ── Databricks connection details ──────────────────────────────────
HOST = "dbc-7b1446b4-fd6c.cloud.databricks.com"
HTTP_PATH = "/sql/1.0/warehouses/827f355ef744ab16"
CATALOG = "workspace"
SCHEMA = "dbt_amarquardt"
TABLE = "mart_label_mismatches"

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "output")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "mart_label_mismatches.xlsx")


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("Connecting to Databricks (OAuth — browser will open)…")
    with sql.connect(
        server_hostname=HOST,
        http_path=HTTP_PATH,
        auth_type="databricks-oauth",
        catalog=CATALOG,
        schema=SCHEMA,
    ) as conn:
        query = f"SELECT * FROM {CATALOG}.{SCHEMA}.{TABLE}"
        print(f"Running: {query}")
        df = pd.read_sql(query, conn)

    print(f"Got {len(df)} rows.")

    df.to_excel(OUTPUT_FILE, index=False, sheet_name="Mismatches")
    print(f"Saved → {os.path.abspath(OUTPUT_FILE)}")

    # Auto-open the file on macOS
    subprocess.run(["open", os.path.abspath(OUTPUT_FILE)])


if __name__ == "__main__":
    main()
