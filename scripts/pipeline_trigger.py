#!/usr/bin/env python3
"""
Pipeline trigger: detects new/updated tables in rawdatalabeling
and runs the full dbt DAG automatically.

Usage:
  python3 scripts/pipeline_trigger.py              # one-shot: check + run
  python3 scripts/pipeline_trigger.py --watch      # poll every 60s
  python3 scripts/pipeline_trigger.py --watch 30   # poll every 30s
"""

import subprocess, sys, time, json, os
from pathlib import Path
from datetime import datetime

PROJECT_DIR = Path(__file__).resolve().parent.parent
STATE_FILE = PROJECT_DIR / ".pipeline_state.json"

DATABRICKS_HOST = "dbc-7b1446b4-fd6c.cloud.databricks.com"
DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/827f355ef744ab16"
CATALOG = "workspace"
SCHEMA = "rawdatalabeling"


def get_connection():
    from databricks import sql as dbsql
    return dbsql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        auth_type="databricks-oauth",
    )


def get_table_fingerprints():
    """Get table names + row counts from rawdatalabeling to detect changes."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(f"SHOW TABLES IN {CATALOG}.{SCHEMA}")
    tables = [row[1] for row in cursor.fetchall()]

    fingerprints = {}
    for t in tables:
        try:
            cursor.execute(
                f"SELECT count(*) FROM {CATALOG}.{SCHEMA}.`{t}`"
            )
            row_count = cursor.fetchone()[0]
            fingerprints[t] = row_count
        except Exception as e:
            print(f"  Warning: could not read {t}: {e}")

    cursor.close()
    conn.close()
    return fingerprints


def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"fingerprints": {}, "last_run": None}


def save_state(state):
    STATE_FILE.write_text(json.dumps(state, indent=2))


def find_changes(old_fp, new_fp):
    """Return list of tables that are new or changed."""
    changes = []
    for table, count in new_fp.items():
        if table not in old_fp:
            changes.append(("NEW", table, count))
        elif old_fp[table] != count:
            changes.append(("UPDATED", table, count))
    return changes


def has_dbt_models(table_name):
    """Check if staging model exists for this table."""
    models_dir = PROJECT_DIR / "models"
    # Look for any .sql file referencing this table name
    for sql_file in models_dir.rglob("*.sql"):
        if table_name in sql_file.read_text():
            return True
    return False


def run_dbt_build():
    print("\n═══ Running dbt build ═══")
    result = subprocess.run(["dbt", "build"], cwd=PROJECT_DIR)
    return result.returncode == 0


def run_sync():
    print("\n═══ Syncing mart → Azure SQL + GitHub ═══")
    result = subprocess.run(
        [sys.executable, str(PROJECT_DIR / "scripts" / "sync_mart.py")],
        cwd=PROJECT_DIR,
    )
    return result.returncode == 0


def check_and_run():
    """Check for changes and run pipeline if needed. Returns True if pipeline ran."""
    state = load_state()
    old_fp = state.get("fingerprints", {})

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Checking rawdatalabeling for changes...")
    new_fp = get_table_fingerprints()
    changes = find_changes(old_fp, new_fp)

    if not changes:
        print("  No changes detected. Pipeline up to date.")
        return False

    for kind, table, count in changes:
        print(f"  {kind}: {table} ({count} rows)")

    success = run_dbt_build()
    if success:
        print("\n✓ dbt build succeeded")
        state["fingerprints"] = new_fp
        state["last_run"] = datetime.now().isoformat()
        save_state(state)

        try:
            run_sync()
            print("✓ Sync complete")
        except Exception as e:
            print(f"⚠ Sync failed (non-fatal): {e}")
    else:
        print("\n✗ dbt build FAILED — state not updated, will retry next run")

    return success


def main():
    watch = "--watch" in sys.argv
    interval = 60

    # Parse interval from --watch N
    if watch:
        idx = sys.argv.index("--watch")
        if idx + 1 < len(sys.argv):
            try:
                interval = int(sys.argv[idx + 1])
            except ValueError:
                pass

    if watch:
        print(f"Watching rawdatalabeling (polling every {interval}s). Ctrl+C to stop.\n")
        try:
            while True:
                check_and_run()
                print(f"\n  Next check in {interval}s...\n")
                time.sleep(interval)
        except KeyboardInterrupt:
            print("\nStopped.")
    else:
        check_and_run()


if __name__ == "__main__":
    main()
