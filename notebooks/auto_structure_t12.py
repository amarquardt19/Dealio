# Databricks notebook source
# MAGIC %md
# MAGIC # Auto-Structure T12 Pipeline
# MAGIC **Fully event-driven — triggered by file arrival in UC Volume.**
# MAGIC
# MAGIC **Workflow:** Drop a CSV into `/Volumes/workspace/rawdatalabeling/uploads/`
# MAGIC → file arrival trigger fires → this notebook ingests the file as a table
# MAGIC → structures ALL t12 tables → writes output to `mart_generic_labeled`.
# MAGIC
# MAGIC **File naming:** `t12|MMYY|propertyname.csv` (e.g., `t12|0225|newproperty.csv`)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config

# COMMAND ----------

import os

CATALOG = "workspace"
RAW_SCHEMA = "rawdatalabeling"
OUTPUT_SCHEMA = "dbt_amarquardt"
OUTPUT_TABLE = "mart_generic_labeled"
UPLOAD_VOLUME = f"/Volumes/{CATALOG}/{RAW_SCHEMA}/uploads"
PROCESSED_DIR = f"{UPLOAD_VOLUME}/_processed"

# Tables with their own dedicated pipelines — skip these
EXCLUDE_TABLES = [
    "t12|022023|preserveatgreison",
    "t12|0925|skytop",
]

print(f"Source: {CATALOG}.{RAW_SCHEMA}")
print(f"Output: {CATALOG}.{OUTPUT_SCHEMA}.{OUTPUT_TABLE}")
print(f"Upload Volume: {UPLOAD_VOLUME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: Ingest New Files from Volume
# MAGIC Checks `/Volumes/workspace/rawdatalabeling/uploads/` for new CSV files.
# MAGIC Each file is loaded as a table in `rawdatalabeling` using the filename
# MAGIC as the table name. Processed files are moved to `_processed/`.

# COMMAND ----------

# Ensure _processed directory exists
os.makedirs(PROCESSED_DIR, exist_ok=True)

import re as _re

MONTH_NAMES = {
    'jan': 1, 'january': 1, 'feb': 2, 'february': 2,
    'mar': 3, 'march': 3, 'apr': 4, 'april': 4,
    'may': 5, 'jun': 6, 'june': 6,
    'jul': 7, 'july': 7, 'aug': 8, 'august': 8,
    'sep': 9, 'sept': 9, 'september': 9, 'oct': 10, 'october': 10,
    'nov': 11, 'november': 11, 'dec': 12, 'december': 12,
}

def slugify(name):
    """Convert a filename into a clean table-name slug."""
    name = name.lower().strip()
    name = _re.sub(r'[^a-z0-9]+', '_', name)  # replace non-alphanum with _
    name = _re.sub(r'_+', '_', name).strip('_')  # collapse multiple _
    return name

def extract_date_from_filename(fname):
    """Try to extract a MMYY or MMYYYY date code from a filename."""
    # Look for patterns like "May 22", "May 2022", "05-2022", "052022", etc.
    month_map = {
        'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
        'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
        'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12',
    }
    # "May 22" or "May 2022" or "May-22"
    m = _re.search(r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*[\s\-_]+(\d{2,4})', fname.lower())
    if m:
        mm = month_map.get(m.group(1)[:3], '01')
        yy = m.group(2) if len(m.group(2)) == 4 else m.group(2)
        return f"{mm}{yy}"
    # "05/2022" or "05-2022" or "052022"
    m = _re.search(r'(\d{2})[\-/]?(\d{2,4})', fname)
    if m and 1 <= int(m.group(1)) <= 12:
        return f"{m.group(1)}{m.group(2)}"
    return "0000"

# Find ALL new CSV files in the upload volume (any name)
new_files = [
    f for f in os.listdir(UPLOAD_VOLUME)
    if f.lower().endswith(".csv") and not f.startswith(".")
]

newly_ingested_tables = []

if new_files:
    print(f"Found {len(new_files)} new CSV file(s) to ingest:")
    for fname in new_files:
        file_path = f"{UPLOAD_VOLUME}/{fname}"
        base_name = os.path.splitext(fname)[0]

        # Build a t12| table name from the filename
        if base_name.lower().startswith("t12|"):
            table_name = base_name
        else:
            date_code = extract_date_from_filename(base_name)
            prop_slug = slugify(base_name)
            # Remove date-like parts from the slug for cleaner property name
            prop_slug = _re.sub(r'(12_month_statement_?|t12_?|_?\d{2,4}_?\d{2,4}_?)', '', prop_slug).strip('_')
            if not prop_slug:
                prop_slug = "uploaded"
            table_name = f"t12|{date_code}|{prop_slug}"

        full_table = f"{CATALOG}.{RAW_SCHEMA}.`{table_name}`"
        print(f"  {fname}")
        print(f"    → table: {table_name}")

        # Read CSV WITHOUT headers — we need to find the real header row
        raw = spark.read.option("header", "false").option("inferSchema", "false").csv(file_path)
        all_rows = raw.collect()
        col_count = len(raw.columns)
        print(f"    → {len(all_rows)} rows, {col_count} columns (raw, no header)")

        # Month name pattern for scanning (Jan, Feb, ..., Dec followed by year)
        _MONTH_PAT = r'(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*'

        def is_date_cell(v):
            """Check if a cell value looks like a date header."""
            v = v.strip()
            if not v:
                return False
            # MM/DD/YYYY or MM/DD/YY
            if _re.match(r'^\d{1,2}/\d{1,2}/\d{2,4}$', v):
                return True
            # Mon YYYY, Month YYYY, Mon-YYYY, Mon YY, Mon-YY
            if _re.match(r'^' + _MONTH_PAT + r'[\s\-]+\d{2,4}$', v, _re.IGNORECASE):
                return True
            # M/YYYY or M/YY
            if _re.match(r'^\d{1,2}/\d{2,4}$', v):
                return True
            # YYYY-MM
            if _re.match(r'^\d{4}-\d{1,2}$', v):
                return True
            return False

        # Scan rows to find the one with month dates
        date_row_idx = None
        metadata = {}
        for i, row in enumerate(all_rows):
            vals = [str(v).strip() if v else "" for v in row]

            # Extract metadata from rows above the date header
            first = vals[0] if vals[0] else ""
            first_lower = first.lower()

            # "Location:" or "Property:" style
            if "location" in first_lower or "property" in first_lower:
                val = next((v for v in vals[1:] if v), "")
                # Also handle "Property = z283210 z283211" in same cell
                eq_match = _re.search(r'(?:property|location)\s*[=:]\s*(.+)', first, _re.IGNORECASE)
                if eq_match:
                    val = eq_match.group(1).strip()
                if val:
                    metadata["property"] = val

            # "As of Date:" or "Period =" style
            if "as of date" in first_lower or "period" in first_lower:
                val = next((v for v in vals[1:] if v), "")
                eq_match = _re.search(r'(?:period|as of date)\s*[=:]\s*(.+)', first, _re.IGNORECASE)
                if eq_match:
                    val = eq_match.group(1).strip()
                if val:
                    metadata["as_of_date"] = val

            # Check if this row has date-like values (skip first col which is labels)
            date_count = 0
            for v in vals[1:]:
                if is_date_cell(v):
                    date_count += 1
            if date_count >= 3:
                date_row_idx = i
                sample = [v for v in vals[1:] if v][:4]
                print(f"    → date header found at row {i}: {sample}")
                break

        if date_row_idx is not None:
            # Use dates from the date row as column headers
            date_vals = [str(v).strip() if v else "" for v in all_rows[date_row_idx]]

            # Count leading non-date columns (account_code, account_name, etc.)
            first_date_col = None
            for j in range(col_count):
                if is_date_cell(date_vals[j] if date_vals[j] else ""):
                    first_date_col = j
                    break
            if first_date_col is None:
                first_date_col = 1  # fallback

            # Name the leading columns
            if first_date_col >= 2:
                header_names = ["account_code", "account_name"]
                # If more than 2 leading cols, name them generically
                for k in range(2, first_date_col):
                    header_names.append(f"label_col_{k}")
                print(f"    → {first_date_col} leading cols: account_code + account_name")
            else:
                header_names = ["account_name"]
                print(f"    → 1 leading col: account_name only")

            # Name the date and remaining columns
            for j in range(first_date_col, col_count):
                h = date_vals[j] if date_vals[j] else f"col_{j}"
                # Sanitize for Delta
                h = _re.sub(r'[^a-zA-Z0-9/_\-\.]', '_', h)
                h = _re.sub(r'_+', '_', h).strip('_')
                if not h:
                    h = f"col_{j}"
                header_names.append(h)

            # Skip metadata rows + date row + any "Actual"/"Budget" row right after
            data_start = date_row_idx + 1
            if data_start < len(all_rows):
                next_vals = [str(v).strip().lower() if v else "" for v in all_rows[data_start]]
                if any(v in ('actual', 'budget', 'variance') for v in next_vals):
                    data_start += 1
                    print(f"    → skipping 'Actual/Budget' row at {data_start - 1}")

            # Build DataFrame from data rows only (serverless-compatible, no sparkContext)
            from pyspark.sql.types import StructType, StructField, StringType
            data_rows = all_rows[data_start:]
            schema = StructType([StructField(n, StringType(), True) for n in header_names])
            if data_rows:
                df = spark.createDataFrame([list(r) for r in data_rows], schema=schema)
            else:
                df = spark.createDataFrame([], schema=schema)

            print(f"    → clean columns: {df.columns}")
            print(f"    → data rows: {len(data_rows)} (skipped {data_start} header/metadata rows)")

            # Use as_of_date from CSV metadata for accurate date code (overrides filename)
            if metadata.get("as_of_date"):
                aod = metadata["as_of_date"].strip()
                aod_date_code = None

                # MM/DD/YYYY format (e.g., 01/31/2023)
                aod_match = _re.match(r'^(\d{1,2})/\d{1,2}/(\d{4})$', aod)
                if aod_match:
                    aod_date_code = f"{aod_match.group(1).zfill(2)}{aod_match.group(2)}"

                # "May 2022-Apr 2023" or "May 2022 - Apr 2023" → take the END date
                if not aod_date_code:
                    range_match = _re.search(r'[\-–]\s*([A-Za-z]+)\s+(\d{4})\s*$', aod)
                    if range_match:
                        end_mn = MONTH_NAMES.get(range_match.group(1).lower()[:3])
                        if end_mn:
                            aod_date_code = f"{str(end_mn).zfill(2)}{range_match.group(2)}"

                # "Apr 2023" standalone
                if not aod_date_code:
                    mon_match = _re.match(r'^([A-Za-z]+)\s+(\d{4})$', aod)
                    if mon_match:
                        mn = MONTH_NAMES.get(mon_match.group(1).lower()[:3])
                        if mn:
                            aod_date_code = f"{str(mn).zfill(2)}{mon_match.group(2)}"

                if aod_date_code:
                    date_code = aod_date_code
                    print(f"    → date code from as_of_date '{aod}': {date_code}")

            # Use metadata to refine table name
            if metadata.get("property") and prop_slug in ("uploaded", "detail"):
                prop_slug = slugify(metadata["property"])
            table_name = f"t12|{date_code}|{prop_slug}"
            full_table = f"{CATALOG}.{RAW_SCHEMA}.`{table_name}`"
            print(f"    → final table name: {table_name}")

            # Save metadata rows + date-column mapping as notes
            notes_rows = []
            for i in range(min(date_row_idx, len(all_rows))):
                vals = [str(v).strip() if v else "" for v in all_rows[i]]
                label = vals[0] if vals[0] else ""
                value = " | ".join(v for v in vals[1:] if v)
                if label or value:
                    notes_rows.append((table_name, prop_slug, "metadata", label, value))

            # Store the date-to-month_XX mapping so users know which column is which
            raw_date_vals = [str(v).strip() if v else "" for v in all_rows[date_row_idx]]
            month_idx = 1
            for j in range(first_date_col, col_count):
                raw_date = raw_date_vals[j] if raw_date_vals[j] else ""
                if is_date_cell(raw_date):
                    notes_rows.append((table_name, prop_slug, "month_map",
                        f"month_{str(month_idx).zfill(2)}", raw_date))
                    month_idx += 1

            if notes_rows:
                notes_df = spark.createDataFrame(notes_rows,
                    ["source_table", "property_name", "note_type", "label", "value"])
                notes_table = f"{CATALOG}.{OUTPUT_SCHEMA}.t12_notes"
                notes_df.write.mode("append").option("overwriteSchema", "true").saveAsTable(notes_table)
                print(f"    → saved {len(notes_rows)} notes to {notes_table}")

            if metadata:
                print(f"    → metadata: {metadata}")
        else:
            # No date row found — fall back to simple header=true read
            print(f"    → no date header row found, using row 0 as header")
            df = spark.read.option("header", "true").option("inferSchema", "false").csv(file_path)
            clean_names = []
            for c in df.columns:
                clean = _re.sub(r'[^a-zA-Z0-9/_\-\.]', '_', c.strip())
                clean = _re.sub(r'_+', '_', clean).strip('_') or f"col_{len(clean_names)}"
                base = clean
                ctr = 2
                while clean in clean_names:
                    clean = f"{base}_{ctr}"
                    ctr += 1
                clean_names.append(clean)
            df = df.toDF(*clean_names)

        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table)
        print(f"    ✓ Created table with {df.count()} rows, {len(df.columns)} columns")

        newly_ingested_tables.append(table_name)

        # Move file to _processed
        processed_path = f"{PROCESSED_DIR}/{fname}"
        if os.path.exists(processed_path):
            os.remove(processed_path)
        os.rename(file_path, processed_path)
        print(f"    ✓ Moved to _processed/")
else:
    print("No new files in upload volume. Will process existing tables...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Discover T12 Tables

# COMMAND ----------

if newly_ingested_tables:
    # Process ONLY the tables we just ingested from the Volume
    t12_tables = newly_ingested_tables
    print(f"Processing {len(t12_tables)} newly ingested table(s):")
else:
    # No new files — fall back to processing all existing t12| tables
    tables_df = spark.sql(f"""
        SELECT table_name
        FROM {CATALOG}.information_schema.tables
        WHERE table_schema = '{RAW_SCHEMA}'
          AND lower(table_name) LIKE 't12|%'
        ORDER BY table_name
    """)
    all_tables = [row.table_name for row in tables_df.collect()]
    t12_tables = [t for t in all_tables if t not in EXCLUDE_TABLES]
    print(f"Found {len(t12_tables)} existing t12 table(s):")

for t in t12_tables:
    print(f"  - {t}")

if not t12_tables:
    dbutils.notebook.exit("No t12 tables to process.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Stage All Tables
# MAGIC Auto-detects column layout per table:
# MAGIC - **Structured** (from ingest_t12_docx notebook): has `row_type` column
# MAGIC - **Raw 15+ col**: account_code + account_name + 12 months + total
# MAGIC - **Raw 14 col**: account_name only + 12 months + total

# COMMAND ----------

from pyspark.sql import DataFrame
import pyspark.sql.functions as F
import re
import calendar

# ---------------------------------------------------------------------------
# Date header parser — handles any standard date format in column headers
# (MONTH_NAMES already defined in Step 0)
# ---------------------------------------------------------------------------

def parse_month_year(header):
    """Parse a column header into (year, month) tuple or None.
    Handles: 4/30/2022, 04/30/2022, 4/30/22, Apr 2022, April 2022,
             Apr 22, 4/2022, 4/22, 2022-04, Apr-22, Apr-2022, etc."""
    h = header.strip()

    # MM/DD/YYYY  e.g. 4/30/2022, 04/30/2022
    m = re.match(r'^(\d{1,2})/\d{1,2}/(\d{4})$', h)
    if m and 1 <= int(m.group(1)) <= 12: return (int(m.group(2)), int(m.group(1)))

    # MM/DD/YY  e.g. 4/30/22
    m = re.match(r'^(\d{1,2})/\d{1,2}/(\d{2})$', h)
    if m and 1 <= int(m.group(1)) <= 12: return (2000 + int(m.group(2)), int(m.group(1)))

    # M/YYYY  e.g. 4/2022
    m = re.match(r'^(\d{1,2})/(\d{4})$', h)
    if m and 1 <= int(m.group(1)) <= 12: return (int(m.group(2)), int(m.group(1)))

    # M/YY  e.g. 4/22
    m = re.match(r'^(\d{1,2})/(\d{2})$', h)
    if m and 1 <= int(m.group(1)) <= 12: return (2000 + int(m.group(2)), int(m.group(1)))

    # Mon YYYY or Month YYYY  e.g. Apr 2022, April 2022, Apr-2022
    m = re.match(r'^([A-Za-z]+)[\s\-]+(\d{4})$', h)
    if m:
        mn = MONTH_NAMES.get(m.group(1).lower().rstrip('.'))
        if mn: return (int(m.group(2)), mn)

    # Mon YY or Month YY  e.g. Apr 22, April 22, Apr-22
    m = re.match(r'^([A-Za-z]+)[\s\-]+(\d{2})$', h)
    if m:
        mn = MONTH_NAMES.get(m.group(1).lower().rstrip('.'))
        if mn: return (2000 + int(m.group(2)), mn)

    # YYYY-MM  e.g. 2022-04
    m = re.match(r'^(\d{4})-(\d{1,2})$', h)
    if m and 1 <= int(m.group(2)) <= 12: return (int(m.group(1)), int(m.group(2)))

    return None

def is_total_col(header):
    """Check if a column header is a total/budget/annual column (not a month)."""
    h = header.strip().lower()
    return h in ('total', 'totals', 'budget', 'annual', 'ytd', 'year total',
                 'trailing 12', 't12', 't-12', '12 month total', 'total/avg',
                 'actual', 'variance')

def parse_period_end(date_code):
    """Parse MMYY or MMYYYY date code from table name into (year, month)."""
    date_code = date_code.strip()
    try:
        if len(date_code) == 4:   # MMYY
            month, year = int(date_code[:2]), 2000 + int(date_code[2:])
        elif len(date_code) == 6: # MMYYYY
            month, year = int(date_code[:2]), int(date_code[2:])
        else:
            return None
        if 1 <= month <= 12:
            return (year, month)
    except ValueError:
        pass
    return None

def compute_month_labels(period_end, num_months=12):
    """Given (year, month) period end, compute calendar labels for month_01..month_N."""
    if not period_end:
        return None
    end_year, end_month = period_end
    labels = []
    for i in range(num_months):
        offset = num_months - 1 - i
        m = end_month - offset
        y = end_year
        while m <= 0:
            m += 12
            y -= 1
        labels.append(f"{calendar.month_abbr[m]} {y}")
    return labels

def detect_account_codes(raw_df, col_name):
    """Sample first column values to detect if they look like account codes (e.g. 4100-0100)."""
    sample_rows = raw_df.select(F.col(f"`{col_name}`")).filter(
        F.col(f"`{col_name}`").isNotNull() &
        (F.trim(F.col(f"`{col_name}`")) != "")
    ).limit(20).collect()
    sample_vals = [str(r[0]).strip() for r in sample_rows]
    if not sample_vals:
        return False
    # Account codes: digits with optional dashes/dots, e.g. 4100-0100, 5000, 5000.01
    code_count = sum(1 for v in sample_vals if re.match(r'^[\d][\d\-\.]*[\d]$', v) or re.match(r'^\d+$', v))
    return code_count > len(sample_vals) * 0.5

# ---------------------------------------------------------------------------

staged_dfs = []

for table_name in t12_tables:
    parts = table_name.split("|")
    property_slug = parts[2] if len(parts) >= 3 else table_name
    date_code = parts[1] if len(parts) >= 2 else ""

    # Parse period end from table name for calendar month labels
    period_end = parse_period_end(date_code)
    if period_end:
        print(f"\n  {table_name}: period ends {calendar.month_abbr[period_end[1]]} {period_end[0]}")

    full_name = f"{CATALOG}.{RAW_SCHEMA}.`{table_name}`"
    raw_df = spark.sql(f"SELECT * FROM {full_name}")
    cols = raw_df.columns
    num_cols = len(cols)
    first_col = cols[0]

    if first_col == "row_type":
        # Structured source (from ingest notebook)
        month_labels = compute_month_labels(period_end) or [f"month_{str(m).zfill(2)}" for m in range(1, 13)]
        print(f"    STRUCTURED format ({num_cols} cols)")
        print(f"    Months: {', '.join(month_labels)}")
        staged = raw_df.select(
            F.lit(property_slug).alias("property_name"),
            F.col("account_code"),
            F.trim(F.col("account_name")).alias("account_name"),
            *[F.col(f"month_{str(m).zfill(2)}").cast("string").alias(f"month_{str(m).zfill(2)}") for m in range(1, 13)],
            F.col("total").cast("string").alias("total"),
            F.col("row_idx").cast("bigint").alias("row_num"),
            F.lit(f"{period_end[0]}-{str(period_end[1]).zfill(2)}" if period_end else None).alias("period_end_date"),
        )
    else:
        # Raw source — dynamically detect column layout
        # Check for explicit column names from our smart ingestion first
        if first_col == "account_code" and len(cols) > 1 and cols[1] == "account_name":
            has_account_code = True
            print(f"    RAW {num_cols}-col WITH account_code + account_name columns (from CSV ingestion)")
        elif first_col == "account_name":
            has_account_code = False
            print(f"    RAW {num_cols}-col, account_name column (from CSV ingestion)")
        else:
            # Legacy tables: sample actual values to detect account codes
            has_account_code = detect_account_codes(raw_df, first_col)
            if has_account_code:
                print(f"    RAW {num_cols}-col WITH account codes (sampled first col)")
            else:
                print(f"    RAW {num_cols}-col, NO account codes (first col = '{first_col}')")

        if has_account_code:
            acct_code_col = first_col
            acct_name_col = cols[1]
            data_cols = cols[2:]
        else:
            acct_code_col = None
            acct_name_col = first_col
            data_cols = cols[1:]

        # Separate total/budget columns from month columns
        total_col = None
        month_candidates = []
        for c in data_cols:
            if is_total_col(c):
                if total_col is None:
                    total_col = c
                # skip additional total/budget/variance columns
            else:
                month_candidates.append(c)

        # Try to parse date headers for smart chronological ordering
        dated_cols = []
        undated_cols = []
        for c in month_candidates:
            parsed = parse_month_year(c)
            if parsed:
                dated_cols.append((c, parsed))
            else:
                undated_cols.append(c)

        if dated_cols and len(dated_cols) >= len(month_candidates) * 0.5:
            # DATE HEADERS FOUND — sort chronologically
            dated_cols.sort(key=lambda x: x[1])
            month_cols_ordered = [c[0] for c in dated_cols]
            month_labels = [f"{calendar.month_abbr[c[1][1]]} {c[1][0]}" for c in dated_cols]
            print(f"    DATE HEADERS detected ({len(dated_cols)} months):")
            for i, c in enumerate(dated_cols):
                print(f"      {c[0]} → month_{str(i+1).zfill(2)} ({calendar.month_abbr[c[1][1]]} {c[1][0]})")
        else:
            # NO date headers — use positional mapping
            month_cols_ordered = month_candidates
            # Compute labels from table name if possible
            month_labels = compute_month_labels(period_end, len(month_cols_ordered))
            if month_labels:
                print(f"    Positional mapping ({len(month_cols_ordered)} months), calendar from table name:")
                for i, lbl in enumerate(month_labels):
                    print(f"      {month_cols_ordered[i]} → month_{str(i+1).zfill(2)} ({lbl})")
            else:
                print(f"    Positional mapping ({len(month_cols_ordered)} months, no date info)")

        # If no explicit total column found, assume last data column is total
        if total_col is None and len(month_cols_ordered) > 12:
            total_col = month_cols_ordered[-1]
            month_cols_ordered = month_cols_ordered[:-1]
            print(f"    Last col '{total_col}' assumed as total (>12 data cols)")
        elif total_col is None and len(month_cols_ordered) > 1:
            total_col = month_cols_ordered[-1]
            month_cols_ordered = month_cols_ordered[:-1]
            print(f"    Last col '{total_col}' assumed as total")

        # Build select expressions
        select_exprs = [
            F.lit(property_slug).alias("property_name"),
            F.col(f"`{acct_code_col}`").alias("account_code") if has_account_code else F.lit(None).cast("string").alias("account_code"),
            F.trim(F.col(f"`{acct_name_col}`")).alias("account_name"),
        ]

        # Map month columns to month_01..month_12, null-fill the rest
        for m in range(1, 13):
            month_label = f"month_{str(m).zfill(2)}"
            if m - 1 < len(month_cols_ordered):
                select_exprs.append(F.col(f"`{month_cols_ordered[m-1]}`").alias(month_label))
            else:
                select_exprs.append(F.lit(None).cast("string").alias(month_label))

        # Total + row number + period info
        if total_col:
            select_exprs.append(F.col(f"`{total_col}`").alias("total"))
        else:
            select_exprs.append(F.lit(None).cast("string").alias("total"))
        select_exprs.append(F.monotonically_increasing_id().cast("bigint").alias("row_num"))
        select_exprs.append(F.lit(f"{period_end[0]}-{str(period_end[1]).zfill(2)}" if period_end else None).alias("period_end_date"))

        staged = raw_df.select(*select_exprs)

    staged_dfs.append(staged)

# Union all staged tables
from functools import reduce
staged_all = reduce(DataFrame.unionAll, staged_dfs)
staged_all.createOrReplaceTempView("staged")

print(f"\n✓ Staged {staged_all.count()} total rows from {len(t12_tables)} table(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Classify — Section Headers → Categories
# MAGIC Replicates `int_generic_t12` logic: detect section headers, propagate down,
# MAGIC assign `section_category` and `section_subcategory`.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMP VIEW with_headers AS
SELECT
    *,
    CASE
        WHEN trim(account_name) IN (
            'Rental Income',
            'Vacancy, Losses & Concessions', 'Vacancy, Loss & Concessions',
            'Vacancy Loss & Concessions', 'Concessions', 'Lost Rent',
            'Other Income', 'Non-Revenue Income',
            'Payroll & Related', 'Payroll',
            'Maintenance & Repairs', 'Repairs & Maintenance',
            'Turnover Expense', 'Turnover', 'Turnkey',
            'Contract Services', 'Contract',
            'Marketing Expenses', 'Marketing',
            'Administrative Expenses', 'General & Administrative',
            'Utilities',
            'Management Fees', 'Management Fee', 'Management & Professional Fees',
            'Taxes & Insurance', 'Taxes', 'Insurance', 'Property Taxes',
            'Bad Debt',
            'Interest & Misc Expense', 'Capital Expenses', 'Operating Expenses'
        )
        AND nullif(trim(coalesce(month_01, '')), '') IS NULL
        THEN trim(account_name)
        ELSE NULL
    END AS section_header
FROM staged
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW propagated AS
SELECT
    *,
    last_value(section_header, true) OVER (
        PARTITION BY property_name
        ORDER BY row_num
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS current_section
FROM with_headers
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW line_items AS
SELECT *
FROM propagated
WHERE current_section IS NOT NULL
  AND section_header IS NULL
  AND trim(coalesce(account_name, '')) != ''
  AND upper(account_name) NOT LIKE '%TOTAL%'
  AND upper(account_name) NOT LIKE 'NET %'
  AND upper(account_name) NOT LIKE '%SUBTOTAL%'
  AND upper(account_name) != 'INCOME'
  AND upper(account_name) != 'EXPENSES'
  AND upper(account_name) != 'CONTROLLABLE EXPENSES'
  AND upper(account_name) != 'NON-CONTROLLABLE EXPENSES'
  AND upper(account_name) != 'ACTUAL'
  AND account_name NOT RLIKE '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
  AND upper(account_name) != 'MONTH ENDING'
  AND upper(account_name) != 'OPERATING EXPENSES'
  AND current_section NOT IN ('Interest & Misc Expense', 'Capital Expenses')
""")

classified_df = spark.sql("""
SELECT
    property_name,

    CASE
        WHEN current_section IN (
            'Rental Income', 'Vacancy, Losses & Concessions',
            'Vacancy, Loss & Concessions', 'Vacancy Loss & Concessions',
            'Concessions', 'Lost Rent',
            'Other Income', 'Non-Revenue Income'
        ) THEN 'Revenue'
        ELSE 'Expenses'
    END AS section_category,

    CASE
        WHEN current_section = 'Rental Income' THEN 'Rental Income'
        WHEN current_section IN ('Vacancy, Losses & Concessions',
                                 'Vacancy, Loss & Concessions',
                                 'Vacancy Loss & Concessions',
                                 'Concessions', 'Lost Rent')
            THEN 'Collection Loss'
        WHEN current_section = 'Rental Income'
             AND lower(trim(account_name)) = 'vacancy'
            THEN 'Collection Loss'
        WHEN current_section = 'Other Income'
             AND (lower(account_name) LIKE '%electric rebill%'
                  OR lower(account_name) LIKE '%electric reimbursement%'
                  OR lower(account_name) LIKE '%trash rebill%'
                  OR lower(account_name) LIKE '%trash reimbursement%'
                  OR lower(account_name) LIKE '%gas rebill%'
                  OR lower(account_name) LIKE '%gas reimbursement%'
                  OR lower(account_name) LIKE '%water/sewer rebill%'
                  OR lower(account_name) LIKE '%water/sewer reimbursement%'
                  OR lower(account_name) LIKE '%water sewer rebill%'
                  OR lower(account_name) LIKE '%water sewer reimbursement%'
                  OR lower(account_name) LIKE '%water / sewer reimbursement%'
                  OR lower(account_name) = 'utility income')
             AND lower(account_name) NOT LIKE '%pest control%'
            THEN 'RUBS'
        WHEN current_section IN ('Other Income', 'Non-Revenue Income') THEN 'Other Income'
        WHEN current_section IN ('Payroll & Related', 'Payroll') THEN 'Payroll'
        WHEN current_section IN ('Maintenance & Repairs', 'Repairs & Maintenance') THEN 'Repairs & Maintenance'
        WHEN current_section IN ('Turnover Expense', 'Turnover', 'Turnkey') THEN 'Turnover'
        WHEN current_section IN ('Contract Services', 'Contract')
             AND lower(account_name) LIKE '%trash removal%'
            THEN 'Utilities'
        WHEN current_section IN ('Contract Services', 'Contract') THEN 'Contract'
        WHEN current_section IN ('Marketing Expenses', 'Marketing') THEN 'Marketing'
        WHEN current_section IN ('Administrative Expenses', 'General & Administrative') THEN 'General & Administrative'
        WHEN current_section = 'Utilities' THEN 'Utilities'
        WHEN current_section IN ('Management Fees', 'Management Fee', 'Management & Professional Fees') THEN 'Management Fees'
        WHEN current_section = 'Taxes & Insurance'
             AND lower(account_name) LIKE '%insurance%'
            THEN 'Insurance'
        WHEN current_section IN ('Taxes & Insurance', 'Taxes', 'Property Taxes') THEN 'Taxes'
        WHEN current_section = 'Insurance' THEN 'Insurance'
        WHEN current_section = 'Bad Debt' THEN 'Collection Loss'
        ELSE 'Other'
    END AS section_subcategory,

    account_code,
    account_name,

    try_cast(replace(replace(replace(nullif(trim(month_01), ''), ',', ''), '(', '-'), ')', '') AS double) AS month_01,
    try_cast(replace(replace(replace(nullif(trim(month_02), ''), ',', ''), '(', '-'), ')', '') AS double) AS month_02,
    try_cast(replace(replace(replace(nullif(trim(month_03), ''), ',', ''), '(', '-'), ')', '') AS double) AS month_03,
    try_cast(replace(replace(replace(nullif(trim(month_04), ''), ',', ''), '(', '-'), ')', '') AS double) AS month_04,
    try_cast(replace(replace(replace(nullif(trim(month_05), ''), ',', ''), '(', '-'), ')', '') AS double) AS month_05,
    try_cast(replace(replace(replace(nullif(trim(month_06), ''), ',', ''), '(', '-'), ')', '') AS double) AS month_06,
    try_cast(replace(replace(replace(nullif(trim(month_07), ''), ',', ''), '(', '-'), ')', '') AS double) AS month_07,
    try_cast(replace(replace(replace(nullif(trim(month_08), ''), ',', ''), '(', '-'), ')', '') AS double) AS month_08,
    try_cast(replace(replace(replace(nullif(trim(month_09), ''), ',', ''), '(', '-'), ')', '') AS double) AS month_09,
    try_cast(replace(replace(replace(nullif(trim(month_10), ''), ',', ''), '(', '-'), ')', '') AS double) AS month_10,
    try_cast(replace(replace(replace(nullif(trim(month_11), ''), ',', ''), '(', '-'), ')', '') AS double) AS month_11,
    try_cast(replace(replace(replace(nullif(trim(month_12), ''), ',', ''), '(', '-'), ')', '') AS double) AS month_12,
    try_cast(replace(replace(replace(nullif(trim(total), ''), ',', ''), '(', '-'), ')', '') AS double) AS total,
    row_num,
    period_end_date

FROM line_items
""")

classified_df.createOrReplaceTempView("classified")
print(f"✓ Classified {classified_df.count()} line items")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Apply Labels

# COMMAND ----------

labeled_df = spark.sql("""
SELECT
    property_name,
    account_code,
    account_name,
    section_category,
    section_subcategory,
    CASE
        WHEN section_subcategory = 'Rental Income' THEN null
        WHEN section_subcategory = 'General & Administrative' THEN 'G&A'
        ELSE section_subcategory
    END AS proprietary_labeling,
    month_01, month_02, month_03, month_04,
    month_05, month_06, month_07, month_08,
    month_09, month_10, month_11, month_12,
    total,
    row_num,
    period_end_date
FROM classified
ORDER BY property_name, row_num
""")

labeled_df.createOrReplaceTempView("labeled")
display(labeled_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Write Output

# COMMAND ----------

# Write per-property staging tables with CALENDAR MONTH column names
properties = [r.property_name for r in labeled_df.select("property_name").distinct().collect()]

# Try to read month mapping from notes table
month_map_by_prop = {}
try:
    notes_table = f"{CATALOG}.{OUTPUT_SCHEMA}.t12_notes"
    notes_df = spark.sql(f"""
        SELECT property_name, label, value
        FROM {notes_table}
        WHERE note_type = 'month_map'
    """)
    for row in notes_df.collect():
        month_map_by_prop.setdefault(row.property_name, {})[row.label] = row.value
except Exception:
    pass

for prop in properties:
    prop_df = labeled_df.filter(F.col("property_name") == prop)

    # Rename month columns to calendar months for this property
    renamed_df = prop_df
    if prop in month_map_by_prop:
        mapping = month_map_by_prop[prop]
        for generic_name, date_val in mapping.items():
            # Parse date to friendly name: "02/28/2022" → "Feb_2022"
            parsed = parse_month_year(date_val)
            if parsed:
                friendly = f"{calendar.month_abbr[parsed[1]]}_{parsed[0]}"
                renamed_df = renamed_df.withColumnRenamed(generic_name, friendly)
        print(f"  ✓ {prop}: month columns → {[c for c in renamed_df.columns if '_20' in c or 'month_' in c]}")
    elif prop_df.select("period_end_date").first() and prop_df.select("period_end_date").first()[0]:
        # Fall back to period_end_date to compute month names
        ped = prop_df.select("period_end_date").first()[0]
        ped_parts = ped.split("-")
        if len(ped_parts) == 2:
            pe = (int(ped_parts[0]), int(ped_parts[1]))
            labels = compute_month_labels(pe, 12)
            if labels:
                for i, lbl in enumerate(labels):
                    generic = f"month_{str(i+1).zfill(2)}"
                    friendly = lbl.replace(" ", "_")
                    renamed_df = renamed_df.withColumnRenamed(generic, friendly)
                print(f"  ✓ {prop}: month columns from period_end → {[c for c in renamed_df.columns if '_20' in c or 'month_' in c]}")

    prop_table = f"{CATALOG}.{OUTPUT_SCHEMA}.stg_t12_{prop}"
    renamed_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(prop_table)
    print(f"  ✓ {prop_table} — {renamed_df.count()} rows")

# Also write combined table for convenience
output_full = f"{CATALOG}.{OUTPUT_SCHEMA}.{OUTPUT_TABLE}"
labeled_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_full)

row_count = labeled_df.count()
print(f"\n✓ Written {row_count} total rows to {output_full}")
print(f"  Properties: {', '.join(properties)}")
print(f"  Per-property tables: stg_t12_<property> in {CATALOG}.{OUTPUT_SCHEMA}")
print(f"\nStructured output is now live in Databricks!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary by Property

# COMMAND ----------

display(spark.sql("""
    SELECT
        property_name,
        period_end_date,
        count(*) as line_items,
        count(distinct section_subcategory) as categories,
        round(sum(coalesce(total, 0)), 2) as grand_total
    FROM labeled
    GROUP BY property_name, period_end_date
    ORDER BY property_name
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: File Arrival Trigger
# MAGIC
# MAGIC **One-time setup (already done):**
# MAGIC 1. `CREATE VOLUME IF NOT EXISTS workspace.rawdatalabeling.uploads;`
# MAGIC 2. Create Job → select this notebook → File arrival trigger → `/Volumes/workspace/rawdatalabeling/uploads/`
# MAGIC
# MAGIC **Ongoing workflow:**
# MAGIC 1. Drop a CSV into `/Volumes/workspace/rawdatalabeling/uploads/`
# MAGIC 2. Name it `t12|MMYY|propertyname.csv` (e.g., `t12|0225|newproperty.csv`)
# MAGIC 3. File arrival trigger fires automatically
# MAGIC 4. This notebook ingests the file → creates table → structures everything
# MAGIC 5. Structured output appears in `workspace.dbt_amarquardt.mart_generic_labeled`
# MAGIC
# MAGIC **That's it — drop a file, get structured data.**
