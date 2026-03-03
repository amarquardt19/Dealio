# Databricks notebook source
# MAGIC %md
# MAGIC # Auto-Label T12 Operating Statements
# MAGIC **Drop a raw T12 into `rawdatalabeling` → Run this notebook → Get labeled output.**
# MAGIC No manual staging models needed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Find all raw tables in rawdatalabeling

# COMMAND ----------

catalog = "dealio_emilio"  # Azure Databricks catalog
schema = "rawdatalabeling"

tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
table_names = [row.tableName for row in tables]

print("Tables found in rawdatalabeling:")
for t in table_names:
    print(f"  - {t}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Pick which table to process
# MAGIC Change the index below or set the table name directly.

# COMMAND ----------

# Set this to the table you want to process:
target_table = "t12|1223|junctionatvinings"

print(f"Processing: {catalog}.{schema}.`{target_table}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Preview raw data + AI detects column layout

# COMMAND ----------

raw_df = spark.sql(f"SELECT * FROM {catalog}.{schema}.`{target_table}` LIMIT 20")
display(raw_df)

# COMMAND ----------

# Sample first 8 rows as text for the AI to analyze
sample_rows = spark.sql(f"SELECT * FROM {catalog}.{schema}.`{target_table}` LIMIT 8").toPandas()
sample_text = sample_rows.to_string()

prompt = f"""You are analyzing a raw multifamily apartment T12 operating statement loaded into a database table.
The columns are named _c0, _c1, _c2, etc.

Here are the first 8 rows:

{sample_text}

Based on this data, identify:
1. Which column contains account codes (format like 4001-0000 or 5211-000)
2. Which column contains the account name/description
3. Which columns contain monthly dollar amounts (there should be ~12 consecutive columns)
4. Which column contains the annual total
5. Which column contains the proprietary label (the category label assigned by the property manager, usually the last meaningful column)
6. How many header rows should be skipped (rows before actual line item data starts)

Respond ONLY in this exact format with no other text:
ACCOUNT_CODE_COL=_cX
ACCOUNT_NAME_COL=_cX
FIRST_MONTH_COL=_cX
LAST_MONTH_COL=_cX
TOTAL_COL=_cX
LABEL_COL=_cX
SKIP_ROWS=N"""

# Use ai_query to detect the layout
result = spark.sql(f"""
    SELECT ai_query('databricks-meta-llama-3-3-70b-instruct', '{prompt.replace("'", "''")}') AS mapping
""").collect()[0].mapping

print("AI detected column mapping:")
print(result)

# COMMAND ----------

# Parse the AI response into a dict
col_map = {}
for line in result.strip().split('\n'):
    line = line.strip()
    if '=' in line:
        key, val = line.split('=', 1)
        col_map[key.strip()] = val.strip()

print("Parsed mapping:")
for k, v in col_map.items():
    print(f"  {k}: {v}")

acct_code = col_map.get('ACCOUNT_CODE_COL', '_c0')
acct_name = col_map.get('ACCOUNT_NAME_COL', '_c1')
first_month = col_map.get('FIRST_MONTH_COL', '_c2')
last_month = col_map.get('LAST_MONTH_COL', '_c13')
total_col = col_map.get('TOTAL_COL', '_c14')
label_col = col_map.get('LABEL_COL', '_c15')
skip_rows = int(col_map.get('SKIP_ROWS', '4'))

# Figure out month column range
first_idx = int(first_month.replace('_c', ''))
last_idx = int(last_month.replace('_c', ''))
month_cols = [f"_c{i}" for i in range(first_idx, last_idx + 1)]

print(f"\nMonth columns: {month_cols}")
print(f"Skip first {skip_rows} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Structure the raw data

# COMMAND ----------

# Build the month column aliases
month_aliases = [f"CAST(`{col}` AS DOUBLE) AS month_{str(i+1).zfill(2)}" for i, col in enumerate(month_cols)]
month_sql = ",\n    ".join(month_aliases)

structure_sql = f"""
SELECT
    `{acct_code}` AS account_code,
    `{acct_name}` AS account_name,
    {month_sql},
    CAST(`{total_col}` AS DOUBLE) AS total,
    `{label_col}` AS proprietary_labeling
FROM {catalog}.{schema}.`{target_table}`
WHERE `{acct_code}` IS NOT NULL
  AND `{acct_code}` RLIKE '^[0-9]'
"""

print("Generated SQL:")
print(structure_sql)

structured_df = spark.sql(structure_sql)
display(structured_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: AI labels every line item

# COMMAND ----------

# Get existing labeling patterns from past properties (if mart exists)
try:
    context_df = spark.sql("""
        SELECT DISTINCT account_name, section_subcategory, proprietary_labeling
        FROM dbt_amarquardt.mart_label_mismatches
    """).toPandas()
    context_lines = "\n".join(
        f"  {row.account_name} → {row.section_subcategory} (source: {row.proprietary_labeling})"
        for _, row in context_df.iterrows()
    )
    context_block = f"Here are labeling patterns from previously processed properties:\n{context_lines}\n\n"
except:
    context_block = ""
    print("No existing mart_label_mismatches found — labeling from scratch.")

# Build AI labeling query
label_sql = f"""
SELECT
    account_code,
    account_name,
    proprietary_labeling,
    total,
    ai_query(
        'databricks-meta-llama-3-3-70b-instruct',
        concat(
            'You are labeling line items on a multifamily apartment operating statement (T12). ',
            'The standard sections are: Rental Income, Other Income, Utility Income (RUBS), ',
            'Payroll, Repairs & Maintenance, Turnover, Contract, General & Administrative, ',
            'Marketing, Utilities, Insurance, Management Fees, Taxes, Capital Expenditures, Debt Service, Collection Loss.\\n\\n',
            '{context_block.replace(chr(39), chr(39)+chr(39))}',
            'Now label this line item:\\n',
            '- Account Code: ', account_code, '\\n',
            '- Account Name: ', account_name, '\\n',
            '- Source Label: ', COALESCE(proprietary_labeling, 'NONE'), '\\n',
            '- Annual Total: $', CAST(ROUND(total, 2) AS STRING), '\\n\\n',
            'Respond with ONLY the label name. Nothing else.'
        )
    ) AS ai_label
FROM structured_t12
WHERE account_code IS NOT NULL
"""

# Register the structured data as a temp view so we can query it
structured_df.createOrReplaceTempView("structured_t12")

labeled_df = spark.sql(label_sql)
display(labeled_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Save the labeled output

# COMMAND ----------

# Property name from the table name
property_name = target_table.split("|")[-1].replace("_", " ").title()

# Add metadata columns
from pyspark.sql.functions import lit

output_df = labeled_df.withColumn("property_name", lit(property_name)) \
                       .withColumn("source_table", lit(target_table))

# Write to a persistent table
output_table = f"{catalog}.dbt_amarquardt.auto_labeled_{target_table.split('|')[-1]}"
output_df.write.mode("overwrite").saveAsTable(output_table)

print(f"✓ Labeled output saved to: {output_table}")
print(f"  Rows: {output_df.count()}")
display(output_df)
