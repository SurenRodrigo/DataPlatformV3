## `reporting.financial_reporting_data_2026` – Table Structure

> NOTE: This file documents the structure of the `reporting.financial_reporting_data_2026` table in the `dataplatform` Azure SQL database. The actual column metadata needs to be captured by running the diagnostic query below from an environment that has network access and a working ODBC driver.

### How to inspect the table structure

Run the following SQL against `dataplatform`:

```sql
SELECT 
    c.COLUMN_NAME,
    c.DATA_TYPE,
    c.CHARACTER_MAXIMUM_LENGTH,
    c.NUMERIC_PRECISION,
    c.NUMERIC_SCALE,
    c.IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS c
WHERE c.TABLE_SCHEMA = 'reporting'
  AND c.TABLE_NAME = 'financial_reporting_data_2026'
ORDER BY c.ORDINAL_POSITION;
```

Optionally, also capture the row count:

```sql
SELECT COUNT(*) AS row_count
FROM [reporting].[financial_reporting_data_2026];
```

### Suggested Python helper (reuses `app/.env` service principal settings)

From the `bridgestone_dataplatform/dbt_poc` folder (inside your `.venv`), you can run:

```python
import os
import pyodbc
from pathlib import Path

root = Path(__file__).resolve().parents[1]  # project root
env_file = root / "app" / ".env"
if env_file.exists():
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ[k.strip()] = v.strip().strip('"').strip("'")

server = os.getenv("AZURE_SQL_SERVER_FQDN")
database = os.getenv("AZURE_SQL_DATABASE_NAME", "dataplatform")
client_id = os.getenv("AZURE_DATAPLATFORM_DATA_CLIENT_ID")
client_secret = os.getenv("AZURE_DATAPLATFORM_DATA_CLIENT_SECRET")
tenant_id = os.getenv("AZURE_DATAPLATFORM_DATA_TENANT_ID")

driver = "ODBC Driver 18 for SQL Server"
conn_str = (
    f"Driver={{{driver}}};"
    f"Server=tcp:{server},1433;"
    f"Database={database};"
    f"Uid={client_id};"
    f"Pwd={client_secret};"
    f"Encrypt=yes;"
    f"TrustServerCertificate=no;"
    f"Connection Timeout=60;"
    f"Authentication=ActiveDirectoryServicePrincipal"
)

cn = pyodbc.connect(conn_str)
cur = cn.cursor()

cur.execute("""
SELECT 
    c.COLUMN_NAME,
    c.DATA_TYPE,
    c.CHARACTER_MAXIMUM_LENGTH,
    c.NUMERIC_PRECISION,
    c.NUMERIC_SCALE,
    c.IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS c
WHERE c.TABLE_SCHEMA = 'reporting'
  AND c.TABLE_NAME = 'financial_reporting_data_2026'
ORDER BY c.ORDINAL_POSITION
""")

print("COLUMN_NAME | DATA_TYPE | CHAR_MAX_LEN | NUM_PRECISION | NUM_SCALE | IS_NULLABLE")
for row in cur.fetchall():
    print(" | ".join(str(x) if x is not None else "" for x in row))

cur.execute("SELECT COUNT(*) FROM [reporting].[financial_reporting_data_2026]")
print("ROW_COUNT:", cur.fetchone()[0])

cn.close()
```

### Documented structure (from latest investigation)

- **Row count**: `120,497` rows

**Columns:**

- **`customer_name`** (`nvarchar(max)`): nullable `YES` – Customer name for the invoice/credit line.
- **`item_code`** (`nvarchar(max)`): nullable `YES` – Product/item code.
- **`seller_id`** (`bigint` (19,0)): nullable `YES` – Internal numeric identifier of the seller.
- **`seller_name`** (`nvarchar(max)`): nullable `YES` – Name of the seller / account manager.
- **`quantity`** (`float` (53)): nullable `YES` – Quantity sold/credited for the line.
- **`line_total`** (`float` (53)): nullable `YES` – Line-level monetary amount (e.g. net sales or credit).
- **`gross_profit`** (`float` (53)): nullable `YES` – Line-level gross profit for the transaction.
- **`customer_code`** (`bigint` (19,0)): nullable `YES` – Numeric customer identifier (foreign key to customer dimension).
- **`items_group_name`** (`nvarchar(max)`): nullable `YES` – Item group / product group description.
- **`posted_date`** (`datetime2`): nullable `YES` – Posting date of the transaction.
- **`end_of_month_bucket`** (`datetime2`): nullable `YES` – Last day of the month for `posted_date` (used for monthly bucketing).
- **`item_category`** (`nvarchar(max)`): nullable `YES` – High‑level item category, derived from account–category mapping.
- **`year`** (`bigint` (19,0)): nullable `YES` – Calendar year extracted from `posted_date`.
- **`customer_group`** (`nvarchar(max)`): nullable `YES` – Customer grouping/segment (e.g. channel or region), from customer master.
- **`cohort`** (`float` (53)): nullable `YES` – Numeric cohort value for the customer (e.g. cohort year or index).
- **`ipc`** (`nvarchar(max)`): nullable `YES` – Item purchase code / supplier catalog number from product master.
- **`dim`** (`nvarchar(max)`): nullable `YES` – Free‑form dimension / concatenated dimension key (exact semantics depend on upstream model).
- **`record_type`** (`nvarchar(max)`): nullable `YES` – Record classification (e.g. invoice vs credit, or other flags).
- **`_sync_checksum`** (`varchar(64)`): nullable `YES` – Technical checksum used by the sync process for change detection.
- **`_sync_updated_at`** (`datetime2`): nullable `YES` – Timestamp when the record was last updated by the sync process.

This file now serves as the single-source documentation for the `financial_reporting_data_2026` table used by the reporting layer.

---

## Data type optimization recommendations (SQL Server)

Given this table is primarily used for **financial reporting** on SQL Server, there are some opportunities to make the data types more precise and efficient while remaining analytics‑friendly.

### 1. Monetary and numeric columns

- **`line_total`**, **`gross_profit`**  
  - **Current**: `float` (binary floating point)  
  - **Recommended**: `decimal(19,4)` (or `decimal(18,2)` if you are absolutely sure 2 decimals are enough)  
  - **Reasoning**:
    - `float` is an approximate type and can introduce rounding artifacts in sums, joins, and comparisons.
    - For money, SQL Server best practice in 2025 is to use `decimal(p,s)` rather than `money` or `float`, with `decimal(19,4)` a common choice that matches the internal range/precision of `money` but is explicit and portable.
    - Reporting aggregates (e.g. total revenue, margin) will be more accurate and predictable with `decimal`.

- **`quantity`**  
  - **Current**: `float`  
  - **Recommended**: `decimal(18,3)` (or `decimal(18,0)` if always whole units)  
  - **Reasoning**:
    - Same concern as above: `float` is approximate.
    - Quantities are typically finite-precision; using `decimal` avoids subtle rounding issues in totals and ratios.

- **`year`**  
  - **Current**: `bigint`  
  - **Recommended**: `smallint` (or `int` if you prefer consistency)  
  - **Reasoning**:
    - Years fall well within the `smallint` range (–32,768 to 32,767). Using `bigint` is over‑sized and wastes space.
    - For a reporting table with hundreds of thousands or millions of rows, down‑sizing year saves storage and I/O.

- **`cohort`**  
  - **Current**: `float`  
  - **Recommended**:  
    - If it represents a **year** (e.g. cohort year): `smallint`  
    - If it represents a **ratio or score**: `decimal(5,2)` or similar (depending on required range/precision)  
  - **Reasoning**:
    - Again, avoid `float` for business metrics where exactness matters.
    - Choosing an explicit numeric scale makes cohort‑based segmenting and filtering more robust.

### 2. String columns

Many columns are currently `nvarchar(max)` (reported as length –1). For reporting‑oriented tables on SQL Server, best practice is to **avoid `nvarchar(max)` unless you truly need very long text**, because:

- `nvarchar(max)` cannot participate fully in certain indexes and columnstore indexes.
- Values larger than 8 KB are stored out‑of‑row and can add overhead.
- It’s harder to reason about cardinality and storage.

Suggested bounds (adjust after profiling real data):

- **`customer_name`**, **`seller_name`**  
  - **Recommended**: `nvarchar(200)`  
  - Reasoning: Typical business names comfortably fit within 200 characters; still leaves room for long legal names.

- **`item_code`**  
  - **Current**: `nvarchar(max)`  
  - **Recommended**: `nvarchar(50)` (or `nvarchar(100)` if codes are longer)  
  - Reasoning: Item codes are usually short and benefit from being in normal `nvarchar(n)` for indexing and joins.

- **`items_group_name`**, **`item_category`**, **`customer_group`**  
  - **Recommended**: `nvarchar(200)`–`nvarchar(255)`  
  - Reasoning: Group/category labels can be moderately long, but rarely require `max`. A bounded length improves storage and index efficiency.

- **`ipc`** (supplier catalog number)  
  - **Recommended**: `nvarchar(100)` (or based on known max length of supplier IDs)  
  - Reasoning: Catalog numbers are typically concise identifiers; bounding length makes indexing more efficient.

- **`dim`**  
  - **Recommended**: `nvarchar(400)` (or based on actual concatenated dimension design)  
  - Reasoning: This may be a concatenated key; give it enough room but keep it non‑MAX to retain indexability and good columnstore behavior.

- **`record_type`**  
  - **Recommended**: `nvarchar(10)`  
  - Reasoning: This column only ever contains one of two values (`'invoice'` or `'credit'`), so a very small bounded length is sufficient and more efficient than `nvarchar(max)`.

- **`_sync_checksum`**  
  - **Current**: `varchar(64)` – this is already appropriate for most hash/checksum schemes (e.g. SHA‑256 hex).

### 3. Date/time columns

- **`posted_date`**, **`end_of_month_bucket`**, **`_sync_updated_at`**  
  - **Current**: `datetime2` (unspecified scale)  
  - **Recommended**: keep `datetime2`, optionally specify scale, e.g. `datetime2(0)` or `datetime2(3)` depending on whether you care about seconds vs. milliseconds.  
  - Reasoning:
    - `datetime2` is the recommended modern type over `datetime` in SQL Server.
    - Explicitly choosing a scale can slightly reduce storage (`datetime2(0)` uses 6 bytes vs. 8 for full precision) and still be sufficient for reporting.

### 4. Keys and identifiers

- **`customer_code`**, **`seller_id`**  
  - **Current**: `bigint` – this is generally fine, especially if it matches upstream source keys.
  - Only change if you know the upstream system uses a smaller integer type and you want to align for strict consistency.

---

In summary, the most impactful optimizations for `reporting.financial_reporting_data_2026` are:

- Converting **monetary and numeric** columns from `float` to **`decimal`** with appropriate precision/scale.
- Replacing most `nvarchar(max)` usages with **bounded `nvarchar(n)`** lengths based on realistic data requirements.
- Right‑sizing helper columns like `year` and `cohort` to `smallint` / `decimal` where appropriate.

These changes will:

- Improve numeric accuracy for financial aggregates.
- Reduce storage and I/O.
- Enable more efficient indexing and columnstore usage for reporting workloads.

