# MSSQL to MSSQL Sync Utility - Usage Examples

This document provides comprehensive usage examples for the `mssql_to_mssql_sync.py` utility with both schema modes.

## Table of Contents

1. [Schema Modes Overview](#schema-modes-overview)
2. [Mode 1: Auto-Create Schema (Default)](#mode-1-auto-create-schema-default)
3. [Mode 2: Use Existing Table Schema (New Feature)](#mode-2-use-existing-table-schema-new-feature)
4. [Complete Asset Examples](#complete-asset-examples)

---

## Schema Modes Overview

The utility supports two schema handling modes:

### Mode 1: Auto-Create Schema (Default - `use_existing_table_schema=False`)
- **Behavior**: Infers schema from source query results
- **Table Creation**: Auto-creates table if it doesn't exist
- **Schema Types**: Uses generous/safe types (BIGINT, NVARCHAR(MAX), FLOAT)
- **Use Case**: Quick data syncs, prototyping, dynamic schemas
- **Backward Compatible**: Yes (this is the original behavior)

### Mode 2: Use Existing Table Schema (New - `use_existing_table_schema=True`)
- **Behavior**: Uses predefined table schema without modification
- **Table Requirement**: Destination table MUST exist
- **Validation**: Validates source data columns and types match destination
- **Use Case**: Production environments with strict schema control, compliance requirements
- **Backward Compatible**: Yes (opt-in via parameter)

---

## Mode 1: Auto-Create Schema (Default)

### Example 1.1: Basic Auto-Create with SQL Authentication

```python
import sys
import os
from dagster import asset, MetadataValue, AssetExecutionContext

sys.path.append('/app/data-manager')
from pyairbyte.utils.mssql_to_mssql_sync import sync_mssql_query_to_mssql

@asset(name="sync_sales_auto_create", group_name="sales_pipeline")
def sync_sales_auto_create(context: AssetExecutionContext):
    """Sync sales data using auto-create mode (default behavior)."""
    
    # Source: On-premises MSSQL with SQL Auth
    source_config = {
        "server": "source-server.local",
        "database": "production_db",
        "username": os.getenv("SOURCE_MSSQL_USER"),
        "password": os.getenv("SOURCE_MSSQL_PASSWORD")
    }
    
    # Destination: Azure SQL with SQL Auth
    dest_config = {
        "server": "dest-server.database.windows.net",
        "database": "analytics_db",
        "username": os.getenv("DEST_MSSQL_USER"),
        "password": os.getenv("DEST_MSSQL_PASSWORD")
    }
    
    query = """
    SELECT 
        OrderID,
        CustomerName,
        OrderDate,
        TotalAmount,
        Status
    FROM Sales.Orders
    WHERE OrderDate >= DATEADD(DAY, -30, GETDATE())
    """
    
    # DEFAULT MODE: use_existing_table_schema=False (can omit parameter)
    result = sync_mssql_query_to_mssql(
        source_config=source_config,
        source_query=query,
        dest_config=dest_config,
        dest_schema="dbo",
        dest_table="sales_orders",
        merge_key_columns=["OrderID"],
        use_streaming=True,
        chunk_size=1000,
        max_workers=4
        # use_existing_table_schema=False  # Default, not required
    )
    
    context.add_output_metadata({
        "status": MetadataValue.text(result['status']),
        "schema_mode": MetadataValue.text(result['schema_mode']),  # "auto_create"
        "rows_inserted": MetadataValue.int(result['result']['rows_inserted']),
        "rows_updated": MetadataValue.int(result['result']['rows_updated'])
    })
    
    return result
```

**What Happens:**
1. Table `dbo.sales_orders` will be auto-created if it doesn't exist
2. Schema inferred from query results with generous types:
   - `OrderID` → BIGINT
   - `CustomerName` → NVARCHAR(MAX)
   - `OrderDate` → DATETIME2
   - `TotalAmount` → FLOAT
   - `Status` → NVARCHAR(MAX)
3. `_sync_checksum` and `_sync_updated_at` columns added automatically

---

### Example 1.2: Auto-Create with Entra ID Authentication

```python
@asset(name="sync_inventory_auto_create", group_name="inventory_pipeline")
def sync_inventory_auto_create(context: AssetExecutionContext):
    """Sync inventory data using auto-create mode with Entra ID."""
    
    # Source: Azure SQL with Entra ID Service Principal
    source_config = {
        "server": "source.database.windows.net",
        "database": "production_db",
        "client_id": os.getenv("SOURCE_AZURE_CLIENT_ID"),
        "client_secret": os.getenv("SOURCE_AZURE_CLIENT_SECRET"),
        "tenant_id": os.getenv("SOURCE_AZURE_TENANT_ID"),
        "port": "1433"
    }
    
    # Destination: Azure SQL with Entra ID Service Principal
    dest_config = {
        "server": "dest.database.windows.net",
        "database": "analytics_db",
        "client_id": os.getenv("DEST_AZURE_CLIENT_ID"),
        "client_secret": os.getenv("DEST_AZURE_CLIENT_SECRET"),
        "tenant_id": os.getenv("DEST_AZURE_TENANT_ID"),
        "port": "1433"
    }
    
    query = """
    SELECT 
        ProductID,
        WarehouseID,
        Quantity,
        LastUpdated
    FROM Inventory.Stock
    """
    
    result = sync_mssql_query_to_mssql(
        source_config=source_config,
        source_query=query,
        dest_config=dest_config,
        dest_schema="staging",
        dest_table="inventory_stock",
        use_streaming=False  # Small dataset, load all at once
    )
    
    return result
```

---

## Mode 2: Use Existing Table Schema (New Feature)

### Example 2.1: Sync to Existing Table with Strict Schema

```python
@asset(name="sync_sales_existing_schema", group_name="sales_pipeline")
def sync_sales_existing_schema(context: AssetExecutionContext):
    """
    Sync sales data to existing table with predefined schema.
    
    Prerequisites:
    - Destination table dbo.sales_orders MUST exist
    - Table schema must match source query columns
    """
    
    # Source config
    source_config = {
        "server": "source-server.local",
        "database": "production_db",
        "username": os.getenv("SOURCE_MSSQL_USER"),
        "password": os.getenv("SOURCE_MSSQL_PASSWORD")
    }
    
    # Destination config
    dest_config = {
        "server": "dest-server.database.windows.net",
        "database": "analytics_db",
        "client_id": os.getenv("DEST_AZURE_CLIENT_ID"),
        "client_secret": os.getenv("DEST_AZURE_CLIENT_SECRET"),
        "tenant_id": os.getenv("DEST_AZURE_TENANT_ID")
    }
    
    query = """
    SELECT 
        OrderID,
        CustomerName,
        OrderDate,
        TotalAmount,
        Status
    FROM Sales.Orders
    WHERE OrderDate >= DATEADD(DAY, -30, GETDATE())
    """
    
    # NEW FEATURE: use_existing_table_schema=True
    result = sync_mssql_query_to_mssql(
        source_config=source_config,
        source_query=query,
        dest_config=dest_config,
        dest_schema="dbo",
        dest_table="sales_orders",
        merge_key_columns=["OrderID"],
        use_streaming=True,
        chunk_size=1000,
        max_workers=4,
        use_existing_table_schema=True  # NEW PARAMETER
    )
    
    context.add_output_metadata({
        "status": MetadataValue.text(result['status']),
        "schema_mode": MetadataValue.text(result['schema_mode']),  # "existing_table"
        "rows_inserted": MetadataValue.int(result['result']['rows_inserted']),
        "rows_updated": MetadataValue.int(result['result']['rows_updated'])
    })
    
    return result
```

**What Happens:**
1. Validates that `dbo.sales_orders` exists (fails if not)
2. Gets existing table schema from destination
3. Validates source query columns match destination columns
4. Validates data types are compatible
5. Fails fast with clear error if validation fails
6. Adds `_sync_checksum` column if missing (only modification allowed)

---

### Example 2.2: Predefined Table Creation Script

When using `use_existing_table_schema=True`, you must create the destination table first. Here's an example:

```sql
-- Create destination table with precise schema
CREATE TABLE [dbo].[sales_orders] (
    [OrderID] INT NOT NULL,
    [CustomerName] NVARCHAR(200) NOT NULL,
    [OrderDate] DATE NOT NULL,
    [TotalAmount] DECIMAL(18,2) NOT NULL,
    [Status] NVARCHAR(50) NOT NULL,
    [CreatedAt] DATETIME2 DEFAULT GETDATE()
);

-- The sync utility will automatically add these columns:
-- [_sync_checksum] VARCHAR(64) NULL
-- [_sync_updated_at] DATETIME2 DEFAULT GETDATE()

-- Create index on OrderID for merge performance
CREATE INDEX IX_sales_orders_OrderID ON [dbo].[sales_orders] ([OrderID]);
```

Then sync with validation:

```python
@asset(name="sync_to_predefined_table", group_name="sales_pipeline")
def sync_to_predefined_table(context: AssetExecutionContext):
    """Sync to table with strict schema control."""
    
    result = sync_mssql_query_to_mssql(
        source_config=source_config,
        source_query=query,
        dest_config=dest_config,
        dest_schema="dbo",
        dest_table="sales_orders",
        merge_key_columns=["OrderID"],
        use_existing_table_schema=True  # Validates against predefined schema
    )
    
    return result
```

---

### Example 2.3: Error Handling - Missing Required Column

**Scenario**: Source query is missing a required NOT NULL column from destination table.

```python
# Destination table has: OrderID, CustomerName, OrderDate, TotalAmount, Status
# Source query ONLY returns: OrderID, CustomerName, OrderDate

query = """
SELECT 
    OrderID,
    CustomerName,
    OrderDate
FROM Sales.Orders
"""

# This will FAIL with clear error message
try:
    result = sync_mssql_query_to_mssql(
        source_config=source_config,
        source_query=query,
        dest_config=dest_config,
        dest_schema="dbo",
        dest_table="sales_orders",
        use_existing_table_schema=True
    )
except ValueError as e:
    print(f"Validation failed: {e}")
    # Output:
    # Source data validation failed for [dbo].[sales_orders]:
    #   Critical: DataFrame missing required columns (NOT NULL, no data provided): TotalAmount, Status
```

---

### Example 2.4: Error Handling - Type Incompatibility

**Scenario**: Source query returns incompatible data types.

```python
# Destination table: OrderID INT
# Source query: OrderID VARCHAR (incompatible)

query = """
SELECT 
    CAST(OrderID AS VARCHAR(50)) AS OrderID,  -- Incompatible type
    CustomerName,
    OrderDate,
    TotalAmount,
    Status
FROM Sales.Orders
"""

# This will FAIL with clear error message
try:
    result = sync_mssql_query_to_mssql(
        source_config=source_config,
        source_query=query,
        dest_config=dest_config,
        dest_schema="dbo",
        dest_table="sales_orders",
        use_existing_table_schema=True
    )
except ValueError as e:
    print(f"Validation failed: {e}")
    # Output:
    # Source data validation failed for [dbo].[sales_orders]:
    #   Type incompatibilities detected:
    #   - Column 'OrderID': table expects INT, but data suggests NVARCHAR(MAX) (pandas dtype: object)
```

---

### Example 2.5: Successful Validation with Extra Columns

**Scenario**: Source query has extra columns not in destination (these are ignored, not an error).

```python
# Destination table: OrderID, CustomerName, OrderDate, TotalAmount, Status
# Source query: ALL above + ExtraColumn1, ExtraColumn2 (extra columns ignored)

query = """
SELECT 
    OrderID,
    CustomerName,
    OrderDate,
    TotalAmount,
    Status,
    'Extra Data 1' AS ExtraColumn1,  -- Will be ignored
    'Extra Data 2' AS ExtraColumn2   -- Will be ignored
FROM Sales.Orders
"""

result = sync_mssql_query_to_mssql(
    source_config=source_config,
    source_query=query,
    dest_config=dest_config,
    dest_schema="dbo",
    dest_table="sales_orders",
    use_existing_table_schema=True
)

# Logs will show warning:
# [Chunk 1] Source data has extra columns that will be ignored: ExtraColumn1, ExtraColumn2

# Result will be successful:
# {
#     'status': 'success',
#     'schema_mode': 'existing_table',
#     'result': {
#         'rows_inserted': 100,
#         'rows_updated': 50,
#         ...
#     }
# }
```

---

## Complete Asset Examples

### Complete Example: Sales Pipeline with Both Modes

```python
import sys
import os
from dagster import asset, MetadataValue, AssetExecutionContext, define_asset_job

sys.path.append('/app/data-manager')
from pyairbyte.utils.mssql_to_mssql_sync import sync_mssql_query_to_mssql

# ASSET 1: Sync to staging with auto-create (exploration/development)
@asset(name="sync_sales_staging", group_name="sales_pipeline")
def sync_sales_staging(context: AssetExecutionContext):
    """Sync sales data to staging table (auto-create mode for flexibility)."""
    
    source_config = {
        "server": "source.database.windows.net",
        "database": "production_db",
        "client_id": os.getenv("SOURCE_CLIENT_ID"),
        "client_secret": os.getenv("SOURCE_CLIENT_SECRET"),
        "tenant_id": os.getenv("TENANT_ID")
    }
    
    dest_config = {
        "server": "analytics.database.windows.net",
        "database": "analytics_db",
        "client_id": os.getenv("DEST_CLIENT_ID"),
        "client_secret": os.getenv("DEST_CLIENT_SECRET"),
        "tenant_id": os.getenv("TENANT_ID")
    }
    
    query = """
    SELECT 
        OrderID,
        CustomerName,
        OrderDate,
        TotalAmount,
        Status,
        Region,
        SalesRepID
    FROM Sales.Orders
    WHERE OrderDate >= DATEADD(DAY, -90, GETDATE())
    """
    
    # Auto-create mode for staging (flexible schema)
    result = sync_mssql_query_to_mssql(
        source_config=source_config,
        source_query=query,
        dest_config=dest_config,
        dest_schema="staging",
        dest_table="sales_orders_raw",
        use_streaming=True,
        chunk_size=1000,
        max_workers=4
        # use_existing_table_schema=False (default)
    )
    
    context.log.info(f"Staging sync completed: {result['result']['rows_inserted']} rows inserted")
    return result


# ASSET 2: Sync to production with existing schema (strict validation)
@asset(name="sync_sales_production", group_name="sales_pipeline", deps=["sync_sales_staging"])
def sync_sales_production(context: AssetExecutionContext):
    """
    Sync sales data to production table (existing schema mode for strict control).
    
    Production table must be pre-created with exact schema.
    """
    
    source_config = {
        "server": "analytics.database.windows.net",
        "database": "analytics_db",
        "client_id": os.getenv("DEST_CLIENT_ID"),
        "client_secret": os.getenv("DEST_CLIENT_SECRET"),
        "tenant_id": os.getenv("TENANT_ID")
    }
    
    dest_config = source_config  # Same database, different schema
    
    # Query from staging table with transformations
    query = """
    SELECT 
        OrderID,
        UPPER(TRIM(CustomerName)) AS CustomerName,
        CAST(OrderDate AS DATE) AS OrderDate,
        ROUND(TotalAmount, 2) AS TotalAmount,
        Status,
        Region
    FROM staging.sales_orders_raw
    WHERE Status IN ('Completed', 'Shipped')
    """
    
    # Existing schema mode for production (strict validation)
    result = sync_mssql_query_to_mssql(
        source_config=source_config,
        source_query=query,
        dest_config=dest_config,
        dest_schema="production",
        dest_table="sales_orders",
        merge_key_columns=["OrderID"],
        use_streaming=True,
        chunk_size=1000,
        max_workers=4,
        use_existing_table_schema=True  # Strict schema validation
    )
    
    if result['status'] == 'success':
        context.add_output_metadata({
            "status": MetadataValue.text("success"),
            "schema_mode": MetadataValue.text(result['schema_mode']),
            "rows_inserted": MetadataValue.int(result['result']['rows_inserted']),
            "rows_updated": MetadataValue.int(result['result']['rows_updated']),
            "processing_time": MetadataValue.float(result['result']['processing_time_seconds'])
        })
    else:
        context.log.error(f"Production sync failed: {result.get('error')}")
        raise Exception(f"Production sync failed: {result.get('error')}")
    
    return result


# Define job to run both assets in sequence
sales_pipeline_job = define_asset_job(
    name="sales_pipeline_job",
    selection=["sync_sales_staging", "sync_sales_production"]
)
```

---

## Decision Guide: Which Mode to Use?

### Use Auto-Create Mode (`use_existing_table_schema=False`) When:
✅ Prototyping or exploring new data sources  
✅ Schema changes frequently  
✅ Quick data syncs without strict schema requirements  
✅ Development/staging environments  
✅ Source schema is unknown or dynamic  

### Use Existing Table Mode (`use_existing_table_schema=True`) When:
✅ Production environments with strict schema control  
✅ Compliance requirements (audit trails, data types)  
✅ Schema is stable and well-defined  
✅ Need precise data types (not generic BIGINT/NVARCHAR(MAX))  
✅ Multiple pipelines write to same table (schema consistency)  
✅ Performance optimization (appropriate column sizes, indexes)  

---

## Summary

The new `use_existing_table_schema` parameter provides flexible schema handling:

| Feature | Auto-Create Mode (Default) | Existing Table Mode (New) |
|---------|---------------------------|---------------------------|
| **Table Creation** | Auto-creates if missing | Must exist |
| **Schema Source** | Inferred from query results | Existing table schema |
| **Data Types** | Generous/safe types | Precise/defined types |
| **Validation** | Basic (compatible types) | Strict (exact match) |
| **Use Case** | Development, exploration | Production, compliance |
| **Backward Compatible** | Yes (original behavior) | Yes (opt-in) |
| **SOLID Principle** | N/A | Open-Closed (extension) |

Both modes support:
- Entra ID and SQL Authentication
- Streaming/chunked processing
- Concurrent workers
- Checksum-based deduplication
- MERGE/UPSERT operations
- Record-level error handling
