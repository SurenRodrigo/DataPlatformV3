# Data Platform Service

**Core Data Orchestration Engine for the 99x Data Platform**

Version: 1.0.0  
Last Updated: January 2025

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Core Components](#core-components)
4. [Key Features](#key-features)
5. [Data Flow Patterns](#data-flow-patterns)
6. [Utility Modules](#utility-modules)
7. [Creating a Data Pipeline](#creating-a-data-pipeline)
8. [DBT Integration](#dbt-integration)
9. [Configuration Management](#configuration-management)
10. [Best Practices](#best-practices)
11. [Troubleshooting](#troubleshooting)

---

## Overview

The **Data Platform Service** is the central data orchestration engine of the 99x Data Platform. It manages the complete lifecycle of data pipelines from extraction through transformation to analytics-ready datasets. Built on Dagster, it provides a robust, scalable, and maintainable framework for enterprise data operations.

### What It Does

- **Data Extraction**: Connects to external APIs, databases, SharePoint, Excel files, and other data sources
- **Data Loading**: Streams data to PostgreSQL cache databases with intelligent chunking and error handling
- **Data Transformation**: Orchestrates DBT models for staging, intermediate, and mart-level transformations
- **Pipeline Orchestration**: Manages dependencies, parallel execution, and error recovery
- **Event Processing**: Handles event-driven workflows with deduplication and retry logic
- **Cache Management**: Maintains multiple isolated cache databases for different data domains

### Technology Stack

- **Orchestration**: Dagster (v1.10.21+) with gRPC-based architecture
- **Data Extraction**: PyAirbyte (v0.28.0+) for API connectors
- **Data Transformation**: DBT Core (v1.9.0+) for SQL transformations
- **Storage**: PostgreSQL 15+ with multi-schema support
- **Language**: Python 3.11+ with type hints and async support
- **Integration**: Microsoft Graph API, REST APIs, MSSQL, MySQL connectors

---

## Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Data Platform Service                     │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │ Data Manager│  │Dagster Code  │  │ DBT Models   │       │
│  │             │  │  Locations   │  │              │       │
│  │ • Config    │  │              │  │ • Staging    │       │
│  │ • Utilities │  │ • Assets     │  │ • Marts      │       │
│  │ • Connectors│  │ • Jobs       │  │ • Reports    │       │
│  │ • Scripts   │  │ • Schedules  │  │ • Snapshots  │       │
│  └─────────────┘  └──────────────┘  └──────────────┘       │
│         │                 │                  │               │
│         └────────┬────────┴─────────┬────────┘              │
│                  │                  │                        │
│         ┌────────▼────────┐  ┌─────▼──────┐                │
│         │  gRPC Servers   │  │ PostgreSQL │                │
│         │  (Port 4273)    │  │   Caches   │                │
│         └─────────────────┘  └────────────┘                │
└─────────────────────────────────────────────────────────────┘
                           │
              ┌────────────┴────────────┐
              │                         │
         ┌────▼─────┐            ┌─────▼──────┐
         │ Platform │            │  External  │
         │ Dagster  │            │  Systems   │
         │ Webserver│            │  (APIs, DB)│
         └──────────┘            └────────────┘
```

### Key Architectural Principles

1. **Zero-Coupling**: Business logic resides entirely in the app layer, platform only provides infrastructure
2. **gRPC Communication**: Each code location runs as an independent gRPC server
3. **Configuration-Driven**: All code locations defined in JSON configuration files
4. **Assets vs Utils Pattern**: Clear separation between business logic (assets) and technical implementation (utils)
5. **Multi-Cache Support**: Isolated cache databases for different data domains
6. **Incremental Processing**: Streaming and chunking for large datasets

---

## Core Components

### 1. Data Manager (`data-manager/`)

**Purpose**: Provides core infrastructure, utilities, and configuration for all data operations.

**Structure**:
```
data-manager/
├── dagster.yaml                    # Dagster instance configuration
├── entrypoint.sh                   # Service startup script
├── requirements.txt                # Python dependencies
├── external-connectors/            # PyAirbyte connector definitions (YAML)
│   ├── example-connector.yaml
│   └── ...
├── pyairbyte/
│   └── utils/                      # Reusable utility modules
│       ├── pyairbyte_sync.py      # Connector syncing
│       ├── common_cache.py        # Cache management
│       ├── excel_to_db_writer.py  # Excel processing
│       ├── sharepoint_client.py   # SharePoint integration
│       ├── sql_writer.py          # Database writing
│       ├── event_store.py         # Event processing
│       ├── graphql_util.py        # GraphQL operations
│       └── ...
├── resources/
│   └── dagster/
│       └── code-locations.json    # Code location registry
└── scripts/
    └── dagster-init.sh            # Multi-gRPC server initialization
```

**Key Responsibilities**:
- Initialize and manage gRPC servers for each code location
- Provide reusable utility modules for common data operations
- Manage external connector configurations
- Handle database connections and caching strategies

### 2. Dagster Code Locations (`dagster_code/`)

**Purpose**: Contains business logic organized as independent, isolated code locations.

**Standard Structure** (per code location):
```
dagster_code/
└── your_code_location/
    ├── __init__.py               # Definitions (assets, jobs, schedules, resources)
    ├── assets/
    │   ├── __init__.py          # Export all assets
    │   ├── cleanup_assets.py    # Cache cleanup operations
    │   ├── sync_assets.py       # Data extraction/loading
    │   └── dbt_assets.py        # DBT orchestration
    └── jobs/
        ├── __init__.py          # Export all jobs
        └── pipeline_jobs.py     # Job definitions
```

**Key Characteristics**:
- **Independence**: Each code location runs in isolation on its own gRPC port
- **Hot Reloading**: Changes are detected and reloaded without service restart
- **Modularity**: Assets can be composed and reused across different jobs
- **Testability**: Each asset can be tested independently

### 3. DBT Models (`dbt_models/` & `dbt_models_se/`)

**Purpose**: SQL-based data transformation layer with version control and testing.

**Standard Structure**:
```
dbt_models/
├── dbt_project.yml              # Project configuration
├── profiles.yml                 # Database connection profiles
├── models/
│   ├── staging/                 # Raw data normalization
│   │   └── stg__source_table.sql
│   ├── intermediate/            # Business logic transformations
│   │   └── int__business_entity.sql
│   ├── marts/                   # Analytics-ready fact/dimension tables
│   │   ├── dim__dimension.sql
│   │   └── fct__fact.sql
│   └── reports/                 # Specialized reporting views
│       └── rpt__report.sql
├── snapshots/                   # SCD Type 2 tracking
│   └── snapshot__table.sql
├── seeds/                       # Reference data (CSV files)
│   └── reference_data.csv
└── macros/                      # Reusable SQL functions
    └── custom_macro.sql
```

**Materialization Strategies**:
- **Staging**: `table` → Fast incremental loads
- **Intermediate**: `table` → Business logic processing
- **Marts**: `table` → Optimized for queries
- **Reports**: `table` → User-facing aggregations
- **Snapshots**: SCD Type 2 → Historical tracking

---

## Key Features

### 1. Multi-Cache Architecture

Support for cache databases to store synced data:

```python
from pyairbyte.utils.common_cache import get_cache

# Default cache for general-purpose data
default_cache = get_cache("default", "my-connector")
# Schema: pyairbyte_cache
# Prefix: my_connector_
```

**Benefits**:
- Centralized data storage in PostgreSQL cache schema
- Configurable table prefixes per connector
- Performance optimization with proper indexing

### 2. Configuration-Driven Code Locations

All code locations are defined in `resources/dagster/code-locations.json`:

```json
{
  "code_locations": [
    {
      "name": "bridgestone_data_sync",
      "enabled": true,
      "description": "Bridgestone data sync code location for Bridgestone data pipeline",
      "module": "dagster_code.bridgestone_data_sync",
      "port": 4273,
      "host": "data-platform-service",
      "metadata": {
        "team": "data",
        "domain": "bridgestone_data_sync",
        "version": "1.0.0"
      }
    }
  ],
  "config": {
    "workspace_name": "appbase-dagster-workspace",
    "description": "AppBase Data Platform Dagster Workspace (bridgestone_data_sync only)",
    "version": "1.0.0"
  }
}
```

**Features**:
- Enable/disable code locations without code changes
- Independent versioning per code location
- Metadata for documentation and discovery
- Hot-reload on configuration changes

### 3. Assets vs Utils Pattern

Clear separation between business logic and technical implementation:

**Assets (Business Logic)** - WHAT to do:
```python
import sys
from dagster import asset, AssetExecutionContext

sys.path.append('/app/data-manager')
from pyairbyte.utils.pyairbyte_sync import sync_connector
from pyairbyte.utils.common_cache import get_cache

@asset(
    name="sync_customer_data",
    group_name="customer_pipeline",
    deps=["cleanup_cache"]
)
def sync_customer_data(context: AssetExecutionContext):
    """
    Business logic: Sync customer data from API to cache.
    """
    # Business decision: Which cache to use
    cache = get_cache("default", "customer-api")
    
    # Business decision: Which connector and streams
    result = sync_connector("customer-api", ["customers", "orders"], cache=cache)
    
    # Dagster-specific: Metadata and logging
    context.add_output_metadata({
        "status": "success",
        "records_synced": result.get('records', 0)
    })
    
    return result
```

**Utils (Technical Implementation)** - HOW to do it:
```python
# pyairbyte/utils/pyairbyte_sync.py
def sync_connector(connector_name: str, streams: list, cache) -> dict:
    """
    Technical implementation: Load connector, validate, sync data.
    """
    config = get_connector_by_name(connector_name)
    source = ab.get_source(config['name'], config=config['config'])
    source.check()
    
    if streams:
        source.select_streams(streams)
    
    result = source.read(cache=cache)
    return {
        'status': 'success',
        'records': result.count(),
        'cache_schema': cache.schema_name
    }
```

**Benefits**:
- Reusability: Utils can be used across multiple assets
- Testability: Utils tested independently, assets tested with mocks
- Maintainability: Technical changes don't affect business logic
- Consistency: Standardized implementations across pipelines

### 4. Large File Processing

Streaming chunk processing for Excel files with 100K+ rows:

```python
from pyairbyte.utils.excel_to_db_writer import ExcelToDbWriter

# Initialize writer with field mapping
writer = ExcelToDbWriter(
    dbms_type="postgresql",
    connection_config={
        "host": "db",
        "port": 5432,
        "database": "dataplatform",
        "username": "user",
        "password": "pass",
        "schema": "public"
    },
    field_mapping={
        "Excel_Column_Name": "table_column_name",
        "Customer Name": "customer_name",
        # ... more mappings
    }
)

# Stream and write in chunks
result = writer.write_excel_to_table(
    excel_path="/path/to/large_file.xlsx",
    sheet_name="Sheet1",
    schema_name="public",
    table_name="customers",
    chunk_size=10000,  # Process 10K rows per chunk
    if_exists="append"
)
```

**Features**:
- Memory-efficient streaming (doesn't load entire file)
- Automatic type conversion based on table schema
- Field mapping with validation
- Partial failure handling with retry logic
- Progress tracking and detailed error reporting

### 5. SharePoint Integration

Microsoft Graph API integration for file downloads:

```python
from pyairbyte.utils.sharepoint_client import SharePointGraphClient
from pyairbyte.utils.excel_reader import ExcelReader

# Initialize Graph client (application permissions)
client = SharePointGraphClient(
    tenant_id=os.getenv("TENANT_ID"),
    client_id=os.getenv("CLIENT_ID"),
    client_secret=os.getenv("CLIENT_SECRET")
)

# Download file from SharePoint
file_content = client.download_file_bytes(
    hostname="company.sharepoint.com",
    site_path="DataSources",
    drive_name="Documents",
    item_path="reports/monthly_data.xlsx"
)

# Read Excel content
reader = ExcelReader()
sheets = reader.read_all_sheets(file_content, sheets=["Sales", "Inventory"])
```

**Features**:
- Microsoft Graph API with MSAL authentication
- Support for application permissions (service accounts)
- Automatic token refresh and retry logic
- Multi-sheet Excel reading
- Compatible with BytesIO, file paths, and file-like objects

### 6. Event-Driven Processing

Event store with deduplication and retry logic:

```python
from pyairbyte.utils.event_store import bulk_write_events, get_unprocessed_or_failed_events
from pyairbyte.utils.api_call import call_api_for_event_processing

# Write events with automatic deduplication
events = [
    {
        "event_type": "CUSTOMER_SYNC",
        "event_data": {"customer_id": 123, "action": "update"}
    },
    # ... more events
]
result = bulk_write_events(events)

# Process unprocessed events
unprocessed = get_unprocessed_or_failed_events("CUSTOMER_SYNC")
for event in unprocessed:
    # Make API call with automatic event logging
    call_api_for_event_processing(
        event_id=event['id'],
        method="POST",
        url="https://api.example.com/customers",
        body=event['event_data']
    )
```

**Features**:
- Hash-based deduplication (prevents duplicate processing)
- Status tracking (pending, processing, success, failed)
- Automatic retry for failed events
- Audit trail with timestamps and error messages
- GraphQL integration for event queries

### 7. DBT Orchestration

Seamless DBT integration with Dagster assets:

```python
from dagster import asset, AssetExecutionContext
from dagster_dbt import DbtCliResource

@asset(
    name="dbt_setup",
    group_name="transformations",
    deps=["sync_data"]
)
def dbt_setup(context: AssetExecutionContext, dbt: DbtCliResource):
    """Install dependencies and parse project."""
    context.log.info("Installing DBT dependencies...")
    dbt.cli(["deps"]).wait()
    
    context.log.info("Parsing DBT project...")
    parse_result = dbt.cli(["parse"]).wait()
    
    if not parse_result.is_successful():
        raise Exception("DBT parse failed")
    
    return {"status": "setup_complete"}

@asset(
    name="dbt_snapshots",
    group_name="transformations",
    deps=["dbt_setup"]
)
def dbt_snapshots(context: AssetExecutionContext, dbt: DbtCliResource):
    """Run DBT snapshots for SCD Type 2 tracking."""
    context.log.info("Running DBT snapshots...")
    
    snapshots_result = dbt.cli(["snapshot", "--threads", "6"]).wait()
    
    if not snapshots_result.is_successful():
        raise Exception("DBT snapshots failed")
    
    return {"status": "snapshots_complete"}

@asset(
    name="dbt_run",
    group_name="transformations",
    deps=["dbt_snapshots"]
)
def dbt_run(context: AssetExecutionContext, dbt: DbtCliResource):
    """Execute DBT transformations."""
    context.log.info("Running DBT models...")
    
    run_result = dbt.cli(["run", "--threads", "8"]).wait()
    
    if not run_result.is_successful():
        raise Exception("DBT run failed")
    
    return {"status": "transformations_complete"}
```

**Features**:
- Native Dagster-DBT integration
- Dependency management between assets
- Parallel execution with thread control
- Model selection and targeting
- Automatic lineage tracking
- Test integration with `dbt test`

### 8. PII Anonymization and Mapping

For sources that contain PII (e.g., customer names, emails, phone numbers), the platform supports deterministic anonymization before data is written to destination tables.

**Core concepts**:

- **Mapping table** (in the Dagster database, `public.pii_field_mappings`):
  - Stores stable mappings from one-way hashes to replacement values:
    - `source_system`, `field_name`, `context`, `hash_algorithm`, `hash_value`, `replacement_value`
  - Uniqueness is enforced on `(source_system, field_name, context, hash_algorithm, hash_value)`.
- **Hashing**:
  - Uses a one-way hash (default: `SHA256`) over a normalized form of the PII value.
  - The same input value always yields the same `hash_value`.
- **Replacement values**:
  - Generated deterministically from the hash (e.g. `CUST_<hash_prefix>`), truncated to a configurable `max_length`.
  - Stored and reused from the mapping table so all pipelines see consistent replacements.

**Utility: `pii_anonymizer.py`**:

```python
from pyairbyte.utils.pii_anonymizer import anonymize_dataframe, get_or_create_replacement_for_value

# Anonymize a DataFrame column using a simple config
pii_config = {
    "email": {
        "hash_algorithm": "SHA256",
        "replacement_prefix": "EMAIL_",
        "max_length": 64,
    },
}

df_anonymized = anonymize_dataframe(
    df,
    pii_config=pii_config,
    source_system="bridgestone_data_sync",
    context="pyairbyte_cache.source_customer_info",
)
```

**Integration with Excel and MSSQL sync**:

- `ExcelToDbWriter`:
  - New optional parameters:
    - `pii_config: Optional[Dict[str, Any]]`
    - `pii_source_system: Optional[str]`
    - `pii_context: Optional[str]`
  - When `pii_config` is provided, anonymization runs after field mapping and before type conversion and write, per chunk.
- `sync_mssql_query_to_mssql`:
  - New optional parameters:
    - `pii_config: Optional[Dict[str, Any]]`
    - `pii_source_system: Optional[str]`
    - `pii_context: Optional[str]`
  - When `pii_config` is provided, anonymization runs after `field_mapping` is applied and before checksum generation/MERGE.

**PII config pattern in assets**:

Define PII configuration alongside field mappings, using **destination column names** (post mapping). All per-field attributes are **optional**; you can pass only the field names:

- **List of field names** (defaults for all: SHA256, prefix from field name, max_length=64):

```python
pii_config = ["fp_name", "email", "phone_1"]
```

- **Dict with `True`** for defaults, or optional overrides only when needed:

```python
field_mapping = {
    "E-post": "email",
    "Telefon 1": "phone_1",
}

# Minimal: just mark PII columns (defaults used)
pii_config = {"fp_name": True, "email": True, "phone_1": True}

# Optional: override only the properties you need (rest use defaults)
pii_config = {
    "fp_name": True,
    "email": {"replacement_prefix": "EMAIL_"},
    "phone_1": {"replacement_prefix": "PHONE_", "max_length": 32},
}

# Realistic replacements: use field_type so values look like real names, addresses, emails
# (generated with Faker; unique and deterministic per PII value)
pii_config = {
    "fp_name": {"field_type": "name"},
    "email": {"field_type": "email"},
    "phone_1": {"field_type": "phone"},
    "invoice_address_street": {"field_type": "address"},
    "delivery_address_city": {"field_type": "city"},
}
# Supported field_type: name, address, street, city, postal_code, email, phone, company, text/sentence
```

Pass the config into the relevant utilities:

```python
writer = ExcelToDbWriter(
    dbms_type="mssql",
    connection_config=connection_config,
    field_mapping=field_mapping,
    pii_config=pii_config,
    pii_source_system="bridgestone_data_sync",
    pii_context="pyairbyte_cache.source_customer_info",
)
```

or

```python
result = sync_mssql_query_to_mssql(
    source_config=source_config,
    source_query=query,
    dest_config=dest_config,
    dest_schema="pyairbyte_cache",
    dest_table="source_customer_info",
    field_mapping=field_mapping,
    pii_config=pii_config,
    pii_source_system="bridgestone_data_sync",
    pii_context="pyairbyte_cache.source_customer_info",
)
```

This ensures PII never lands in destination tables in clear text, while preserving deterministic, joinable surrogate values for downstream analytics.

---

## Data Flow Patterns

### Pattern 1: API to Cache to Marts

**Use Case**: Extract data from REST APIs, load to cache, transform with DBT

```
┌──────────┐     ┌──────────────┐     ┌─────────┐     ┌──────────┐
│ External │────▶│   PyAirbyte  │────▶│  Cache  │────▶│   DBT    │
│   API    │     │  Connector   │     │ (Raw)   │     │  Models  │
└──────────┘     └──────────────┘     └─────────┘     └──────────┘
                                                              │
                                                              ▼
                                                       ┌─────────────┐
                                                       │   Marts     │
                                                       │ (Analytics) │
                                                       └─────────────┘
```

**Implementation**:
```python
# Asset 1: Cleanup
@asset(name="cleanup_cache")
def cleanup_cache(context):
    db_manager = PyAirbyteCacheDBManager.from_cache_name("default")
    db_manager.drop_cache_table('_airbyte_state')
    return {"status": "cleanup_complete"}

# Asset 2: Extract & Load
@asset(name="sync_api_data", deps=["cleanup_cache"])
def sync_api_data(context):
    cache = get_cache("default", "api-connector")
    result = sync_connector("api-connector", cache=cache)
    return result

# Asset 3: Transform
@asset(name="transform_data", deps=["sync_api_data"])
def transform_data(context, dbt: DbtCliResource):
    dbt.cli(["run", "--select", "staging_api_data marts_api_data"]).wait()
    return {"status": "transform_complete"}

# Job: Orchestrate pipeline
api_to_marts_job = define_asset_job(
    name="api_to_marts_pipeline",
    selection=["cleanup_cache", "sync_api_data", "transform_data"]
)
```

### Pattern 2: Excel/SharePoint to Database

**Use Case**: Extract Excel files from SharePoint, process and load to database

```
┌────────────┐     ┌───────────┐     ┌──────────────┐     ┌──────────┐
│ SharePoint │────▶│   Graph   │────▶│    Excel     │────▶│   SQL    │
│   (Files)  │     │  Download │     │   Reader     │     │  Writer  │
└────────────┘     └───────────┘     └──────────────┘     └──────────┘
                                                                 │
                                                                 ▼
                                                          ┌─────────────┐
                                                          │  Database   │
                                                          │   Tables    │
                                                          └─────────────┘
```

**Implementation**:
```python
@asset(name="sync_excel_from_sharepoint")
def sync_excel_from_sharepoint(context):
    # Download from SharePoint
    client = SharePointGraphClient(tenant_id, client_id, client_secret)
    file_content = client.download_file_bytes(
        hostname="company.sharepoint.com",
        site_path="Reports",
        drive_name="Documents",
        item_path="data/monthly_report.xlsx"
    )
    
    # Read Excel
    reader = ExcelReader()
    sheets = reader.read_all_sheets(file_content, sheets=["Sales"])
    
    # Write to database
    cache = get_cache("default", "excel-connector")
    sql_writer = SqlWriter("excel-connector", cache=cache)
    
    for sheet_name, df in sheets.items():
        sql_writer.write_df_to_table(df, sheet_name.lower(), if_exists='replace')
    
    return {"status": "success", "sheets_processed": len(sheets)}
```

### Pattern 3: Database to Database (MSSQL/MySQL to PostgreSQL)

**Use Case**: Sync tables from MSSQL or MySQL to PostgreSQL cache

```
┌──────────┐     ┌─────────────┐     ┌──────────────┐
│  MSSQL   │────▶│  Connector  │────▶│  PostgreSQL  │
│  /MySQL  │     │   (Sync)    │     │    Cache     │
└──────────┘     └─────────────┘     └──────────────┘
```

**Implementation**:
```python
from pyairbyte.utils.mssql_sync import sync_mssql_tables

@asset(name="sync_mssql_data")
def sync_mssql_data(context):
    result = sync_mssql_tables(
        server="mssql.example.com",
        database="production_db",
        tables=["customers", "orders", "products"],
        cache_schema="pyairbyte_cache"
    )
    
    context.log.info(f"Synced {result['tables_synced']} tables")
    return result
```

### Pattern 4: Event-Driven Processing

**Use Case**: Process events from GraphQL API, make API calls, track status

```
┌──────────┐     ┌────────────┐     ┌─────────────┐     ┌──────────┐
│ GraphQL  │────▶│   Event    │────▶│   Event     │────▶│ External │
│   Query  │     │ Generation │     │  Processing │     │   API    │
└──────────┘     └────────────┘     └─────────────┘     └──────────┘
                        │                    │
                        ▼                    ▼
                 ┌────────────┐       ┌────────────┐
                 │Event Store │       │Event Store │
                 │  (Create)  │       │  (Update)  │
                 └────────────┘       └────────────┘
```

**Implementation**:
```python
from pyairbyte.utils.graphql_util import query_graphql_api
from pyairbyte.utils.event_store import bulk_write_events, get_unprocessed_or_failed_events
from pyairbyte.utils.api_call import call_api_for_event_processing

# Asset 1: Generate events from GraphQL
@asset(name="generate_events")
def generate_events(context):
    query = """
    query {
      customers(where: {sync_required: {_eq: true}}) {
        id
        name
        email
      }
    }
    """
    result = query_graphql_api(query)
    customers = result.get('data', {}).get('customers', [])
    
    events = [
        {
            "event_type": "CUSTOMER_SYNC",
            "event_data": customer
        }
        for customer in customers
    ]
    
    bulk_result = bulk_write_events(events)
    context.log.info(f"Created {bulk_result['events_created']} events")
    return bulk_result

# Asset 2: Process events
@asset(name="process_events", deps=["generate_events"])
def process_events(context):
    events = get_unprocessed_or_failed_events("CUSTOMER_SYNC")
    
    processed_count = 0
    for event in events:
        call_api_for_event_processing(
            event_id=event['id'],
            method="POST",
            url="https://crm.example.com/api/customers",
            body=event['event_data']
        )
        processed_count += 1
    
    context.log.info(f"Processed {processed_count} events")
    return {"processed": processed_count}
```

---

## Utility Modules

### Available Utilities

| Utility Module | Purpose | Key Functions |
|----------------|---------|---------------|
| `pyairbyte_sync.py` | PyAirbyte connector syncing | `sync_connector()` |
| `common_cache.py` | Multi-cache management | `get_cache()`, `CACHE_CONFIGS` |
| `cache_db_manager.py` | Database/schema operations | `PyAirbyteCacheDBManager` class |
| `excel_to_db_writer.py` | Excel to database streaming | `ExcelToDbWriter` class |
| `sharepoint_client.py` | SharePoint/Graph integration | `SharePointGraphClient` class |
| `excel_reader.py` | Excel file reading | `ExcelReader` class |
| `sql_writer.py` | DataFrame to PostgreSQL | `SqlWriter` class |
| `event_store.py` | Event processing | `bulk_write_events()`, `get_unprocessed_or_failed_events()` |
| `graphql_util.py` | Hasura GraphQL operations | `query_graphql_api()` |
| `api_call.py` | HTTP API calls with logging | `call_api_for_event_processing()` |
| `mssql_sync.py` | MSSQL to PostgreSQL sync | `sync_mssql_tables()` |
| `mssql_to_mssql_sync.py` | MSSQL to MSSQL query sync | `sync_mssql_query_to_mssql()` |
| `mysql_sync.py` | MySQL to PostgreSQL sync | `sync_mysql_tables()` |
| `connector_loader.py` | Connector config loading | `get_connector_by_name()` |
| `pii_anonymizer.py` | PII anonymization and mapping | `anonymize_dataframe()`, `get_or_create_replacement_for_value()` |

### Using Utilities in Assets

**Pattern**: Import from `data-manager` path, use in business logic

```python
import sys
from dagster import asset, AssetExecutionContext

# Add data-manager to Python path
sys.path.append('/app/data-manager')

# Import utilities
from pyairbyte.utils.pyairbyte_sync import sync_connector
from pyairbyte.utils.common_cache import get_cache
from pyairbyte.utils.cache_db_manager import PyAirbyteCacheDBManager

@asset(name="my_asset")
def my_asset(context: AssetExecutionContext):
    """Business logic using utilities."""
    
    # Use cache manager
    db_manager = PyAirbyteCacheDBManager.from_cache_name("default")
    db_manager.drop_cache_table('_airbyte_state')
    
    # Use cache configuration
    cache = get_cache("default", "my-connector")
    
    # Use sync utility
    result = sync_connector("my-connector", cache=cache)
    
    return result
```

---

## Creating a Data Pipeline

### Step-by-Step Example: Creating a New Code Location

This example demonstrates creating a complete data pipeline from scratch.

#### Step 1: Create Directory Structure

```bash
# Create code location directory
mkdir -p app/data-platform-service/dagster_code/sales_pipeline

# Create subdirectories
mkdir -p app/data-platform-service/dagster_code/sales_pipeline/assets
mkdir -p app/data-platform-service/dagster_code/sales_pipeline/jobs

# Create __init__.py files
touch app/data-platform-service/dagster_code/sales_pipeline/__init__.py
touch app/data-platform-service/dagster_code/sales_pipeline/assets/__init__.py
touch app/data-platform-service/dagster_code/sales_pipeline/jobs/__init__.py
```

#### Step 2: Create Connector Configuration

Create `app/data-platform-service/data-manager/external-connectors/sales-api.yaml`:

```yaml
version: 6.60.0
type: DeclarativeSource

definitions:
  linked:
    HttpRequester:
      request_headers:
        Accept: application/json
        Content-Type: application/json
        Authorization: "Bearer {{ config['api_token'] }}"

streams:
  - type: DeclarativeStream
    name: sales_orders
    retriever:
      type: SimpleRetriever
      requester:
        type: HttpRequester
        url: https://api.example.com/v1/orders
        http_method: GET
        request_headers:
          $ref: "#/definitions/linked/HttpRequester/request_headers"
      record_selector:
        type: RecordSelector
        extractor:
          type: DpathExtractor
          field_path:
            - data
    schema_loader:
      type: InlineSchemaLoader
      schema:
        type: object
        properties:
          order_id:
            type: number
          customer_name:
            type: string
          order_date:
            type: string
          total_amount:
            type: number

spec:
  type: Spec
  connection_specification:
    type: object
    required:
      - api_token
    properties:
      api_token:
        type: string
        title: API Token
        airbyte_secret: true
```

#### Step 3: Create Cleanup Asset

Create `app/data-platform-service/dagster_code/sales_pipeline/assets/cleanup_assets.py`:

```python
import sys
from dagster import asset, MetadataValue, AssetExecutionContext

sys.path.append('/app/data-manager')
from pyairbyte.utils.cache_db_manager import PyAirbyteCacheDBManager
from pyairbyte.utils.common_cache import CACHE_CONFIGS

@asset(
    name="cleanup_sales_cache",
    group_name="sales_pipeline"
)
def cleanup_sales_cache(context: AssetExecutionContext):
    """Clean up PyAirbyte cache tables before sync."""
    
    context.log.info("Starting cache cleanup...")
    
    # Get cache configuration
    cache_config = CACHE_CONFIGS["default"]
    db_manager = PyAirbyteCacheDBManager(cache_config=cache_config)
    
    # Tables to drop
    tables_to_drop = [
        '_airbyte_destination_state',
        '_airbyte_state',
        '_airbyte_streams',
        'sync_metadata'
    ]
    
    dropped_tables = []
    for table in tables_to_drop:
        if db_manager.drop_cache_table(table):
            dropped_tables.append(table)
            context.log.info(f"✓ Dropped {table}")
    
    context.add_output_metadata({
        "status": MetadataValue.text("success"),
        "tables_dropped": MetadataValue.int(len(dropped_tables))
    })
    
    return {"status": "success", "dropped_tables": dropped_tables}
```

#### Step 4: Create Sync Asset

Create `app/data-platform-service/dagster_code/sales_pipeline/assets/sync_assets.py`:

```python
import sys
from dagster import asset, MetadataValue, AssetExecutionContext

sys.path.append('/app/data-manager')
from pyairbyte.utils.pyairbyte_sync import sync_connector
from pyairbyte.utils.common_cache import get_cache

@asset(
    name="sync_sales_data",
    group_name="sales_pipeline",
    deps=["cleanup_sales_cache"]
)
def sync_sales_data(context: AssetExecutionContext):
    """Sync sales data from API to PostgreSQL cache."""
    
    context.log.info("Starting sales data sync...")
    
    # Get cache with connector-specific prefix
    cache = get_cache("default", "sales-api")
    context.log.info(f"Using cache schema: {cache.schema_name}")
    
    # Sync data
    result = sync_connector(
        "sales-api",
        streams_to_sync=["sales_orders"],
        cache=cache
    )
    
    if result.get('status') == 'success':
        context.add_output_metadata({
            "status": MetadataValue.text("success"),
            "cache_schema": MetadataValue.text(result.get('cache_schema')),
            "connector": MetadataValue.text("sales-api"),
            "details": MetadataValue.json(result.get('result', {}))
        })
        context.log.info("Sales data synced successfully")
    else:
        context.log.error(f"Sync failed: {result.get('error')}")
        raise Exception(f"Sales sync failed: {result.get('error')}")
    
    return result
```

#### Step 5: Create DBT Asset

Create `app/data-platform-service/dagster_code/sales_pipeline/assets/dbt_assets.py`:

```python
from dagster import asset, AssetExecutionContext
from dagster_dbt import DbtCliResource

@asset(
    name="transform_sales_data",
    group_name="sales_pipeline",
    deps=["sync_sales_data"]
)
def transform_sales_data(context: AssetExecutionContext, dbt: DbtCliResource):
    """Transform sales data using DBT models."""
    
    context.log.info("Starting DBT transformations...")
    
    # Parse project
    context.log.info("Parsing DBT project...")
    parse_result = dbt.cli(["parse"]).wait()
    if not parse_result.is_successful():
        raise Exception("DBT parse failed")
    
    # Run transformations
    context.log.info("Running DBT models: staging_sales, marts_sales")
    run_result = dbt.cli([
        "run",
        "--select",
        "staging_sales marts_sales"
    ]).wait()
    
    if run_result.is_successful():
        context.log.info("DBT transformations completed successfully")
        return {"status": "success"}
    else:
        raise Exception("DBT transformations failed")
```

#### Step 6: Export Assets

Update `app/data-platform-service/dagster_code/sales_pipeline/assets/__init__.py`:

```python
# Assets package for sales_pipeline code location
from .cleanup_assets import cleanup_sales_cache
from .sync_assets import sync_sales_data
from .dbt_assets import transform_sales_data

__all__ = [
    "cleanup_sales_cache",
    "sync_sales_data",
    "transform_sales_data"
]
```

#### Step 7: Create Job

Create `app/data-platform-service/dagster_code/sales_pipeline/jobs/pipeline_jobs.py`:

```python
from dagster import define_asset_job
from ..assets import cleanup_sales_cache, sync_sales_data, transform_sales_data

# Define pipeline job
sales_pipeline_job = define_asset_job(
    name="sales_pipeline_job",
    selection=[
        "cleanup_sales_cache",   # Step 1: Cleanup
        "sync_sales_data",        # Step 2: Extract & Load
        "transform_sales_data"    # Step 3: Transform
    ],
    description="Complete sales data pipeline: cleanup, sync, transform"
)
```

Update `app/data-platform-service/dagster_code/sales_pipeline/jobs/__init__.py`:

```python
# Jobs package for sales_pipeline code location
from .pipeline_jobs import sales_pipeline_job

__all__ = ["sales_pipeline_job"]
```

#### Step 8: Create Main Definitions

Update `app/data-platform-service/dagster_code/sales_pipeline/__init__.py`:

```python
from dagster import Definitions
from dagster_dbt import DbtCliResource

# Import assets and jobs
from .assets import cleanup_sales_cache, sync_sales_data, transform_sales_data
from .jobs import sales_pipeline_job

# DBT configuration
DBT_PROJECT_DIR = "/app/dbt_models"
DBT_PROFILES_DIR = "/app/dbt_models"

# Define resources
resources = {
    "dbt": DbtCliResource(
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROFILES_DIR,
    )
}

# Create definitions
defs = Definitions(
    assets=[
        cleanup_sales_cache,
        sync_sales_data,
        transform_sales_data
    ],
    jobs=[sales_pipeline_job],
    resources=resources,
    schedules=[]
)
```

#### Step 9: Register Code Location

Update `app/data-platform-service/data-manager/resources/dagster/code-locations.json`:

```json
{
  "code_locations": [
    {
      "name": "sales_pipeline",
      "enabled": true,
      "description": "Sales data pipeline with API sync and DBT transformations",
      "module": "dagster_code.sales_pipeline",
      "port": 4274,
      "host": "data-platform-service",
      "metadata": {
        "team": "data-engineering",
        "domain": "sales",
        "version": "1.0.0"
      }
    }
  ]
}
```

#### Step 10: Create DBT Models

Create `app/data-platform-service/dbt_models/models/sales_sources.yml`:

```yaml
version: 2

sources:
  - name: sales_cache
    description: "Sales data from API via PyAirbyte"
    schema: pyairbyte_cache
    tables:
      - name: sales_api_sales_orders
        description: "Raw sales orders from API"
        columns:
          - name: order_id
            description: "Unique order identifier"
          - name: customer_name
            description: "Customer name"
          - name: order_date
            description: "Order date"
          - name: total_amount
            description: "Total order amount"
```

Create `app/data-platform-service/dbt_models/models/staging/staging_sales.sql`:

```sql
{{
  config(
    materialized='table',
    schema='staging'
  )
}}

SELECT
    order_id::INTEGER AS order_id,
    NULLIF(TRIM(customer_name), '') AS customer_name,
    order_date::TIMESTAMP AS order_date,
    total_amount::NUMERIC(10,2) AS total_amount,
    current_timestamp AS dbt_loaded_at
FROM {{ source('sales_cache', 'sales_api_sales_orders') }}
WHERE order_id IS NOT NULL
```

Create `app/data-platform-service/dbt_models/models/marts/marts_sales.sql`:

```sql
{{
  config(
    materialized='table',
    schema='marts'
  )
}}

SELECT
    order_id,
    customer_name,
    order_date,
    total_amount,
    DATE_TRUNC('month', order_date) AS order_month,
    DATE_TRUNC('year', order_date) AS order_year,
    dbt_loaded_at
FROM {{ ref('staging_sales') }}
```

#### Step 11: Rebuild and Test

```bash
# Rebuild data-platform-service
docker compose -f app/docker-compose.yaml up -d --build data-platform-service

# Verify code location loads
docker exec data-platform-service python3 -c \
  "from dagster_code.sales_pipeline import defs; print('✅ Code location loaded successfully')"

# Check Dagster UI
# Navigate to http://localhost:3030
# Verify "sales_pipeline" code location appears
# Run the "sales_pipeline_job" job
```

---

## DBT Integration

### DBT Project Structure

```
dbt_models/
├── dbt_project.yml          # Project configuration
├── profiles.yml             # Connection profiles
├── models/
│   ├── staging/            # Layer 1: Normalize raw data
│   ├── intermediate/       # Layer 2: Business logic
│   ├── marts/              # Layer 3: Analytics-ready
│   └── reports/            # Layer 4: User-facing
├── snapshots/              # SCD Type 2 tracking
├── seeds/                  # Reference data
└── macros/                 # Reusable SQL functions
```

### DBT Configuration Best Practices

**dbt_project.yml**:
```yaml
name: 'data_platform'
version: '1.0.0'
config-version: 2

profile: 'data_platform'

model-paths: ["models"]
snapshot-paths: ["snapshots"]
seed-paths: ["seeds"]
macro-paths: ["macros"]

models:
  data_platform:
    staging:
      +materialized: table
      +schema: staging
      
    intermediate:
      +materialized: table
      +schema: intermediate

    marts:
      +materialized: table
      +schema: marts

    reports:
      +materialized: table
      +schema: reporting

snapshots:
  data_platform:
    +target_schema: snapshots

seeds:
  +schema: seeds
```

**profiles.yml**:
```yaml
data_platform:
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('APPBASE_DB_HOST') }}"
      user: "{{ env_var('APPBASE_DB_USER') }}"
      password: "{{ env_var('APPBASE_DB_PASSWORD') }}"
      port: "{{ env_var('APPBASE_DB_PORT') | int }}"
      dbname: "{{ env_var('APPBASE_DB_NAME') }}"
      schema: public
      threads: 8
  target: dev
```

### DBT Asset Pattern

Standard pattern for DBT orchestration in Dagster:

```python
from dagster import asset, AssetExecutionContext
from dagster_dbt import DbtCliResource

@asset(name="dbt_deps", group_name="transformations")
def dbt_deps(context: AssetExecutionContext, dbt: DbtCliResource):
    """Install DBT dependencies."""
    deps_result = dbt.cli(["deps"]).wait()
    if not deps_result.is_successful():
        raise Exception("DBT deps failed")
    return {"status": "deps_complete"}

@asset(name="dbt_parse", group_name="transformations", deps=["dbt_deps"])
def dbt_parse(context: AssetExecutionContext, dbt: DbtCliResource):
    """Parse DBT project."""
    parse_result = dbt.cli(["parse"]).wait()
    if not parse_result.is_successful():
        raise Exception("DBT parse failed")
    return {"status": "parse_complete"}

@asset(name="dbt_snapshot", group_name="transformations", deps=["dbt_parse"])
def dbt_snapshot(context: AssetExecutionContext, dbt: DbtCliResource):
    """Run DBT snapshots."""
    snapshot_result = dbt.cli(["snapshot", "--threads", "6"]).wait()
    if not snapshot_result.is_successful():
        raise Exception("DBT snapshot failed")
    return {"status": "snapshot_complete"}

@asset(name="dbt_seed", group_name="transformations", deps=["dbt_snapshot"])
def dbt_seed(context: AssetExecutionContext, dbt: DbtCliResource):
    """Load DBT seed data."""
    seed_result = dbt.cli(["seed", "-f"]).wait()
    if not seed_result.is_successful():
        raise Exception("DBT seed failed")
    return {"status": "seed_complete"}

@asset(name="dbt_run", group_name="transformations", deps=["dbt_seed"])
def dbt_run(context: AssetExecutionContext, dbt: DbtCliResource):
    """Run DBT transformations."""
    run_result = dbt.cli(["run", "--threads", "8"]).wait()
    if not run_result.is_successful():
        raise Exception("DBT run failed")
    return {"status": "run_complete"}

@asset(name="dbt_test", group_name="transformations", deps=["dbt_run"])
def dbt_test(context: AssetExecutionContext, dbt: DbtCliResource):
    """Run DBT tests."""
    test_result = dbt.cli(["test"]).wait()
    if not test_result.is_successful():
        context.log.warning("Some DBT tests failed")
    return {"status": "test_complete"}

@asset(name="dbt_clean", group_name="transformations", deps=["dbt_test"])
def dbt_clean(context: AssetExecutionContext, dbt: DbtCliResource):
    """Clean DBT artifacts."""
    clean_result = dbt.cli(["clean"]).wait()
    if not clean_result.is_successful():
        raise Exception("DBT clean failed")
    return {"status": "clean_complete"}
```

---

## Configuration Management

### Code Location Configuration

**File**: `data-manager/resources/dagster/code-locations.json`

```json
{
  "code_locations": [
    {
      "name": "your_pipeline",
      "enabled": true,
      "description": "Your data pipeline description",
      "module": "dagster_code.your_pipeline",
      "port": 4274,
      "host": "data-platform-service",
      "metadata": {
        "team": "data",
        "domain": "your_domain",
        "version": "1.0.0",
        "maintainer": "data-team@example.com"
      }
    }
  ],
  "config": {
    "workspace_name": "appbase-dagster-workspace",
    "description": "AppBase Data Platform Dagster Workspace",
    "version": "1.0.0"
  }
}
```

**Fields**:
- `name`: Unique identifier for code location (used in Dagster UI)
- `enabled`: Boolean to enable/disable without deleting configuration
- `description`: Human-readable description
- `module`: Python module path (e.g., `dagster_code.bridgestone_data_sync`)
- `port`: gRPC server port (e.g., 4273)
- `host`: Service hostname (typically `data-platform-service`)
- `metadata`: Optional metadata for documentation and discovery

### Cache Configuration

**File**: `data-manager/pyairbyte/utils/common_cache.py`

```python
CACHE_CONFIGS = {
    'default': {
        'host': os.getenv('PYAIRBYTE_CACHE_DB_HOST', 'db'),
        'port': int(os.getenv('PYAIRBYTE_CACHE_DB_PORT', '5432')),
        'database': os.getenv('PYAIRBYTE_CACHE_DB_NAME', 'dataplatform'),
        'username': os.getenv('PYAIRBYTE_CACHE_DB_USER', 'dataplatuser'),
        'password': os.getenv('PYAIRBYTE_CACHE_DB_PASSWORD', 'dataplatpassword'),
        'schema_name': 'pyairbyte_cache',
        'table_prefix': 'default_',
        'cleanup': True
    }
}
```

**Adding New Cache**:
1. Add configuration to `CACHE_CONFIGS` dictionary
2. Set environment variables in `.env` file
3. Create database/schema if needed
4. Use `get_cache("cache_name", "connector_name")` in assets

### Environment Variables

**Platform Configuration** (`platform/.env`):
- `DATABASE_*` - PostgreSQL connection details
- `DAGSTER_*` - Dagster webserver configuration
- `HASURA_*` - Hasura GraphQL engine settings

**Application Configuration** (`app/.env`):
- `APPBASE_DB_*` - Application database connections
- `PYAIRBYTE_CACHE_*` - Cache database configurations
- `DAGSTER_HOME` - Dagster home directory
- Integration API credentials (optional, per connector)

---

## Best Practices

### 1. Asset Design

**DO**:
- Keep assets focused on single responsibility
- Use descriptive names that indicate business purpose
- Add comprehensive docstrings explaining the "why"
- Return structured dictionaries with status information
- Add metadata for monitoring and debugging
- Handle errors gracefully with proper logging

**DON'T**:
- Mix technical implementation in assets (use utils)
- Create circular dependencies between assets
- Ignore return values from utility functions
- Skip error handling and assume success
- Use hardcoded values instead of configuration

### 2. Dependency Management

**Explicit Dependencies**:
```python
@asset(
    name="transform_data",
    deps=["sync_data", "cleanup_cache"]  # Explicit dependencies
)
def transform_data(context):
    # This runs after sync_data AND cleanup_cache complete
    pass
```

**Implicit Dependencies** (via return values):
```python
@asset(name="sync_data")
def sync_data(context):
    return {"table_name": "customers"}

@asset(name="transform_data")
def transform_data(context, sync_data):  # Asset as parameter
    # Access sync_data return value
    table = sync_data["table_name"]
    pass
```

### 3. Error Handling

**Pattern**:
```python
@asset(name="robust_asset")
def robust_asset(context: AssetExecutionContext):
    """Asset with comprehensive error handling."""
    try:
        # Business logic
        result = perform_operation()
        
        if result.get('status') == 'success':
            # Success metadata
            context.add_output_metadata({
                "status": MetadataValue.text("success"),
                "records": MetadataValue.int(result['count'])
            })
            context.log.info("Operation completed successfully")
            return result
        else:
            # Failure metadata
            context.add_output_metadata({
                "status": MetadataValue.text("error"),
                "error": MetadataValue.text(result.get('error'))
            })
            context.log.error(f"Operation failed: {result.get('error')}")
            raise Exception(f"Operation failed: {result.get('error')}")
            
    except ValueError as e:
        context.log.error(f"Configuration error: {e}")
        raise
    except Exception as e:
        context.log.error(f"Unexpected error: {e}")
        raise
```

### 4. Code Organization

**File Structure**:
```
your_code_location/
├── __init__.py              # Only imports and Definitions
├── assets/
│   ├── __init__.py         # Only exports
│   ├── cleanup_assets.py   # Cleanup operations
│   ├── sync_assets.py      # Data extraction/loading
│   ├── transform_assets.py # DBT orchestration
│   └── event_assets.py     # Event processing (if applicable)
└── jobs/
    ├── __init__.py         # Only exports
    └── pipeline_jobs.py    # Job definitions
```

**Naming Conventions**:
- Assets: `verb_noun` (e.g., `sync_customer_data`, `transform_sales_data`)
- Jobs: `noun_pipeline_job` (e.g., `customer_pipeline_job`)
- Groups: `domain_name` (e.g., `customer_pipeline`, `sales_analytics`)

### 5. Testing

**Unit Testing Utils**:
```python
# tests/test_utils.py
from pyairbyte.utils.common_cache import get_cache

def test_get_cache():
    cache = get_cache("default", "test-connector")
    assert cache.schema_name == "pyairbyte_cache"
    assert "test_connector_" in cache.table_prefix
```

**Integration Testing Assets**:
```python
# tests/test_assets.py
from dagster import build_asset_context
from dagster_code.sales_pipeline.assets import sync_sales_data

def test_sync_sales_data():
    context = build_asset_context()
    result = sync_sales_data(context)
    assert result['status'] == 'success'
```

### 6. Performance Optimization

**Parallel Execution**:
```python
# Assets with no dependencies run in parallel
@asset(name="sync_customers")
def sync_customers(context):
    pass

@asset(name="sync_orders")
def sync_orders(context):
    pass

# Both run simultaneously when job starts
```

**Chunking Large Datasets**:
```python
writer = ExcelToDbWriter(...)
result = writer.write_excel_to_table(
    excel_path=path,
    sheet_name="Data",
    schema_name="public",
    table_name="large_table",
    chunk_size=10000  # Process 10K rows at a time
)
```

**DBT Threading**:
```python
# Use multiple threads for parallel model execution
dbt.cli(["run", "--threads", "8"]).wait()
```

### 7. Monitoring and Observability

**Add Rich Metadata**:
```python
context.add_output_metadata({
    "status": MetadataValue.text("success"),
    "records_processed": MetadataValue.int(10000),
    "processing_time_seconds": MetadataValue.float(45.2),
    "cache_schema": MetadataValue.text("pyairbyte_cache"),
    "connector": MetadataValue.text("sales-api"),
    "execution_timestamp": MetadataValue.text(datetime.now().isoformat()),
    "data_quality_score": MetadataValue.float(0.95)
})
```

**Structured Logging**:
```python
context.log.info(f"Starting sync: connector={connector_name}, streams={streams}")
context.log.debug(f"Cache config: {cache.schema_name}")
context.log.warning(f"Performance degradation detected: {elapsed_time}s")
context.log.error(f"Sync failed: {error_message}")
```

---

## Troubleshooting

### Common Issues

#### 1. Code Location Not Loading

**Symptoms**:
- Code location doesn't appear in Dagster UI
- gRPC server health check fails

**Solutions**:
```bash
# Check if code location is enabled
cat app/data-platform-service/data-manager/resources/dagster/code-locations.json | jq '.code_locations[] | select(.name == "bridgestone_data_sync")'

# Verify module loads
docker exec data-platform-service python3 -c "from dagster_code.bridgestone_data_sync import defs; print('OK')"

# Check gRPC server logs
docker logs data-platform-service | grep "bridgestone_data_sync"

# Verify port is listening
docker exec data-platform-service netstat -tlnp | grep 4273
```

#### 2. Import Errors in Assets

**Symptoms**:
- `ModuleNotFoundError` when importing utilities
- Assets fail to materialize

**Solutions**:
```python
# Ensure sys.path is set correctly
import sys
sys.path.append('/app/data-manager')  # Add BEFORE imports

# Use absolute imports
from pyairbyte.utils.pyairbyte_sync import sync_connector  # Correct

# Check Python path in container
docker exec data-platform-service python3 -c "import sys; print(sys.path)"
```

#### 3. Cache Connection Errors

**Symptoms**:
- `OperationalError: could not connect to server`
- Cache operations fail

**Solutions**:
```python
# Verify cache configuration
from pyairbyte.utils.common_cache import CACHE_CONFIGS
print(CACHE_CONFIGS["default"])

# Test connection
from pyairbyte.utils.cache_db_manager import PyAirbyteCacheDBManager
db_manager = PyAirbyteCacheDBManager.from_cache_name("default")
conn = db_manager.get_connection()  # Should not raise error

# Check environment variables
docker exec data-platform-service env | grep PYAIRBYTE_CACHE
```

#### 4. DBT Errors

**Symptoms**:
- `Compilation Error: model not found`
- `Database Error: relation does not exist`

**Solutions**:
```bash
# Test DBT parse
docker exec data-platform-service bash -c "cd /app/dbt_models && dbt parse"

# Check source schema in sources.yml
# Ensure schema: pyairbyte_cache is specified

# Test database connection
docker exec data-platform-service bash -c "cd /app/dbt_models && dbt debug"

# Run specific model
docker exec data-platform-service bash -c "cd /app/dbt_models && dbt run --select staging_model"
```

#### 5. Excel Processing Errors

**Symptoms**:
- `FileNotFoundError: Excel file not found`
- `ValueError: Sheet not found in Excel file`
- Type conversion errors

**Solutions**:
```python
# Verify file path
import os
assert os.path.exists("/app/external_files/data.xlsx"), "File not found"

# Check available sheets
reader = ExcelReader()
excel_file = pd.ExcelFile("/app/external_files/data.xlsx")
print(f"Available sheets: {excel_file.sheet_names}")

# Verify field mapping matches Excel columns
df = pd.read_excel("/app/external_files/data.xlsx", nrows=0)
print(f"Excel columns: {df.columns.tolist()}")

# Check table schema
db_manager = PyAirbyteCacheDBManager(...)
schema = db_manager.get_table_schema("public", "target_table")
print(f"Table columns: {list(schema.keys())}")
```

#### 6. Event Processing Issues

**Symptoms**:
- Duplicate events created
- Events not being processed
- Event status not updating

**Solutions**:
```python
# Check event deduplication
from pyairbyte.utils.event_store import bulk_write_events
events = [{"event_type": "TEST", "event_data": {"id": 1}}]
result = bulk_write_events(events)
print(f"Created: {result['events_created']}, Duplicates: {result['duplicates_skipped']}")

# Query unprocessed events
from pyairbyte.utils.event_store import get_unprocessed_or_failed_events
unprocessed = get_unprocessed_or_failed_events("TEST")
print(f"Unprocessed: {len(unprocessed)}")

# Check event status in database
from pyairbyte.utils.graphql_util import query_graphql_api
query = """
query {
  event_store(where: {event_type: {_eq: "TEST"}}) {
    id
    status
    created_at
  }
}
"""
result = query_graphql_api(query)
```

### Debugging Techniques

#### 1. Add Detailed Logging

```python
@asset(name="debug_asset")
def debug_asset(context: AssetExecutionContext):
    context.log.info("=" * 50)
    context.log.info("Starting debug_asset")
    context.log.info(f"Context run ID: {context.run_id}")
    
    try:
        # Log input data
        context.log.debug(f"Input parameters: {locals()}")
        
        # Perform operation
        result = perform_operation()
        
        # Log intermediate results
        context.log.debug(f"Intermediate result: {result}")
        
        # Log final result
        context.log.info(f"Final result: {result}")
        return result
        
    except Exception as e:
        context.log.error(f"Exception type: {type(e).__name__}")
        context.log.error(f"Exception message: {str(e)}")
        context.log.error(f"Exception traceback:", exc_info=True)
        raise
```

#### 2. Test Assets in Isolation

```python
# Test asset outside Dagster
from dagster import build_asset_context
from dagster_code.sales_pipeline.assets import sync_sales_data

# Build mock context
context = build_asset_context(
    resources={},
    run_id="test-run-123"
)

# Run asset
result = sync_sales_data(context)
print(f"Result: {result}")
```

#### 3. Inspect Cache Tables

```bash
# List cache tables
docker exec -e PGPASSWORD=dataplatpassword data-platform-service \
  psql -h db -U dataplatuser -d dataplatform \
  -c "SELECT tablename FROM pg_tables WHERE schemaname = 'pyairbyte_cache';"

# Sample data from cache
docker exec -e PGPASSWORD=dataplatpassword data-platform-service \
  psql -h db -U dataplatuser -d dataplatform \
  -c "SELECT * FROM pyairbyte_cache.connector_table LIMIT 5;"

# Check table schema
docker exec -e PGPASSWORD=dataplatpassword data-platform-service \
  psql -h db -U dataplatuser -d dataplatform \
  -c "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'pyairbyte_cache' AND table_name = 'connector_table';"
```

---

## Database Synchronization Utilities

### MSSQL to MSSQL Query Sync (`mssql_to_mssql_sync.py`)

**Purpose**: Query a source MSSQL database and sync results to a destination MSSQL table with checksum-based deduplication and auto-table creation.

**Key Features**:
- Custom SQL query execution on source database
- **Streaming/chunked processing** for large datasets (millions of records)
- **Concurrent chunk processing** using thread pools (up to 8 workers)
- **Thread-safe design** with dedicated connections per worker thread
- Checksum-based deduplication (SHA256 hash)
- Auto-create destination table with inferred schema
- MERGE/UPSERT operations (insert new, update changed, skip duplicates)
- Support for Entra ID Service Principal and SQL Authentication
- Configurable record-level error handling (skip vs fail-fast)
- Progress tracking for long-running operations

**Authentication Methods**:
1. **Entra ID Service Principal** (Azure SQL):
   - Uses `client_id`, `client_secret`, `tenant_id` for authentication
   - Encrypted connections with certificate validation
   - Recommended for Azure SQL Database

2. **SQL Authentication** (On-premises or Azure):
   - Uses `username` and `password` for authentication
   - Compatible with both on-premises and cloud MSSQL

**Main Function**: `sync_mssql_query_to_mssql()`

**Parameters**:
- `source_config`: Source connection config (dict with auth credentials)
- `source_query`: SQL SELECT query to execute on source
- `dest_config`: Destination connection config (dict with auth credentials)
- `dest_schema`: Destination schema name (e.g., "dbo", "reporting")
- `dest_table`: Destination table name
- `merge_key_columns`: Optional columns to use as merge keys (default: use checksum)
- `batch_size`: Rows per batch for MERGE operation (default: 10000)
- `validate_row_counts`: Whether to validate row counts (default: True)
- `chunk_size`: Rows to read per chunk from source (default: 1000, optimized for streaming)
- `max_workers`: Number of concurrent workers for chunk processing (default: 4)
- `use_streaming`: Enable streaming mode for large datasets (default: True)

**Environment Variables**:
- `SKIP_ON_DATA_RECORD_LEVEL_ERROR`: "true" or "false" (default: "false")
  - `true`: Skip problematic records and continue processing
  - `false`: Fail immediately on first record-level error

**Usage Example - Entra ID Authentication**:

```python
import sys
import os
from dagster import asset, MetadataValue, AssetExecutionContext

sys.path.append('/app/data-manager')
from pyairbyte.utils.mssql_to_mssql_sync import sync_mssql_query_to_mssql

@asset(name="sync_sales_report", group_name="sales_pipeline")
def sync_sales_report(context: AssetExecutionContext):
    """Sync sales report from source to destination MSSQL using Entra ID."""
    
    # Source: Azure SQL with Entra ID Service Principal
    source_config = {
        "server": os.getenv("SOURCE_AZURE_SQL_SERVER"),  # e.g., "source.database.windows.net"
        "database": os.getenv("SOURCE_AZURE_SQL_DB"),
        "client_id": os.getenv("SOURCE_AZURE_CLIENT_ID"),
        "client_secret": os.getenv("SOURCE_AZURE_CLIENT_SECRET"),
        "tenant_id": os.getenv("SOURCE_AZURE_TENANT_ID"),
        "port": "1433"
    }
    
    # Destination: Azure SQL with Entra ID Service Principal
    dest_config = {
        "server": os.getenv("DEST_AZURE_SQL_SERVER"),
        "database": os.getenv("DEST_AZURE_SQL_DB"),
        "client_id": os.getenv("DEST_AZURE_CLIENT_ID"),
        "client_secret": os.getenv("DEST_AZURE_CLIENT_SECRET"),
        "tenant_id": os.getenv("DEST_AZURE_TENANT_ID"),
        "port": "1433"
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
    
    result = sync_mssql_query_to_mssql(
        source_config=source_config,
        source_query=query,
        dest_config=dest_config,
        dest_schema="reporting",
        dest_table="recent_orders",
        merge_key_columns=["OrderID"],  # Use OrderID as merge key
        batch_size=10000,
        chunk_size=1000,  # Stream 1000 rows at a time
        max_workers=4,  # Process up to 4 chunks concurrently
        use_streaming=True  # Enable for large datasets
    )
    
    # Handle result based on status
    if result['status'] == 'success':
        context.add_output_metadata({
            "status": MetadataValue.text("success"),
            "rows_inserted": MetadataValue.int(result['result']['rows_inserted']),
            "rows_updated": MetadataValue.int(result['result']['rows_updated']),
            "rows_unchanged": MetadataValue.int(result['result']['rows_unchanged'])
        })
    elif result['status'] == 'partial_success':
        context.add_output_metadata({
            "status": MetadataValue.text("partial_success"),
            "rows_skipped": MetadataValue.int(result['result']['rows_skipped'])
        })
        context.log.warning(f"Partial success: {result['result']['rows_skipped']} rows skipped")
    
    return result
```

**Usage Example - SQL Authentication**:

```python
# For on-premises MSSQL with SQL Authentication
source_config = {
    "server": "onprem-server.local",
    "database": "production_db",
    "username": os.getenv("MSSQL_USERNAME"),
    "password": os.getenv("MSSQL_PASSWORD"),
    "schema": "dbo"
}
```

**Checksum-Based Deduplication**:

The utility generates a SHA256 checksum for each row based on specified columns (or all columns). The MERGE operation then:
- **Inserts** rows with new checksums (not in destination)
- **Updates** rows with matching merge keys but different checksums (data changed)
- **Skips** rows with identical checksums (no change)

**Auto-Table Creation**:

If the destination table doesn't exist, it is automatically created with:
- Inferred schema from source query results
- `_sync_checksum VARCHAR(64)` column for deduplication
- `_sync_updated_at DATETIME2` column for tracking
- Index on `_sync_checksum` for performance

**Error Handling**:

- **System-level errors** (connection, query syntax, permissions): Always fail immediately
- **Record-level errors** (data conversion, validation): Configurable via `SKIP_ON_DATA_RECORD_LEVEL_ERROR`
  - When `true`: Skip problematic records, log errors, return partial success
  - When `false`: Fail immediately, rollback transaction

**Return Value**:

```python
{
    "status": "success" | "partial_success" | "error",
    "source_query": "SELECT ...",
    "destination": "schema.table",
    "authentication_method": "source:service_principal, dest:sql_auth",
    "result": {
        "rows_queried": 1000000,
        "rows_inserted": 850000,
        "rows_updated": 100000,
        "rows_unchanged": 50000,
        "rows_skipped": 0,
        "chunks_processed": 1000,  # Number of chunks processed
        "processing_time_seconds": 125.5,
        "streaming_enabled": true,
        "chunk_size": 1000,
        "max_workers": 4,
        "error_summary": {...}
    }
}
```

**Performance Notes**:
- **Datasets < 10K rows**: Use `use_streaming=False` for faster processing
- **Datasets 10K-1M rows**: Use `use_streaming=True` with `chunk_size=1000`, `max_workers=4`
- **Datasets > 1M rows**: Use `use_streaming=True` with `chunk_size=1000`, `max_workers=6-8`
- **Memory Usage**: Streaming mode uses ~10MB per chunk (1000 rows), concurrent processing uses ~40MB with 4 workers
- **Processing Speed**: 4 concurrent workers can process ~4000 rows/second with typical Azure SQL latency

**Thread Safety**:
- Implements industry-standard pattern: **separate database connections per worker thread**
- pyodbc connections have threadsafety level 1 (connections cannot be shared across threads)
- Each thread creates its own connection at chunk start, closes in finally block
- Prevents "Connection is busy with results for another command" errors
- Thread-safe table creation using locks (only created once by first thread)

---

## Summary

The **Data Platform Service** is a production-ready, enterprise-grade data orchestration engine that provides:

- **Scalable Architecture**: gRPC-based code locations with independent scaling
- **Reusable Components**: 15+ utility modules for common data operations
- **Multi-Source Integration**: APIs, databases, Excel, SharePoint, and more
- **Robust Processing**: Streaming, chunking, error handling, and retry logic
- **DBT Integration**: Seamless SQL-based transformations with version control
- **Configuration-Driven**: JSON-based configuration for code locations and caches
- **Zero-Coupling**: Complete isolation between platform and application layers
- **Production-Ready**: Comprehensive error handling, logging, and monitoring

Use this documentation as a reference when building new data pipelines, understanding the system architecture, or troubleshooting issues. For specific implementation examples, refer to the existing code locations in `dagster_code/`.

---

**Need Help?**

- Check existing code locations for patterns and examples
- Review utility module documentation in source files
- Consult the PyAirbyte Integration Guide: `docs/pyairbyte-dagster-integration-guide.md`
- Review the Platform Architecture documentation: `AGENTS.md`

**Contributing**:

When adding new features:
1. Follow the Assets vs Utils pattern
2. Add comprehensive docstrings and comments
3. Include error handling and logging
4. Update this documentation with new patterns
5. Add tests for new utilities and assets
