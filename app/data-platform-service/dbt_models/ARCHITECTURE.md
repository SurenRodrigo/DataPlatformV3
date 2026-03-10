# dbt PoC Architecture Flow

## Complete Data Pipeline Architecture

```
Source (pyairbyte_cache)
    │
    ├─► [Dimensions] ──► Snapshots (track history) ──► Staging (current records) ──► Core ──► Reporting
    │        │                    │
    │        │                    ├── snap_customer_info
    │        │                    ├── snap_product_data
    │        │                    └── snap_acc_cat_mapping
    │        │
    │        └── source_customer_info
    │        └── source_meta_data
    │        └── source_account_category_map
    │
    └─► [Facts] ──────────────────► Staging (direct from source) ──► Core ──► Reporting
              │
              ├── source_invoice_data ──► stg_invoice_data
              └── source_credit_data ──► stg_credit_data
```

## Staging Models Detail

### Dimension Staging Models (from Snapshots)

```
Snapshots
    │
    ├── snap_customer_info ──► stg_customer_data
    │       │
    │       └── Transformations:
    │           • Boolean conversion (9 fields: is_inactive, is_deleted, is_active, 
    │             ic_synchronize, edi_active, frakttillegg, fakturagebyr, feature_7, feature_1)
    │
    ├── snap_product_data ──► stg_product_data
    │       │
    │       └── Transformations:
    │           • Boolean conversion (11 fields: feature_1, additional_id, ply_rating,
    │             ic_synchronize, feature_5, is_active, is_sales_item, feature_22,
    │             feature_6, feature_7, no_discounts)
    │
    └── snap_acc_cat_mapping ──► stg_acc_cat_mapping
            │
            └── Transformations:
                • Trim whitespace (account, category)
                • Filter empty/null values
```

### Fact Staging Models (direct from Source)

```
Sources
    │
    ├── source_invoice_data ──► stg_invoice_data
    │       │
    │       └── Transformations:
    │           • Date conversion (dd.mm.yy → DATE)
    │           • Add calculated columns: year, eom_bucket
    │
    └── source_credit_data ──► stg_credit_data
            │
            └── Transformations:
                • Date conversion (dd.mm.yy → DATE)
                • Add calculated columns: year, eom_bucket
```

## Summary of Staging Models

| Staging Model | Source | Snapshot | Type | Key Transformations |
|--------------|--------|----------|------|---------------------|
| `stg_customer_data` | `source_customer_info` | `snap_customer_info` | Dimension | Boolean conversion (9 fields) |
| `stg_product_data` | `source_meta_data` | `snap_product_data` | Dimension | Boolean conversion (11 fields) |
| `stg_acc_cat_mapping` | `source_account_category_map` | `snap_acc_cat_mapping` | Reference | Trim, filter empty values |
| `stg_invoice_data` | `source_invoice_data` | None (direct) | Fact | Date conversion, year, eom_bucket |
| `stg_credit_data` | `source_credit_data` | None (direct) | Fact | Date conversion, year, eom_bucket |

## Data Flow Patterns

### Dimension Tables (Mutable)
```
Source → Snapshot (track history) → Staging (current state) → Core → Reporting
```

### Fact Tables (Append-Only)
```
Source → Staging (current state) → Core → Reporting
```

## Key Design Decisions

1. **Snapshots on Sources**: Preserves raw data history, allows recalculation if staging logic changes
2. **Staging Reads Current Snapshots**: Uses `WHERE dbt_valid_to IS NULL` to get latest version
3. **No Snapshots for Facts**: Invoice and credit data are append-only, no need for historical tracking
4. **Stable Source Systems**: SAP B1 sources are stable, reducing risk of snapshot logic changes
