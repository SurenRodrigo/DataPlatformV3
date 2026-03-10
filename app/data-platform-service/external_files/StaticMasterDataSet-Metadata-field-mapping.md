# StaticMasterDataSet-Metadata.xlsx — Field Mapping (Norwegian → English)

Use the **Excel column** as the key and **table_column** as the value in `field_mapping`.

| # | Excel column (Norwegian)     | English meaning              | table_column (for field_mapping) |
|---|-----------------------------|-----------------------------|----------------------------------|
| 1 | Nr.                         | Number / Item no.           | `item_number`                    |
| 2 | Varenummer                  | Article number / Item code  | `item_code`                      |
| 3 | Varebeskrivelse             | Product description         | `item_description`               |
| 4 | På lager                    | In stock                    | `stock_quantity`                 |
| 5 | Ordre kunde                 | Customer order              | `customer_order_quantity`        |
| 6 | Bestilt leverandør          | Ordered from supplier       | `supplier_order_quantity`        |
| 7 | Varegruppe                  | Product group               | `item_group`                     |
| 8 | Strekkode                   | Barcode                     | `barcode`                        |
| 9 | Egenskap 1                  | Attribute 1                 | `feature_1`                      |
|10 | Leverandørkatalognr.        | Supplier catalog number     | `supplier_catalog_number`        |
|11 | Tilleggs-ID                 | Additional ID               | `additional_id`                  |
|12 | BS dekkmerke                | BS tire brand               | `tire_brand`                     |
|13 | PlyRating                   | Ply rating                  | `ply_rating`                     |
|14 | Mønster                     | Tread pattern              | `tread_pattern`                   |
|15 | Volum - salgsenhet          | Volume per sales unit       | `sales_unit_volume`              |
|16 | Vekt 1 - salgsenhet         | Weight per sales unit       | `sales_unit_weight`              |
|17 | Miljøavgift gruppe          | Environmental tax group     | `environmental_tax_group`        |
|18 | BS BCat                     | BS (Bridgestone) category   | `bs_category`                    |
|19 | IC Synchronize              | IC Synchronize              | `ic_synchronize`                 |
|20 | Egenskap 5                  | Attribute 5                 | `feature_5`                      |
|21 | Aktiv                       | Active                      | `active`                         |
|22 | Salgsvare                   | Sales item / Sellable       | `is_sales_item`                  |
|23 | Siste innkjøpspris          | Last purchase price         | `last_purchase_price`            |
|24 | BS Gruppe                   | BS (Bridgestone) group      | `bs_group`                       |
|25 | EU Fuel                     | EU fuel efficiency rating   | `eu_fuel_rating`                 |
|26 | EU Wether index             | EU wet grip index           | `eu_weather_index`               |
|27 | Eu Noise                    | EU noise class              | `eu_noise_rating`                |
|28 | EU Noice DB                 | EU noise (dB)               | `eu_noise_db`                    |
|29 | Load Index                  | Load index                  | `load_index`                     |
|30 | Hastighetskode              | Speed rating/code           | `speed_rating`                   |
|31 | Bredde                      | Width                       | `width`                          |
|32 | Profil                      | Profile (aspect ratio)      | `profile`                        |
|33 | Felg diameter               | Rim diameter                | `rim_diameter`                   |
|34 | Egenskap 22                 | Attribute 22                | `feature_22`                     |
|35 | Egenskap 6                  | Attribute 6                 | `feature_6`                      |
|36 | Egenskap 7                  | Attribute 7                 | `feature_7`                      |
|37 | Ingen rabatter              | No discounts                | `no_discounts`                   |
|38 | Varegruppe2                 | Product group 2             | `item_group_2`                   |
|39 | Concat_Dim                  | Concatenated dimension      | `concat_dim`                     |
|40 | Netto Lager                 | Net stock                   | `net_stock`                      |
|41 | Pigg/Piggfri                | Studded / Stud-free         | `studded_or_stud_free`           |
|42 | OR Cat                      | OR category                 | `or_category`                    |
|43 | Salg 2024                   | Sales 2024                  | `sales_2024`                     |

---

## Python `field_mapping` dict (copy‑paste)

```python
field_mapping = {
    "Nr.": "item_number",
    "Varenummer": "item_code",
    "Varebeskrivelse": "item_description",
    "På lager": "stock_quantity",
    "Ordre kunde": "customer_order_quantity",
    "Bestilt leverandør": "supplier_order_quantity",
    "Varegruppe": "item_group",
    "Strekkode": "barcode",
    "Egenskap 1": "feature_1",
    "Leverandørkatalognr.": "supplier_catalog_number",
    "Tilleggs-ID": "additional_id",
    "BS dekkmerke": "tire_brand",
    "PlyRating": "ply_rating",
    "Mønster": "tread_pattern",
    "Volum - salgsenhet": "sales_unit_volume",
    "Vekt 1 - salgsenhet": "sales_unit_weight",
    "Miljøavgift gruppe": "environmental_tax_group",
    "BS BCat": "bs_category",
    "IC Synchronize": "ic_synchronize",
    "Egenskap 5": "feature_5",
    "Aktiv": "active",
    "Salgsvare": "is_sales_item",
    "Siste innkjøpspris": "last_purchase_price",
    "BS Gruppe": "bs_group",
    "EU Fuel": "eu_fuel_rating",
    "EU Wether index": "eu_weather_index",
    "Eu Noise": "eu_noise_rating",
    "EU Noice DB": "eu_noise_db",
    "Load Index": "load_index",
    "Hastighetskode": "speed_rating",
    "Bredde": "width",
    "Profil": "profile",
    "Felg diameter": "rim_diameter",
    "Egenskap 22": "feature_22",
    "Egenskap 6": "feature_6",
    "Egenskap 7": "feature_7",
    "Ingen rabatter": "no_discounts",
    "Varegruppe2": "item_group_2",
    "Concat_Dim": "concat_dim",
    "Netto Lager": "net_stock",
    "Pigg/Piggfri": "studded_or_stud_free",
    "OR Cat": "or_category",
    "Salg 2024": "sales_2024",
}
```

**Duplicate header:** There are two Excel columns named `Concat_Dim`. Only the first is mapped to `concat_dim`; the second (pandas: `Concat_Dim.1`) is not mapped and will be dropped.

---

## Naming conventions used

- **Numbers / IDs:** `item_number`, `item_code`, `additional_id`
- **Descriptions:** `item_description`
- **Quantities / numeric measures:** `stock_quantity`, `sales_unit_volume`, `sales_unit_weight`, `net_stock`, `sales_2024`
- **Flags / booleans:** `active`, `is_sales_item`, `no_discounts`, `ic_synchronize`
- **Attributes (Egenskap):** `feature_1`, `feature_5`, `feature_6`, `feature_7`, `feature_22`
- **Tire/BS terms:** `tire_brand`, `ply_rating`, `tread_pattern`, `bs_category`, `bs_group`, `rim_diameter`, `width`, `profile`, `speed_rating`, `studded_or_stud_free`
- **EU labels:** `eu_fuel_rating`, `eu_weather_index`, `eu_noise_rating`, `eu_noise_db`
- **Pricing / commercial:** `last_purchase_price`, `supplier_catalog_number`
- **Groups / categories:** `item_group`, `item_group_2`, `environmental_tax_group`, `or_category`

All `table_column` values are `snake_case` and suitable for PostgreSQL/Azure SQL.
