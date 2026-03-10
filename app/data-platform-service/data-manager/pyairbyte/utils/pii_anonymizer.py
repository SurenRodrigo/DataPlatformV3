import logging
import os
from typing import Any, Dict, List, Optional, Iterable, Set, Tuple, Union

import hashlib
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


logger = logging.getLogger(__name__)


# Supported field_type values for realistic replacement (Faker). Maps to Faker methods.
# When field_type is set, replacements look like real names/addresses/emails etc. and are unique per hash.
FAKER_FIELD_TYPES: Dict[str, str] = {
    "name": "name",
    "person": "name",
    "address": "address",
    "street": "street_address",
    "city": "city",
    "postal_code": "postcode",
    "postcode": "postcode",
    "email": "email",
    "phone": "phone_number",
    "phone_number": "phone_number",
    "company": "company",
    "organization": "company",
    "text": "sentence",
    "sentence": "sentence",
}


def _should_anonymize() -> bool:
    """
    Global switch for anonymization controlled by ANNONYMIZE_DATA env var.

    - ANNONYMIZE_DATA=false (case-insensitive) -> do NOT anonymize, return original values.
    - Any other value or missing env var        -> anonymize (default behaviour).
    """
    flag = os.getenv("ANNONYMIZE_DATA")
    if flag is None:
        return True
    return flag.strip().lower() not in {"false", "0", "no"}


# Target location for the PII mapping table:
# - Database: DAGSTER_DB_NAME (falls back to APPBASE_DB_NAME or "dagster")
# - Schema : public
PII_SCHEMA_NAME = os.getenv("PII_MAPPING_SCHEMA", "public")
PII_TABLE_NAME = os.getenv("PII_MAPPING_TABLE", "pii_field_mappings")


def _get_dagster_db_connection() -> psycopg2.extensions.connection:
    """
    Get a PostgreSQL connection to the Dagster database.

    Connection details:
    - Host/User/Password/Port from APPBASE_DB_* (same Postgres instance)
    - Database from DAGSTER_DB_NAME (or APPBASE_DB_NAME, or "dagster" as fallback)
    """
    host = os.getenv("APPBASE_DB_HOST", "db")
    port = int(os.getenv("APPBASE_DB_PORT", "5432"))
    user = os.getenv("APPBASE_DB_USER", "dataplatuser")
    password = os.getenv("APPBASE_DB_PASSWORD", "dataplatpassword")
    database = os.getenv(
        "DAGSTER_DB_NAME",
        os.getenv("APPBASE_DB_NAME", "dagster"),
    )

    logger.info(
        "Connecting to Dagster PII mapping database",
        extra={"host": host, "port": port, "database": database},
    )

    return psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
    )


def _ensure_mapping_table_exists(conn: psycopg2.extensions.connection) -> None:
    """
    Ensure the PII mapping table exists in the Dagster database.

    Schema (public.pii_field_mappings by default):
        id               SERIAL PRIMARY KEY
        source_system    VARCHAR(100)  NOT NULL
        field_name       VARCHAR(255)  NOT NULL
        context          VARCHAR(255)  NOT NULL DEFAULT ''
        hash_algorithm   VARCHAR(50)   NOT NULL  -- e.g. 'SHA256'
        hash_value       VARCHAR(128)  NOT NULL  -- hex-encoded hash
        replacement_value VARCHAR(255) NOT NULL
        created_at       TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
        updated_at       TIMESTAMP     DEFAULT CURRENT_TIMESTAMP

    Uniqueness:
        (source_system, field_name, context, hash_algorithm, hash_value)
    """
    table_qualified = f'{PII_SCHEMA_NAME}.{PII_TABLE_NAME}'

    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE SCHEMA IF NOT EXISTS {PII_SCHEMA_NAME};
            """
        )

        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_qualified} (
                id SERIAL PRIMARY KEY,
                source_system      VARCHAR(100)  NOT NULL,
                field_name         VARCHAR(255)  NOT NULL,
                context            VARCHAR(255)  NOT NULL DEFAULT '',
                hash_algorithm     VARCHAR(50)   NOT NULL,
                hash_value         VARCHAR(128)  NOT NULL,
                replacement_value  VARCHAR(255)  NOT NULL,
                created_at         TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
                updated_at         TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT uq_pii_field_mappings
                    UNIQUE (source_system, field_name, context, hash_algorithm, hash_value)
            );
            """
        )

        # Optional index to speed up lookups by hash
        cur.execute(
            f"""
            CREATE INDEX IF NOT EXISTS ix_pii_field_mappings_lookup
            ON {table_qualified} (source_system, field_name, context, hash_algorithm, hash_value);
            """
        )

    conn.commit()


def _normalize_context(context: Optional[str]) -> str:
    """Normalize context to a non-null string for storage."""
    return (context or "").strip()


def _normalize_value_for_hash(value: Any) -> str:
    """
    Normalize a raw value into a deterministic string for hashing.

    - None/NaN → empty string
    - datetime-like → ISO8601
    - bytes → hex
    - strings → strip + lowercase so "John", "JOHN", "john" produce the same hash
    - others → str(value) then strip + lowercase
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""

    # Pandas NaT / NaN handling
    if isinstance(value, (pd.Timestamp, pd.NaT.__class__)):  # type: ignore[attr-defined]
        if pd.isna(value):
            return ""
        return value.isoformat()

    # numpy / pandas NA
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        # pd.isna does not support all types; ignore
        pass

    if isinstance(value, bytes):
        return value.hex()

    # Case and whitespace normalization so different capitalizations produce the same hash
    return str(value).strip().lower()


def _hash_value(raw_value: Any, algorithm: str = "SHA256") -> str:
    """Compute a one-way hash for a raw value using the given algorithm."""
    value_str = _normalize_value_for_hash(raw_value)

    algo_upper = algorithm.upper()
    if algo_upper not in {"SHA256", "SHA1", "MD5"}:
        raise ValueError(f"Unsupported hash algorithm: {algorithm}")

    if algo_upper == "SHA256":
        h = hashlib.sha256()
    elif algo_upper == "SHA1":
        h = hashlib.sha1()
    else:
        h = hashlib.md5()

    h.update(value_str.encode("utf-8"))
    return h.hexdigest()


def _generate_replacement_value(
    hash_value: str,
    field_name: str,
    prefix: Optional[str] = None,
    max_length: int = 64,
) -> str:
    """
    Generate a human-readable replacement value from a hash.

    By default, uses `<UPPER_FIELD_PREFIX>_<hash_prefix>`, truncated to max_length.
    """
    if max_length <= 0:
        raise ValueError("max_length must be positive")

    if prefix is None:
        # Use field name prefix as default, e.g. EMAIL_ or PHONE_
        base = (field_name or "PII").upper()
        prefix = f"{base[:8]}_"

    # Ensure prefix itself is not longer than max_length
    prefix = prefix[: max_length]
    remaining = max_length - len(prefix)
    if remaining <= 0:
        return prefix

    return prefix + hash_value[:remaining]


def _get_faker_generator(locale: Optional[str] = None):  # type: ignore[no-untyped-def]
    """Return a Faker instance (lazy import to avoid requiring Faker unless using field_type)."""
    try:
        from faker import Faker
    except ImportError:
        raise ImportError(
            "Using field_type for realistic PII replacement requires the Faker package. "
            "Install with: pip install Faker"
        )
    return Faker(locale or "en_US")


def _faker_generate_one(fake: Any, field_type: str) -> str:
    """Generate one value from Faker for the given field_type (e.g. 'name', 'email')."""
    method_name = FAKER_FIELD_TYPES.get((field_type or "").strip().lower())
    if not method_name:
        raise ValueError(
            f"Unsupported field_type for realistic replacement: {field_type!r}. "
            f"Supported: {list(FAKER_FIELD_TYPES.keys())}"
        )
    method = getattr(fake, method_name, None)
    if not callable(method):
        raise ValueError(f"Faker has no method {method_name!r} for field_type {field_type!r}")
    return str(method())


def _get_existing_replacement_values(
    conn: psycopg2.extensions.connection,
    source_system: str,
    field_name: str,
    context: str,
) -> Set[str]:
    """Return set of replacement_value strings already stored for this (source_system, field_name, context)."""
    table_qualified = f"{PII_SCHEMA_NAME}.{PII_TABLE_NAME}"
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT replacement_value
            FROM {table_qualified}
            WHERE source_system = %s AND field_name = %s AND context = %s
            """,
            (source_system, field_name, context),
        )
        rows = cur.fetchall()
    return {row[0] for row in rows} if rows else set()


def _generate_realistic_replacement(
    hash_value: str,
    field_type: str,
    field_name: str,
    max_length: int,
    conn: psycopg2.extensions.connection,
    source_system: str,
    context: str,
    locale: Optional[str] = None,
    forbidden: Optional[Set[str]] = None,
) -> str:
    """
    Generate a realistic-looking replacement (name, address, email, etc.) that is deterministic
    for the given hash and unique among existing DB mappings and (optionally) the forbidden set.
    Uses Faker with seed derived from hash; on collision reseeds until a new value is found.
    If forbidden is provided, values in it are avoided and the chosen value is added to it.
    """
    existing = _get_existing_replacement_values(conn, source_system, field_name, context)
    if forbidden is not None:
        existing = existing | forbidden
    try:
        seed = int(hash_value[:16], 16)
    except (ValueError, TypeError):
        seed = 0
    fake = _get_faker_generator(locale)
    for attempt in range(10000):
        s = seed + attempt
        fake.seed_instance(s)
        value = _faker_generate_one(fake, field_type)
        if max_length and len(value) > max_length:
            value = value[:max_length]
        if value and value not in existing:
            if forbidden is not None:
                forbidden.add(value)
            return value
    logger.warning(
        "Could not generate unique Faker value after 10000 attempts for field_type=%s; using hash fallback",
        field_type,
    )
    fallback = _generate_replacement_value(
        hash_value, field_name=field_name, prefix="PII_", max_length=max_length
    )
    if forbidden is not None:
        forbidden.add(fallback)
    return fallback


def get_or_create_replacement_for_value(
    source_system: str,
    field_name: str,
    raw_value: Any,
    context: Optional[str] = None,
    *,
    hash_algorithm: str = "SHA256",
    replacement_prefix: Optional[str] = None,
    max_length: int = 64,
    field_type: Optional[str] = None,
    locale: Optional[str] = None,
) -> str:
    """
    Get or create a replacement value for a single PII value.

    If field_type is set (e.g. 'name', 'email', 'address'), uses Faker to generate
    a realistic-looking value that is unique and deterministic for this hash.
    Otherwise uses prefix + hash (or defaults).
    """
    if not _should_anonymize():
        return raw_value  # type: ignore[return-value]

    if raw_value is None or (isinstance(raw_value, float) and pd.isna(raw_value)):
        return raw_value  # type: ignore[return-value]

    norm_ctx = _normalize_context(context)
    hash_val = _hash_value(raw_value, hash_algorithm)

    conn = _get_dagster_db_connection()
    try:
        _ensure_mapping_table_exists(conn)
        table_qualified = f'{PII_SCHEMA_NAME}.{PII_TABLE_NAME}'

        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT replacement_value
                FROM {table_qualified}
                WHERE source_system = %s
                  AND field_name = %s
                  AND context = %s
                  AND hash_algorithm = %s
                  AND hash_value = %s
                """,
                (source_system, field_name, norm_ctx, hash_algorithm.upper(), hash_val),
            )
            row = cur.fetchone()
            if row:
                return row[0]

            if field_type and (field_type.strip().lower() in FAKER_FIELD_TYPES):
                replacement = _generate_realistic_replacement(
                    hash_val,
                    field_type=field_type,
                    field_name=field_name,
                    max_length=max_length,
                    conn=conn,
                    source_system=source_system,
                    context=norm_ctx,
                    locale=locale,
                )
            else:
                replacement = _generate_replacement_value(
                    hash_val,
                    field_name=field_name,
                    prefix=replacement_prefix,
                    max_length=max_length,
                )

            cur.execute(
                f"""
                INSERT INTO {table_qualified}
                    (source_system, field_name, context, hash_algorithm, hash_value, replacement_value)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (source_system, field_name, context, hash_algorithm, hash_value)
                DO NOTHING
                """,
                (
                    source_system,
                    field_name,
                    norm_ctx,
                    hash_algorithm.upper(),
                    hash_val,
                    replacement,
                ),
            )
        conn.commit()
        return replacement
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _bulk_get_or_create_replacements(
    conn: psycopg2.extensions.connection,
    source_system: str,
    field_name: str,
    unique_values: Iterable[Any],
    *,
    context: Optional[str],
    hash_algorithm: str,
    replacement_prefix: Optional[str],
    max_length: int,
    field_type: Optional[str] = None,
    locale: Optional[str] = None,
) -> Dict[Any, str]:
    """
    Bulk lookup and creation of replacement values for a set of raw values.

    If field_type is set (e.g. 'name', 'email'), new replacements are generated with Faker
    so they look realistic and are unique. Otherwise uses prefix + hash.
    Returns a mapping raw_value -> replacement_value.
    """
    norm_ctx = _normalize_context(context)
    algo = hash_algorithm.upper()
    table_qualified = f'{PII_SCHEMA_NAME}.{PII_TABLE_NAME}'
    use_realistic = field_type and (field_type.strip().lower() in FAKER_FIELD_TYPES)

    # Normalize and hash all unique values
    raw_list = list(unique_values)
    if not raw_list:
        return {}

    normalized_pairs: Dict[Any, Tuple[str, str]] = {}
    for raw in raw_list:
        value_str = _normalize_value_for_hash(raw)
        hash_val = _hash_value(raw, algo)
        normalized_pairs[raw] = (value_str, hash_val)

    all_hashes = [hv for (_val_str, hv) in normalized_pairs.values()]

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT hash_value, replacement_value
            FROM {table_qualified}
            WHERE source_system = %s
              AND field_name = %s
              AND context = %s
              AND hash_algorithm = %s
              AND hash_value = ANY(%s)
            """,
            (source_system, field_name, norm_ctx, algo, all_hashes),
        )
        existing = dict(cur.fetchall()) if cur.rowcount and cur.rowcount > 0 else {}

        missing_hashes = {
            raw: hv
            for raw, (_val_str, hv) in normalized_pairs.items()
            if hv not in existing
        }

        if missing_hashes:
            used_in_batch: Set[str] = set(existing.values()) if use_realistic else set()
            rows_to_insert = []
            for raw, hv in missing_hashes.items():
                if use_realistic:
                    replacement = _generate_realistic_replacement(
                        hv,
                        field_type=field_type,
                        field_name=field_name,
                        max_length=max_length,
                        conn=conn,
                        source_system=source_system,
                        context=norm_ctx,
                        locale=locale,
                        forbidden=used_in_batch,
                    )
                else:
                    replacement = _generate_replacement_value(
                        hv,
                        field_name=field_name,
                        prefix=replacement_prefix,
                        max_length=max_length,
                    )
                rows_to_insert.append(
                    (
                        source_system,
                        field_name,
                        norm_ctx,
                        algo,
                        hv,
                        replacement,
                    )
                )
                existing[hv] = replacement

            execute_values(
                cur,
                f"""
                INSERT INTO {table_qualified}
                    (source_system, field_name, context, hash_algorithm, hash_value, replacement_value)
                VALUES %s
                ON CONFLICT (source_system, field_name, context, hash_algorithm, hash_value)
                DO NOTHING
                """,
                rows_to_insert,
            )

    conn.commit()

    value_to_replacement = {
        raw: existing[hv]
        for raw, (_val_str, hv) in normalized_pairs.items()
        if hv in existing
    }
    return value_to_replacement


def _normalize_pii_config(
    pii_config: Union[Dict[str, Any], List[str]]
) -> Dict[str, Any]:
    """
    Normalize PII config to a dict of column_name -> config.
    - If a list of field names: treat as column_name -> True (defaults).
    - If a dict: pass through (values can be True or dict with optional overrides).
    """
    if isinstance(pii_config, list):
        return {col: True for col in pii_config}
    return pii_config


def anonymize_dataframe(
    df: pd.DataFrame,
    pii_config: Union[Dict[str, Any], List[str]],
    source_system: str,
    context: Optional[str] = None,
) -> pd.DataFrame:
    """
    Anonymize PII columns in a pandas DataFrame using deterministic replacements.

    Args:
        df: Input DataFrame (will not be modified in-place).
        pii_config:
            Either a list of PII column names (use defaults for all), or a dict:
            - List: e.g. ["fp_name", "email"] — anonymize those columns with defaults.
            - Dict: column_name -> config, where config can be:
              - True: use defaults (SHA256, prefix derived from field name, max_length=64).
              - Dict with optional overrides: pass only the keys you want to override; the rest use defaults.
                    Keys: hash_algorithm, replacement_prefix, max_length, field_type, locale.
                    - field_type: when set, generates realistic-looking values (e.g. real-looking names,
                      addresses, emails) via Faker instead of prefix+hash. Supported: name, address, email,
                      phone, company, city, postal_code, street, text/sentence. Each value is unique and
                      deterministic for the same PII input.
                    - locale: optional Faker locale (e.g. "en_US", "de_DE") for field_type generation.
                    Example: {"field_type": "name"} or {"replacement_prefix": "EMAIL_", "field_type": "email"}.
        source_system:
            Logical source system identifier (e.g. "bridgestone_data_sync").
        context:
            Optional additional context (e.g. "pyairbyte_cache.source_customer_info").

    Returns:
        New DataFrame with configured PII columns replaced by anonymized values.
    """
    if not _should_anonymize():
        logger.debug("ANNONYMIZE_DATA is disabled; returning DataFrame unchanged")
        return df

    pii_config = _normalize_pii_config(pii_config)
    if not pii_config:
        logger.debug("No PII config provided; returning DataFrame unchanged")
        return df

    if df.empty:
        return df

    df_out = df.copy()

    conn = _get_dagster_db_connection()
    try:
        _ensure_mapping_table_exists(conn)

        for column, cfg in pii_config.items():
            if column not in df_out.columns:
                logger.debug(
                    "PII config column '%s' not present in DataFrame; skipping", column
                )
                continue

            series = df_out[column]
            # Extract non-null unique values
            non_null_series = series[series.notna()]
            if non_null_series.empty:
                continue

            # Default configuration for this column
            if cfg is True:
                cfg_dict: Dict[str, Any] = {}
            elif isinstance(cfg, dict):
                cfg_dict = cfg
            else:
                logger.warning(
                    "Unsupported PII config for column '%s': %r (expected dict or True). Skipping.",
                    column,
                    cfg,
                )
                continue

            hash_algorithm = cfg_dict.get("hash_algorithm", "SHA256")
            replacement_prefix = cfg_dict.get("replacement_prefix")
            max_length = int(cfg_dict.get("max_length", 64))
            field_type = cfg_dict.get("field_type")
            locale = cfg_dict.get("locale")

            unique_values = non_null_series.unique().tolist()
            value_to_replacement = _bulk_get_or_create_replacements(
                conn=conn,
                source_system=source_system,
                field_name=column,
                unique_values=unique_values,
                context=context,
                hash_algorithm=hash_algorithm,
                replacement_prefix=replacement_prefix,
                max_length=max_length,
                field_type=field_type,
                locale=locale,
            )

            if not value_to_replacement:
                continue

            logger.info(
                "Anonymized PII column '%s' in source_system '%s' (unique values: %d)",
                column,
                source_system,
                len(value_to_replacement),
            )

            # Map original values to replacements; keep NaN/None as-is
            df_out[column] = series.map(
                lambda v: value_to_replacement.get(v, v) if pd.notna(v) else v
            )

        return df_out
    finally:
        try:
            conn.close()
        except Exception:
            pass

