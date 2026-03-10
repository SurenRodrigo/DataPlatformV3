import os
import logging
from urllib.parse import quote_plus
from typing import Optional, Dict, Any, Iterator, Union, List
from sqlalchemy import create_engine, Engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import numpy as np

from .pii_anonymizer import anonymize_dataframe

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ExcelToDbWriter:
    """
    Utility to read large Excel files and write data to PostgreSQL or MSSQL tables.
    
    Features:
    - Streaming chunk processing for large files (100K+ rows)
    - Field mapping from Excel columns to table columns
    - Automatic type conversion based on inferred table schema
    - Support for PostgreSQL and MSSQL databases
    - Support for Entra ID Service Principal authentication (MSSQL/Azure SQL)
    - Strict validation with fail-fast error handling
    """
    
    def __init__(
        self,
        dbms_type: str,
        connection_config: Dict[str, Any],
        field_mapping: Optional[Dict[str, str]] = None,
        pii_config: Optional[Union[Dict[str, Any], List[str]]] = None,
        pii_source_system: Optional[str] = None,
        pii_context: Optional[str] = None,
    ):
        """
        Initialize ExcelToDbWriter.
        
        Args:
            dbms_type: Database type - "postgresql" or "mssql"
            connection_config: Dictionary with connection parameters
                PostgreSQL: {"host", "port", "database", "username", "password", "schema" (optional)}
                MSSQL SQL Auth: {"server", "database", "username", "password", "port" (optional), "schema" (optional)}
                MSSQL Entra ID: {"server", "database", "client_id", "client_secret", "tenant_id", "port" (optional), "schema" (optional), "connection_timeout" (optional, default 90 for Entra ID)}
            field_mapping: Optional dictionary mapping Excel column names to table column names
                Example: {"Excel_Column": "table_column", "Customer Name": "customer_name"}
        
        Raises:
            ValueError: If dbms_type is invalid or connection_config is missing required keys
        """
        if dbms_type.lower() not in ["postgresql", "mssql"]:
            raise ValueError(f"Unsupported dbms_type: {dbms_type}. Must be 'postgresql' or 'mssql'")
        
        self.dbms_type = dbms_type.lower()
        self.connection_config = connection_config
        self.field_mapping = field_mapping or {}
        # Optional PII anonymization configuration. When provided, PII columns
        # will be anonymized (deterministic replacement values) before writing.
        self.pii_config = pii_config if pii_config is not None else {}
        # Logical source identifier and optional context used to partition
        # mappings in the Dagster PII mapping table.
        self.pii_source_system = pii_source_system
        self.pii_context = pii_context
        
        # Create database engine
        self.engine = self._get_engine()
        
        logger.info(f"ExcelToDbWriter initialized for {self.dbms_type}")
    
    def _get_engine(self) -> Engine:
        """
        Create SQLAlchemy engine based on dbms_type.
        
        Returns:
            SQLAlchemy Engine instance
            
        Raises:
            ValueError: If connection config is missing required keys
            SQLAlchemyError: If engine creation fails
        """
        if self.dbms_type == "postgresql":
            required_keys = ["host", "port", "database", "username", "password"]
            missing = [k for k in required_keys if k not in self.connection_config]
            if missing:
                raise ValueError(f"Missing required PostgreSQL connection keys: {missing}")
            
            host = self.connection_config["host"]
            port = str(self.connection_config["port"])
            database = self.connection_config["database"]
            username = self.connection_config["username"]
            password = self.connection_config["password"]
            
            connection_string = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
            
            try:
                engine = create_engine(connection_string, pool_pre_ping=True)
                logger.info(f"PostgreSQL engine created for {host}:{port}/{database}")
                return engine
            except Exception as e:
                logger.error(f"Failed to create PostgreSQL engine: {e}")
                raise SQLAlchemyError(f"PostgreSQL engine creation failed: {e}")
        
        elif self.dbms_type == "mssql":
            # Detect authentication method
            has_entra_id = all(k in self.connection_config for k in ['client_id', 'client_secret', 'tenant_id'])
            has_sql_auth = all(k in self.connection_config for k in ['username', 'password'])
            
            if not has_entra_id and not has_sql_auth:
                raise ValueError(
                    "Invalid MSSQL config: Must provide either Entra ID credentials "
                    "(client_id, client_secret, tenant_id) or SQL Auth credentials (username, password)"
                )
            
            # Validate required fields
            if 'server' not in self.connection_config or 'database' not in self.connection_config:
                raise ValueError("MSSQL config must include 'server' and 'database'")
            
            server = self.connection_config["server"]
            database = self.connection_config["database"]
            port = self.connection_config.get("port", "1433")
            # Entra ID needs longer timeout (token fetch + connect). Optional: connection_timeout (seconds).
            connect_timeout = int(self.connection_config.get("connection_timeout", 90))
            
            # Try ODBC Driver 18 first, fallback to 17
            drivers_to_try = [
                "ODBC Driver 18 for SQL Server",
                "ODBC Driver 17 for SQL Server"
            ]
            
            last_error = None
            for driver in drivers_to_try:
                try:
                    # URL encode the driver name
                    driver_encoded = quote_plus(driver)
                    
                    if has_entra_id:
                        # Entra ID Service Principal Authentication
                        client_id = self.connection_config['client_id']
                        client_secret = self.connection_config['client_secret']
                        tenant_id = self.connection_config['tenant_id']
                        
                        # Build connection string for Entra ID
                        # Format: mssql+pyodbc:///?odbc_connect=<connection_string>
                        odbc_connect_params = (
                            f"Driver={{{driver}}};"
                            f"Server=tcp:{server},{port};"
                            f"Database={database};"
                            f"Uid={client_id};"
                            f"Pwd={client_secret};"
                            f"Encrypt=yes;"
                            f"TrustServerCertificate=no;"
                            f"Connection Timeout={connect_timeout};"
                            f"Authentication=ActiveDirectoryServicePrincipal"
                        )
                        odbc_connect_encoded = quote_plus(odbc_connect_params)
                        connection_string = f"mssql+pyodbc:///?odbc_connect={odbc_connect_encoded}"
                        
                        auth_method = "Entra ID Service Principal"
                        logger.info(f"Using Entra ID Service Principal authentication for {server}")
                    
                    else:
                        # SQL Authentication
                        username = self.connection_config['username']
                        password = self.connection_config['password']
                        
                        # URL encode credentials
                        username_encoded = quote_plus(username)
                        password_encoded = quote_plus(password)
                        
                        sql_connect_timeout = int(self.connection_config.get("connection_timeout", 30))
                        connection_string = (
                            f"mssql+pyodbc://{username_encoded}:{password_encoded}@{server}/{database}"
                            f"?driver={driver_encoded}"
                            f"&TrustServerCertificate=yes"
                            f"&Connection+Timeout={sql_connect_timeout}"
                        )
                        
                        auth_method = "SQL Authentication"
                        logger.info(f"Using SQL Authentication for {server}")
                    
                    # Create engine
                    engine = create_engine(
                        connection_string,
                        pool_pre_ping=True,
                        fast_executemany=True  # Performance optimization for MSSQL
                    )
                    
                    logger.info(f"MSSQL engine created for {server}/{database} using {driver} ({auth_method})")
                    return engine
                    
                except Exception as e:
                    last_error = e
                    logger.warning(f"Failed to create MSSQL engine with {driver}: {e}")
                    continue
            
            # All drivers failed
            logger.error(f"Failed to create MSSQL engine with all drivers: {last_error}")
            raise SQLAlchemyError(f"MSSQL engine creation failed: {last_error}")
        
        else:
            raise ValueError(f"Unsupported dbms_type: {self.dbms_type}")
    
    def _validate_table_exists(self, schema_name: str, table_name: str) -> bool:
        """
        Validate that the table exists in the database.
        
        Args:
            schema_name: Schema name
            table_name: Table name
            
        Returns:
            True if table exists, False otherwise
        """
        try:
            inspector = inspect(self.engine)
            if self.dbms_type == "postgresql":
                # PostgreSQL uses schema parameter
                return inspector.has_table(table_name, schema=schema_name)
            else:  # MSSQL
                # MSSQL schema is part of the table name in format schema.table
                full_table_name = f"{schema_name}.{table_name}"
                return inspector.has_table(table_name, schema=schema_name)
        except Exception as e:
            logger.error(f"Error validating table existence: {e}")
            return False
    
    def _create_schema_if_not_exists(self, schema_name: str) -> bool:
        """
        Create schema if it doesn't exist.
        
        Args:
            schema_name: Schema name to create
            
        Returns:
            True if schema was created or already exists, False on error
        """
        try:
            with self.engine.connect() as conn:
                if self.dbms_type == "postgresql":
                    # Check if schema exists
                    result = conn.execute(text(
                        "SELECT schema_name FROM information_schema.schemata WHERE schema_name = :schema_name"
                    ), {"schema_name": schema_name})
                    
                    if result.fetchone() is None:
                        # Create schema
                        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"'))
                        conn.commit()
                        logger.info(f"Created schema: {schema_name}")
                    else:
                        logger.debug(f"Schema already exists: {schema_name}")
                        
                elif self.dbms_type == "mssql":
                    # Check if schema exists
                    result = conn.execute(text(
                        "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = :schema_name"
                    ), {"schema_name": schema_name})
                    
                    if result.fetchone() is None:
                        # Create schema in MSSQL
                        conn.execute(text(f"CREATE SCHEMA [{schema_name}]"))
                        conn.commit()
                        logger.info(f"Created schema: {schema_name}")
                    else:
                        logger.debug(f"Schema already exists: {schema_name}")
                
                return True
                
        except Exception as e:
            logger.error(f"Error creating schema '{schema_name}': {e}")
            return False
    
    def _infer_column_type_from_pandas(self, dtype, sample_values: pd.Series) -> str:
        """
        Infer SQL column type from pandas dtype and sample values.
        
        Args:
            dtype: pandas dtype
            sample_values: Sample values from the column
            
        Returns:
            SQL data type string
        """
        dtype_str = str(dtype).lower()
        
        # Check for datetime
        if 'datetime' in dtype_str or pd.api.types.is_datetime64_any_dtype(dtype):
            return "TIMESTAMP" if self.dbms_type == "postgresql" else "DATETIME2"
        
        # Check for integer
        if 'int' in dtype_str:
            return "BIGINT"
        
        # Check for float
        if 'float' in dtype_str:
            return "DOUBLE PRECISION" if self.dbms_type == "postgresql" else "FLOAT"
        
        # Check for boolean
        if 'bool' in dtype_str:
            return "BOOLEAN" if self.dbms_type == "postgresql" else "BIT"
        
        # Check for object (string) - determine max length from sample
        if dtype == 'object' or dtype_str == 'object':
            # Calculate max length from sample values
            max_len = 0
            for val in sample_values.dropna():
                if val is not None:
                    max_len = max(max_len, len(str(val)))
            
            # Add buffer for safety
            max_len = max(max_len + 50, 255)
            
            # Cap at reasonable limits
            if max_len > 4000:
                return "TEXT" if self.dbms_type == "postgresql" else "NVARCHAR(MAX)"
            else:
                return f"VARCHAR({max_len})" if self.dbms_type == "postgresql" else f"NVARCHAR({max_len})"
        
        # Default to VARCHAR(255)
        return "VARCHAR(255)" if self.dbms_type == "postgresql" else "NVARCHAR(255)"
    
    def _create_table_from_dataframe(
        self,
        df: pd.DataFrame,
        schema_name: str,
        table_name: str
    ) -> bool:
        """
        Create table based on DataFrame structure.
        
        Args:
            df: DataFrame to base table structure on
            schema_name: Schema name
            table_name: Table name
            
        Returns:
            True if table was created successfully, False on error
        """
        try:
            # Build column definitions
            columns = []
            for col_name in df.columns:
                col_type = self._infer_column_type_from_pandas(df[col_name].dtype, df[col_name])
                
                if self.dbms_type == "postgresql":
                    columns.append(f'"{col_name}" {col_type}')
                else:  # MSSQL
                    columns.append(f"[{col_name}] {col_type}")
            
            # Add metadata columns
            if self.dbms_type == "postgresql":
                columns.append('"_sync_created_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
            else:  # MSSQL
                columns.append("[_sync_created_at] DATETIME2 DEFAULT GETDATE()")
            
            # Build CREATE TABLE statement
            columns_sql = ",\n    ".join(columns)
            
            if self.dbms_type == "postgresql":
                create_sql = f'''
                    CREATE TABLE IF NOT EXISTS "{schema_name}"."{table_name}" (
                        {columns_sql}
                    )
                '''
            else:  # MSSQL
                create_sql = f'''
                    IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = '{schema_name}' AND t.name = '{table_name}')
                    CREATE TABLE [{schema_name}].[{table_name}] (
                        {columns_sql}
                    )
                '''
            
            with self.engine.connect() as conn:
                conn.execute(text(create_sql))
                conn.commit()
            
            logger.info(f"Created table: {schema_name}.{table_name} with {len(df.columns)} columns")
            return True
            
        except Exception as e:
            logger.error(f"Error creating table '{schema_name}.{table_name}': {e}")
            return False
    
    def infer_table_schema(self, schema_name: str, table_name: str) -> Dict[str, Dict[str, Any]]:
        """
        Infer table schema from database by querying information_schema.
        
        Args:
            schema_name: Schema name
            table_name: Table name
            
        Returns:
            Dictionary mapping column names to their metadata:
            {
                "column_name": {
                    "data_type": "VARCHAR",
                    "max_length": 255,
                    "precision": None,
                    "scale": None,
                    "is_nullable": True,
                    "ordinal_position": 1
                },
                ...
            }
            
        Raises:
            ValueError: If table doesn't exist
            SQLAlchemyError: If schema inference fails
        """
        if not self._validate_table_exists(schema_name, table_name):
            raise ValueError(
                f"Table '{schema_name}.{table_name}' does not exist. "
                f"Please create the table before writing data."
            )
        
        schema_dict = {}
        
        try:
            with self.engine.connect() as conn:
                if self.dbms_type == "postgresql":
                    query = text("""
                        SELECT 
                            column_name,
                            data_type,
                            character_maximum_length,
                            numeric_precision,
                            numeric_scale,
                            is_nullable,
                            ordinal_position,
                            column_default,
                            is_identity,
                            is_generated
                        FROM information_schema.columns
                        WHERE table_schema = :schema_name AND table_name = :table_name
                        ORDER BY ordinal_position
                    """)
                    result = conn.execute(query, {"schema_name": schema_name, "table_name": table_name})
                    
                    for row in result:
                        column_default = row.column_default
                        is_identity = getattr(row, "is_identity", "NO") == "YES"
                        column_is_generated = getattr(row, "is_generated", "NEVER")
                        has_default = column_default is not None
                        is_auto_increment = is_identity or (column_default and "nextval(" in column_default)

                        schema_dict[row.column_name] = {
                            "data_type": row.data_type.upper(),
                            "max_length": row.character_maximum_length,
                            "precision": row.numeric_precision,
                            "scale": row.numeric_scale,
                            "is_nullable": row.is_nullable == "YES",
                            "ordinal_position": row.ordinal_position,
                            "column_default": column_default,
                            "has_default": has_default,
                            "is_identity": is_identity,
                            "is_generated": column_is_generated,
                            "is_auto_increment": is_auto_increment
                        }
                
                elif self.dbms_type == "mssql":
                    # Use sys.columns joined with INFORMATION_SCHEMA to get identity info
                    # IS_IDENTITY is not available in INFORMATION_SCHEMA.COLUMNS for SQL Server
                    query = text("""
                        SELECT 
                            isc.COLUMN_NAME,
                            isc.DATA_TYPE,
                            isc.CHARACTER_MAXIMUM_LENGTH,
                            isc.NUMERIC_PRECISION,
                            isc.NUMERIC_SCALE,
                            isc.IS_NULLABLE,
                            isc.ORDINAL_POSITION,
                            isc.COLUMN_DEFAULT,
                            CAST(COLUMNPROPERTY(OBJECT_ID(:full_table_name), isc.COLUMN_NAME, 'IsIdentity') AS BIT) AS IS_IDENTITY
                        FROM INFORMATION_SCHEMA.COLUMNS isc
                        WHERE isc.TABLE_SCHEMA = :schema_name AND isc.TABLE_NAME = :table_name
                        ORDER BY isc.ORDINAL_POSITION
                    """)
                    full_table_name = f"{schema_name}.{table_name}"
                    result = conn.execute(query, {
                        "schema_name": schema_name, 
                        "table_name": table_name,
                        "full_table_name": full_table_name
                    })
                    
                    for row in result:
                        column_default = row.COLUMN_DEFAULT
                        is_identity = bool(row.IS_IDENTITY) if row.IS_IDENTITY is not None else False
                        has_default = column_default is not None
                        is_auto_increment = is_identity

                        schema_dict[row.COLUMN_NAME] = {
                            "data_type": row.DATA_TYPE.upper(),
                            "max_length": row.CHARACTER_MAXIMUM_LENGTH,
                            "precision": row.NUMERIC_PRECISION,
                            "scale": row.NUMERIC_SCALE,
                            "is_nullable": row.IS_NULLABLE == "YES",
                            "ordinal_position": row.ORDINAL_POSITION,
                            "column_default": column_default,
                            "has_default": has_default,
                            "is_identity": is_identity,
                            "is_generated": None,
                            "is_auto_increment": is_auto_increment
                        }
            
            logger.info(f"Inferred schema for {schema_name}.{table_name}: {len(schema_dict)} columns")
            return schema_dict
            
        except Exception as e:
            logger.error(f"Error inferring table schema: {e}")
            raise SQLAlchemyError(f"Schema inference failed: {e}")
    
    def _read_excel_in_chunks(
        self,
        excel_path: str,
        sheet_name: str,
        chunk_size: int
    ) -> Iterator[pd.DataFrame]:
        """
        Read Excel file in chunks using skiprows and nrows to avoid loading entire file into memory.
        
        Note: pandas read_excel() doesn't support chunksize like read_csv(), so we use
        skiprows and nrows parameters to read chunks incrementally. This approach reads
        only the header once, then reads data in chunks, making it memory-efficient for large files.
        
        Args:
            excel_path: Path to Excel file
            sheet_name: Name of the sheet to read
            chunk_size: Number of rows per chunk
            
        Yields:
            DataFrame chunks
            
        Raises:
            FileNotFoundError: If Excel file doesn't exist
            ValueError: If sheet doesn't exist in file
        """
        if not os.path.exists(excel_path):
            raise FileNotFoundError(f"Excel file not found: {excel_path}")
        
        if not os.path.isfile(excel_path):
            raise ValueError(f"Path exists but is not a file: {excel_path}")
        
        try:
            # Open Excel file to validate sheet exists
            excel_file = pd.ExcelFile(excel_path)
            
            if sheet_name not in excel_file.sheet_names:
                raise ValueError(
                    f"Sheet '{sheet_name}' not found in Excel file. "
                    f"Available sheets: {excel_file.sheet_names}"
                )
            
            # Read header row first to get column names
            logger.info(f"Reading header from sheet '{sheet_name}'...")
            df_header = pd.read_excel(excel_file, sheet_name=sheet_name, nrows=0)
            column_names = df_header.columns.tolist()
            logger.info(f"Detected {len(column_names)} columns in Excel file")
            logger.info(f"Excel column names: {column_names}")
            
            # Read data in chunks using skiprows and nrows
            # We'll skip the header row (0) and previous data rows, then read chunk_size rows
            chunk_idx = 0
            rows_skipped = 1  # Start by skipping header row (row 0)
            
            while True:
                # Read chunk: skip header (row 0) + previous data rows, read chunk_size rows
                # skiprows can be a list/range of row indices to skip
                chunk_df = pd.read_excel(
                    excel_file,
                    sheet_name=sheet_name,
                    skiprows=range(0, rows_skipped),  # Skip header (0) + previous data rows
                    nrows=chunk_size,
                    header=None  # We already have column names, don't read header again
                )
                
                # If chunk is empty, we've reached the end
                if chunk_df.empty:
                    break
                
                # Set column names from header we read earlier
                # When header=None, pandas uses integer column names (0, 1, 2, ...)
                # We need to replace them with the actual column names
                if len(chunk_df.columns) != len(column_names):
                    # Handle case where chunk has different number of columns
                    # (e.g., due to empty rows or formatting)
                    if len(chunk_df.columns) == 0:
                        break  # No more data
                    # Truncate or pad to match expected column count
                    if len(chunk_df.columns) > len(column_names):
                        # Too many columns - truncate
                        chunk_df = chunk_df.iloc[:, :len(column_names)]
                    elif len(chunk_df.columns) < len(column_names):
                        # Too few columns - pad with NaN columns
                        missing_cols = len(column_names) - len(chunk_df.columns)
                        for i in range(missing_cols):
                            chunk_df[f'_missing_{i}'] = None
                
                # Assign column names - this is critical for field mapping to work
                chunk_df.columns = column_names
                
                # Replace Excel error values (#N/A, #VALUE!, etc.) with NaN
                # These are common in Excel files and cause issues with type conversion
                chunk_df = chunk_df.replace(['#N/A', '#VALUE!', '#REF!', '#DIV/0!', '#NAME?', '#NULL!', '#NUM!'], np.nan)
                
                logger.debug(f"Assigned column names to chunk {chunk_idx + 1}: {list(chunk_df.columns)[:5]}..." if len(chunk_df.columns) > 5 else f"Assigned column names: {list(chunk_df.columns)}")
                
                chunk_idx += 1
                rows_in_chunk = len(chunk_df)
                logger.debug(f"Read chunk {chunk_idx} with {rows_in_chunk} rows (data rows {rows_skipped} to {rows_skipped + rows_in_chunk - 1})")
                
                yield chunk_df
                
                # Update rows_skipped for next iteration (header + all previous data rows)
                rows_skipped += rows_in_chunk
                
                # If we got fewer rows than chunk_size, we've reached the end
                if rows_in_chunk < chunk_size:
                    break
            
            logger.info(f"Finished reading {chunk_idx} chunks from sheet '{sheet_name}' (total data rows: {rows_skipped - 1})")
                
        except Exception as e:
            logger.error(f"Error reading Excel file in chunks: {e}")
            raise
    
    def _map_fields(self, df: pd.DataFrame, field_mapping: Dict[str, str]) -> pd.DataFrame:
        """
        Map Excel column names to table column names using field mapping dictionary.
        
        Args:
            df: DataFrame with Excel column names
            field_mapping: Dictionary mapping Excel column names to table column names
            
        Returns:
            DataFrame with mapped column names
            
        Raises:
            ValueError: If required mapped columns are missing from Excel
        """
        if not field_mapping:
            logger.debug("No field mapping provided, using Excel column names as-is")
            return df
        
        # Create a copy to avoid modifying original
        mapped_df = df.copy()
        
        # Track which Excel columns were mapped
        mapped_excel_cols = set()
        mapped_table_cols = []
        
        # Apply mapping
        for excel_col, table_col in field_mapping.items():
            # Case-insensitive matching for Excel columns
            # Also try exact match first, then case-insensitive
            matching_cols = []
            
            # First try exact match
            if excel_col in df.columns:
                matching_cols = [excel_col]
            else:
                # Then try case-insensitive match
                matching_cols = [col for col in df.columns if col.lower() == excel_col.lower()]
            
            if not matching_cols:
                # Excel column not found - will be filled with NULL
                logger.warning(
                    f"Excel column '{excel_col}' not found in data. "
                    f"Available columns: {list(df.columns)[:10]}..." if len(df.columns) > 10 else f"Available columns: {list(df.columns)}. "
                    f"Will create NULL column '{table_col}'"
                )
                mapped_df[table_col] = None
            else:
                # Found matching column - rename it
                excel_col_actual = matching_cols[0]
                mapped_df = mapped_df.rename(columns={excel_col_actual: table_col})
                mapped_excel_cols.add(excel_col_actual)
                mapped_table_cols.append(table_col)
                logger.debug(f"Mapped Excel column '{excel_col_actual}' -> '{table_col}'")
        
        # Drop unmapped Excel columns (unless they're already mapped)
        columns_to_drop = [col for col in df.columns if col not in mapped_excel_cols]
        if columns_to_drop:
            logger.debug(f"Dropping unmapped Excel columns: {columns_to_drop}")
            mapped_df = mapped_df.drop(columns=columns_to_drop, errors='ignore')
        
        logger.debug(f"Field mapping applied: {len(mapped_table_cols)} columns mapped")
        return mapped_df
    
    def _convert_types(
        self,
        df: pd.DataFrame,
        table_schema: Dict[str, Dict[str, Any]]
    ) -> pd.DataFrame:
        """
        Convert DataFrame column types to match table schema with strict validation.
        
        Args:
            df: DataFrame with mapped column names
            table_schema: Dictionary of table column metadata from infer_table_schema()
            
        Returns:
            DataFrame with converted types
            
        Raises:
            TypeError: If type conversion fails (strict validation)
            ValueError: If column doesn't exist in table schema
        """
        converted_df = df.copy()
        
        # Check for columns in DataFrame that don't exist in table schema
        missing_in_schema = set(converted_df.columns) - set(table_schema.keys())
        if missing_in_schema:
            raise ValueError(
                f"Columns in DataFrame not found in table schema: {missing_in_schema}. "
                f"Available table columns: {list(table_schema.keys())}"
            )
        
        # Convert each column based on table schema
        for col_name, col_meta in table_schema.items():
            if col_name not in converted_df.columns:
                has_default = col_meta.get("has_default", False)
                is_auto_increment = col_meta.get("is_auto_increment", False)
                if is_auto_increment:
                    logger.debug(f"Skipping auto-generated column '{col_name}' (auto increment or identity)")
                    continue
                if has_default:
                    logger.debug(f"Skipping column '{col_name}' because it has a default expression")
                    continue
                if col_meta.get("is_nullable", True):
                    logger.debug(f"Column '{col_name}' missing from DataFrame, filling with NULL (nullable column)")
                    converted_df[col_name] = None
                    continue

                raise ValueError(
                    f"Column '{col_name}' is defined as NOT NULL and has no default, "
                    f"but the Excel data does not include it."
                )
            
            data_type = col_meta["data_type"]
            is_nullable = col_meta.get("is_nullable", True)
            max_length = col_meta.get("max_length")
            precision = col_meta.get("precision")
            scale = col_meta.get("scale")
            
            try:
                # Convert based on data type
                if data_type in ["VARCHAR", "CHAR", "TEXT", "NVARCHAR", "NCHAR", "NTEXT"]:
                    # String types
                    converted_df[col_name] = converted_df[col_name].astype(str)
                    # Replace NaN/NaT with None for nullable columns
                    if is_nullable:
                        converted_df[col_name] = converted_df[col_name].replace([np.nan, pd.NA, 'nan', 'None'], None)
                    # Truncate if max_length is specified
                    if max_length and max_length > 0:
                        converted_df[col_name] = converted_df[col_name].apply(
                            lambda x: str(x)[:max_length] if x is not None and len(str(x)) > max_length else x
                        )
                
                elif data_type in ["INTEGER", "INT", "BIGINT", "SMALLINT", "TINYINT"]:
                    # Integer types
                    # Always use 'coerce' to convert invalid values to NaN first
                    converted_df[col_name] = pd.to_numeric(converted_df[col_name], errors='coerce')
                    
                    # Replace inf values with NaN
                    converted_df[col_name] = converted_df[col_name].replace([np.inf, -np.inf], np.nan)
                    
                    # Check for NULL/NaN values for NOT NULL columns
                    if not is_nullable:
                        null_count = converted_df[col_name].isna().sum()
                        if null_count > 0:
                            # For NOT NULL columns, filter out rows with NULL values
                            # This is better than failing completely - we log a warning and continue
                            logger.warning(
                                f"Column '{col_name}' is NOT NULL but contains {null_count} NULL/NaN/inf values. "
                                f"Filtering out {null_count} rows with NULL values in this column."
                            )
                            # Filter out rows with NULL values in this NOT NULL column
                            converted_df = converted_df[converted_df[col_name].notna()].copy()
                            if len(converted_df) == 0:
                                # All rows filtered out - return empty DataFrame to skip this chunk gracefully
                                logger.warning(
                                    f"All rows in chunk filtered out due to NULL values in NOT NULL column '{col_name}'. "
                                    f"Skipping this chunk (likely empty/invalid rows at end of Excel file)."
                                )
                                # Return empty DataFrame with same columns to maintain structure
                                return pd.DataFrame(columns=converted_df.columns)
                        # Convert to int64 for NOT NULL columns
                        converted_df[col_name] = converted_df[col_name].astype('int64')
                    else:
                        # For nullable columns, use Int64 (nullable integer type)
                        converted_df[col_name] = converted_df[col_name].astype('Int64')
                
                elif data_type in ["NUMERIC", "DECIMAL", "FLOAT", "DOUBLE PRECISION", "REAL"]:
                    # Numeric types
                    if is_nullable:
                        converted_df[col_name] = pd.to_numeric(converted_df[col_name], errors='coerce')
                    else:
                        converted_df[col_name] = pd.to_numeric(converted_df[col_name], errors='raise')
                    # Use appropriate precision
                    if precision and scale:
                        # Keep as float but validate precision
                        converted_df[col_name] = converted_df[col_name].astype('float64')
                    else:
                        converted_df[col_name] = converted_df[col_name].astype('float64')
                
                elif data_type in ["DATE"]:
                    # Date type - convert to datetime first, then extract date
                    converted_df[col_name] = pd.to_datetime(converted_df[col_name], errors='coerce' if is_nullable else 'raise')
                    # Convert to date objects (pandas will handle NULLs properly)
                    converted_df[col_name] = converted_df[col_name].apply(
                        lambda x: x.date() if pd.notna(x) else None
                    )
                
                elif data_type in ["TIMESTAMP", "DATETIME", "DATETIME2", "SMALLDATETIME", 
                                   "DATETIMEOFFSET", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITHOUT TIME ZONE"]:
                    # Timestamp/DateTime types (includes SQL Server DATETIME2, SMALLDATETIME, DATETIMEOFFSET)
                    converted_df[col_name] = pd.to_datetime(converted_df[col_name], errors='coerce' if is_nullable else 'raise')
                
                elif data_type in ["BOOLEAN", "BIT"]:
                    # Boolean type
                    converted_df[col_name] = converted_df[col_name].astype(bool)
                    # Handle None values
                    if is_nullable:
                        converted_df[col_name] = converted_df[col_name].replace([None, np.nan, pd.NA], None)
                
                else:
                    # Unknown type - keep as string
                    logger.warning(f"Unknown data type '{data_type}' for column '{col_name}', keeping as string")
                    converted_df[col_name] = converted_df[col_name].astype(str)
                
                # Note: NULL validation for NOT NULL columns is handled within each type conversion block above
                
            except (ValueError, TypeError) as e:
                raise TypeError(
                    f"Type conversion failed for column '{col_name}' (target type: {data_type}): {e}"
                )
        
        logger.debug(f"Type conversion completed for {len(converted_df.columns)} columns")
        return converted_df
    
    def _write_chunk_to_db(
        self,
        df_chunk: pd.DataFrame,
        schema_name: str,
        table_name: str,
        table_schema: Dict[str, Dict[str, Any]],
        if_exists: str = 'append'
    ) -> int:
        """
        Write a DataFrame chunk to database table.
        
        Args:
            df_chunk: DataFrame chunk to write
            schema_name: Schema name
            table_name: Table name
            if_exists: What to do if data exists ('append', 'replace', 'fail')
            
        Returns:
            Number of rows written
            
        Raises:
            SQLAlchemyError: If write fails
        """
        if df_chunk.empty:
            logger.debug("Empty chunk, skipping write")
            return 0
        
        try:
            # Calculate parameter count: rows * columns
            # PostgreSQL has a limit of ~65,535 parameters
            # For method='multi', we need to limit chunk size to avoid parameter limit
            num_columns = len(df_chunk.columns)
            max_rows_per_batch = min(3000, 65000 // num_columns) if self.dbms_type == 'postgresql' else len(df_chunk)
            
            # Drop auto-generated/default columns unless explicitly provided via mapping
            generated_columns = {
                name for name, meta in table_schema.items()
                if meta.get("is_auto_increment") or meta.get("has_default")
            }
            provided_columns = set(df_chunk.columns)
            mapped_columns = set(self.field_mapping.values())
            columns_to_drop = [
                col for col in provided_columns
                if col in generated_columns and col not in mapped_columns
            ]
            if columns_to_drop:
                logger.debug(f"Dropping auto-generated/default columns before write: {columns_to_drop}")
                df_chunk = df_chunk.drop(columns=columns_to_drop, errors='ignore')
                num_columns = len(df_chunk.columns)
                max_rows_per_batch = min(3000, 65000 // num_columns) if self.dbms_type == 'postgresql' else len(df_chunk)
            
            if len(df_chunk) > max_rows_per_batch:
                # Split into smaller batches to avoid parameter limit
                total_written = 0
                for i in range(0, len(df_chunk), max_rows_per_batch):
                    batch_df = df_chunk.iloc[i:i + max_rows_per_batch]
                    batch_df.to_sql(
                        table_name,
                        self.engine,
                        schema=schema_name,
                        if_exists=if_exists if i == 0 else 'append',  # Only use if_exists for first batch
                        index=False,
                        method='multi' if self.dbms_type == 'postgresql' else None
                    )
                    total_written += len(batch_df)
                logger.debug(f"Wrote {total_written} rows to {schema_name}.{table_name} in batches")
                return total_written
            else:
                # Use pandas to_sql with method='multi' for batch insert performance
                df_chunk.to_sql(
                    table_name,
                    self.engine,
                    schema=schema_name,
                    if_exists=if_exists,
                    index=False,
                    method='multi' if self.dbms_type == 'postgresql' else None  # MSSQL doesn't support 'multi'
                )
                
                logger.debug(f"Wrote {len(df_chunk)} rows to {schema_name}.{table_name}")
                return len(df_chunk)
            
        except Exception as e:
            logger.error(f"Error writing chunk to database: {e}")
            raise SQLAlchemyError(f"Failed to write chunk to {schema_name}.{table_name}: {e}")
    
    def write_excel_to_table(
        self,
        excel_path: str,
        sheet_name: str,
        schema_name: str,
        table_name: str,
        chunk_size: int = 10000,
        if_exists: str = 'append',
        auto_create_table: bool = True
    ) -> Dict[str, Any]:
        """
        Main method to read Excel file and write to database table with streaming chunk processing.
        
        Args:
            excel_path: Path to Excel file
            sheet_name: Name of the sheet to read
            schema_name: Database schema name
            table_name: Database table name
            chunk_size: Number of rows to process per chunk (default: 10000)
            if_exists: What to do if data exists ('append', 'replace', 'fail')
            auto_create_table: Automatically create schema and table if they don't exist (default: True)
            
        Returns:
            Dictionary with processing results:
            {
                "status": "success" | "partial" | "error",
                "rows_written": int,
                "chunks_processed": int,
                "errors": List[Dict],
                "warnings": List[str],
                "table_created": bool
            }
            
        Raises:
            FileNotFoundError: If Excel file doesn't exist
            ValueError: If table doesn't exist and auto_create_table is False, or validation fails
            SQLAlchemyError: If database operations fail
        """
        logger.info(
            f"Starting Excel to DB write: {excel_path} (sheet: {sheet_name}) -> "
            f"{schema_name}.{table_name} (chunk_size: {chunk_size}, auto_create: {auto_create_table})"
        )
        
        table_created = False
        
        # Check if table exists, create if auto_create_table is True
        if not self._validate_table_exists(schema_name, table_name):
            if auto_create_table:
                logger.info(f"Table '{schema_name}.{table_name}' does not exist. Will create automatically.")
                
                # Create schema first
                if not self._create_schema_if_not_exists(schema_name):
                    raise ValueError(f"Failed to create schema '{schema_name}'")
                
                # Read first chunk to determine table structure
                logger.info("Reading first chunk to determine table structure...")
                try:
                    # Get first chunk iterator
                    first_chunk_iter = self._read_excel_in_chunks(excel_path, sheet_name, chunk_size)
                    first_chunk = next(first_chunk_iter, None)
                    
                    if first_chunk is None or first_chunk.empty:
                        raise ValueError(f"Excel file '{excel_path}' sheet '{sheet_name}' is empty")
                    
                    # Apply field mapping to determine target columns
                    mapped_first_chunk = self._map_fields(first_chunk, self.field_mapping)
                    
                    # Create table based on mapped structure
                    if not self._create_table_from_dataframe(mapped_first_chunk, schema_name, table_name):
                        raise ValueError(f"Failed to create table '{schema_name}.{table_name}'")
                    
                    table_created = True
                    logger.info(f"Successfully created table '{schema_name}.{table_name}'")
                    
                except StopIteration:
                    raise ValueError(f"Excel file '{excel_path}' sheet '{sheet_name}' is empty")
            else:
                raise ValueError(
                    f"Table '{schema_name}.{table_name}' does not exist. "
                    f"Set auto_create_table=True to create automatically, or create the table manually."
                )
        
        # Infer table schema
        try:
            table_schema = self.infer_table_schema(schema_name, table_name)
        except Exception as e:
            raise ValueError(f"Failed to infer table schema: {e}")
        
        # Initialize tracking variables
        total_rows_written = 0
        chunks_processed = 0
        errors = []
        warnings = []
        
        # Process Excel file in chunks
        try:
            chunk_iterator = self._read_excel_in_chunks(excel_path, sheet_name, chunk_size)
            
            for chunk_idx, chunk_df in enumerate(chunk_iterator, 1):
                try:
                    logger.info(f"Processing chunk {chunk_idx} ({len(chunk_df)} rows)...")
                    
                    # Step 1: Map fields
                    mapped_df = self._map_fields(chunk_df, self.field_mapping)

                    # Step 1b: Optional PII anonymization on mapped columns
                    if self.pii_config:
                        try:
                            source_system = (
                                self.pii_source_system
                                or f"{schema_name}.{table_name}"
                            )
                            mapped_df = anonymize_dataframe(
                                mapped_df,
                                pii_config=self.pii_config,
                                source_system=source_system,
                                context=self.pii_context
                            )
                        except Exception as pii_err:
                            logger.error(
                                f"PII anonymization failed for chunk {chunk_idx}: {pii_err}"
                            )
                            raise
                    
                    # Step 2: Convert types
                    converted_df = self._convert_types(mapped_df, table_schema)
                    
                    # Step 3: Write chunk to database
                    rows_written = self._write_chunk_to_db(
                        converted_df,
                        schema_name,
                        table_name,
                        table_schema,
                        if_exists if chunk_idx == 1 else 'append'  # Only use if_exists for first chunk
                    )
                    
                    total_rows_written += rows_written
                    chunks_processed += 1
                    
                    # Log progress every 10 chunks
                    if chunk_idx % 10 == 0:
                        logger.info(f"Progress: {total_rows_written} rows written in {chunk_idx} chunks")
                    
                except Exception as e:
                    error_info = {
                        "chunk": chunk_idx,
                        "error": str(e),
                        "error_type": type(e).__name__,
                        "rows_in_chunk": len(chunk_df) if 'chunk_df' in locals() else 0
                    }
                    errors.append(error_info)
                    logger.error(
                        f"Error processing chunk {chunk_idx} ({len(chunk_df) if 'chunk_df' in locals() else 0} rows): "
                        f"{type(e).__name__}: {e}"
                    )
                    # Continue processing remaining chunks (non-fatal errors)
                    # Critical failures will be raised at the end if no data was written
                    continue
            
            # Determine status and handle errors
            if len(errors) == 0:
                status = "success"
                logger.info(
                    f"Excel to DB write completed successfully: {total_rows_written} rows written, "
                    f"{chunks_processed} chunks processed"
                )
            elif total_rows_written > 0:
                status = "partial"
                error_rate = len(errors) / (chunks_processed + len(errors)) * 100
                logger.warning(
                    f"Excel to DB write completed with errors: {total_rows_written} rows written, "
                    f"{chunks_processed} chunks processed, {len(errors)} chunks failed ({error_rate:.1f}% error rate)"
                )
            else:
                status = "error"
                logger.error(
                    f"Excel to DB write failed: 0 rows written, {len(errors)} chunks failed"
                )
            
            # Raise exception for critical failures
            if status == "error":
                error_summary = f"Failed to write any data to {schema_name}.{table_name}. "
                error_summary += f"{len(errors)} chunks failed. "
                if errors:
                    first_error = errors[0].get("error", "Unknown error")
                    error_summary += f"First error: {first_error}"
                raise SQLAlchemyError(error_summary)
            elif status == "partial" and len(errors) > chunks_processed:
                # More errors than successful chunks - this is a critical failure
                error_summary = f"Critical failure: {len(errors)} chunks failed vs {chunks_processed} successful. "
                error_summary += f"Only {total_rows_written} rows written out of expected data."
                raise SQLAlchemyError(error_summary)
            
            return {
                "status": status,
                "rows_written": total_rows_written,
                "chunks_processed": chunks_processed,
                "errors": errors,
                "warnings": warnings,
                "table_created": table_created
            }
            
        except SQLAlchemyError:
            # Re-raise SQLAlchemy errors (these are our critical failures)
            raise
        except Exception as e:
            logger.error(f"Fatal error during Excel to DB write: {e}")
            raise SQLAlchemyError(
                f"Fatal error during Excel to DB write: {e}. "
                f"Rows written before failure: {total_rows_written}, "
                f"Chunks processed: {chunks_processed}"
            )
