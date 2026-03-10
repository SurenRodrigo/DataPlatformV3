
import io
import os
import pandas as pd
from typing import Any, Dict, Optional


class ExcelReader:
    """Utility to read Excel files from different input types.

    Accepts:
    - bytes or bytearray
    - io.BytesIO
    - file-like objects exposing `.read()`
    - objects with `.content` or `.value` attributes (ClientResult-like)
    - local file path (str)
    """

    def __init__(self):
        pass

    def _to_bytesio(self, src: Any) -> io.BytesIO:
        """Convert various input types to io.BytesIO.

        Raises TypeError if unable to convert.
        """
        # Local file path
        if isinstance(src, str) and os.path.exists(src):
            with open(src, 'rb') as f:
                return io.BytesIO(f.read())

        # Already a BytesIO
        if isinstance(src, io.BytesIO):
            src.seek(0)
            return src

        # Raw bytes
        if isinstance(src, (bytes, bytearray)):
            return io.BytesIO(bytes(src))

        # File-like with read()
        read_m = getattr(src, 'read', None)
        if callable(read_m):
            try:
                data = read_m()
                if isinstance(data, (bytes, bytearray)):
                    return io.BytesIO(bytes(data))
            except Exception:
                # fall through to attribute checks
                pass

        # ClientResult-like: try common attributes
        for attr in ('content', 'value', 'data', 'raw'):
            if hasattr(src, attr):
                val = getattr(src, attr)
                # If attribute is callable (e.g., a method), try calling it
                if callable(val):
                    try:
                        val = val()
                    except Exception:
                        continue
                if isinstance(val, (bytes, bytearray)):
                    return io.BytesIO(bytes(val))
                if isinstance(val, io.BytesIO):
                    val.seek(0)
                    return val

        # Last attempt: try to coerce to bytes
        try:
            b = bytes(src)
            return io.BytesIO(b)
        except Exception:
            raise TypeError(f"Unable to convert object of type {type(src)} to bytes for Excel reading")

    def read_sheet(self, file_bytes: Any, sheet_Name: Optional[str] = None) -> pd.DataFrame:
        """Read a single sheet (default: first sheet).

        file_bytes can be any supported input described in the class docstring.
        """
        bio = self._to_bytesio(file_bytes)
        bio.seek(0)
        return pd.read_excel(bio, sheet_name=sheet_Name)

    def read_all_sheets(self, file_bytes: Any, sheets: Optional[list] = None) -> Dict[str, pd.DataFrame]:
        """Read all sheets or specific sheets and return a dict of DataFrames."""
        bio = self._to_bytesio(file_bytes)
        bio.seek(0)

        excel = pd.ExcelFile(bio)
        if sheets is None:
            sheets = excel.sheet_names

        df_dict: Dict[str, pd.DataFrame] = {}
        for sh in sheets:
            # Pandas can accept the ExcelFile object directly
            df_dict[sh] = pd.read_excel(excel, sheet_name=sh)

        print(f" Loaded sheets: {list(df_dict.keys())}")
        return df_dict

    def read_sheet_from_path(self, file_path: str, sheet_name: Optional[str] = None) -> pd.DataFrame:
        """Read a single sheet from an Excel file located at the given file path.

        Args:
            file_path: Local file system path to the Excel file (e.g., "/path/to/file.xlsx")
            sheet_name: Name of the sheet to read. If None, reads the first sheet.
            
        Returns:
            pd.DataFrame: DataFrame containing the sheet data
            
        Raises:
            FileNotFoundError: If file_path doesn't exist
            IOError: If file cannot be read
            ValueError: If sheet_name doesn't exist in the file
            TypeError: If file_path is not a string
            
        Examples:
            # Read a specific sheet from local file
            df = reader.read_sheet_from_path("/path/to/file.xlsx", sheet_name="Sheet1")
            
            # Read first sheet from local file
            df = reader.read_sheet_from_path("/path/to/file.xlsx")
        """
        if not isinstance(file_path, str):
            raise TypeError(f"file_path must be a string, got {type(file_path)}")
        
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found at path: '{file_path}'")
        
        if not os.path.isfile(file_path):
            raise ValueError(f"Path exists but is not a file: '{file_path}'")
        
        try:
            with open(file_path, 'rb') as f:
                file_bytes = io.BytesIO(f.read())
            return self.read_sheet(file_bytes, sheet_name)
        except IOError as e:
            raise IOError(f"Failed to read file at path '{file_path}': {e}")

    def read_all_sheets_from_path(self, file_path: str, sheets: Optional[list] = None) -> Dict[str, pd.DataFrame]:
        """Read all sheets or specific sheets from an Excel file located at the given file path.

        Args:
            file_path: Local file system path to the Excel file (e.g., "/path/to/file.xlsx")
            sheets: Optional list of sheet names to read. If None, reads all sheets.
            
        Returns:
            Dict[str, pd.DataFrame]: Dictionary mapping sheet names to DataFrames
            
        Raises:
            FileNotFoundError: If file_path doesn't exist
            IOError: If file cannot be read
            ValueError: If any sheet name in sheets list doesn't exist in the file
            TypeError: If file_path is not a string
            
        Examples:
            # Read all sheets from local file path
            all_sheets = reader.read_all_sheets_from_path("/path/to/file.xlsx")
            
            # Read specific sheets from local file path
            sheets = reader.read_all_sheets_from_path("/path/to/file.xlsx", sheets=["Sheet1", "Sheet2"])
        """
        if not isinstance(file_path, str):
            raise TypeError(f"file_path must be a string, got {type(file_path)}")
        
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found at path: '{file_path}'")
        
        if not os.path.isfile(file_path):
            raise ValueError(f"Path exists but is not a file: '{file_path}'")
        
        try:
            with open(file_path, 'rb') as f:
                file_bytes = io.BytesIO(f.read())
            return self.read_all_sheets(file_bytes, sheets)
        except IOError as e:
            raise IOError(f"Failed to read file at path '{file_path}': {e}")

    def extract_tables_by_blank_columns(self, df: pd.DataFrame, sheet_name: str = "") -> Dict[str, pd.DataFrame]:
        """
        Split sheet into tables separated by sequences of 'Unnamed:X' columns.
        Tables are split ONLY when encountering columns named 'Unnamed:X',
        even if other columns have all empty/null values.
        
        Returns {sheet_name_table_1: df1, sheet_name_table_2: df2, ...}
        """
        # Get list of column indices and names
        cols = list(range(df.shape[1]))
        col_names = df.columns.tolist()
        
        # Find sequences of Unnamed columns (they mark table boundaries)
        blank_sequences = []
        start_blank = None
        for i, col_name in enumerate(col_names):
            is_unnamed = isinstance(col_name, str) and col_name.startswith('Unnamed:')
            
            if is_unnamed and start_blank is None:
                start_blank = i
            elif not is_unnamed and start_blank is not None:
                blank_sequences.append((start_blank, i-1))
                start_blank = None
        
        if start_blank is not None:  # Handle trailing Unnamed columns
            blank_sequences.append((start_blank, len(col_names)-1))
        
        # If no Unnamed columns found, return the entire DataFrame as one table
        if not blank_sequences:
            tables = {f"{sheet_name}_table_1": df.copy()}
            return tables
            
        # Use Unnamed sequences to identify table boundaries
        table_boundaries = []
        last_end = 0
        for start_blank, end_blank in blank_sequences:
            if start_blank > last_end:  # There's a table before these Unnamed columns
                table_boundaries.append((last_end, start_blank))
            last_end = end_blank + 1
            
        if last_end < len(col_names):  # Add final table if it exists
            table_boundaries.append((last_end, len(col_names)))
            
        # Extract and clean tables
        tables = {}
        for i, (start, end) in enumerate(table_boundaries, 1):
            if end > start:  # Valid table range
                # Extract table
                table = df.iloc[:, start:end].copy()
                
                # Clean column names (remove .1, .2 suffixes if they're duplicates)
                clean_cols = []
                col_counts = {}
                # First pass: count base names
                for col in table.columns:
                    if isinstance(col, str):
                        base = col.split('.')[0]
                        col_counts[base] = col_counts.get(base, 0) + 1
                
                # Second pass: clean names
                for col in table.columns:
                    if isinstance(col, str):
                        base = col.split('.')[0]
                        if col_counts[base] == 1:  # Only one column with this base
                            clean_cols.append(base)  # Remove any suffix
                        else:
                            clean_cols.append(col)  # Keep full name to avoid duplicates
                    else:
                        clean_cols.append(col)
                
                table.columns = clean_cols
                
                # Drop completely blank rows
                table = table.dropna(how='all')
                
                if len(table) > 0:  # Only include non-empty tables
                    table_name = f"{sheet_name}_table_{i}"
                    tables[table_name] = table.reset_index(drop=True)
                    
        return tables

