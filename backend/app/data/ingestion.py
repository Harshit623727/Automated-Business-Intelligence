"""
Data Ingestion Module for Automated BI System
Handles CSV/Excel file loading with validation
"""
import pandas as pd
import io
import logging
from typing import Dict, Any, Tuple, Optional
from datetime import datetime
import numpy as np

logger = logging.getLogger(__name__)

class DataIngestion:
    """Handles loading and validating business data files"""
    
    # Expected schema for retail/e-commerce data
    EXPECTED_COLUMNS = {
        'InvoiceNo': 'object',
        'StockCode': 'object',
        'Description': 'object',
        'Quantity': 'float64',
        'InvoiceDate': 'datetime64[ns]',
        'UnitPrice': 'float64',
        'CustomerID': 'object',
        'Country': 'object'
    }
    
    def __init__(self):
        self.validation_errors = []
        self.validation_warnings = []
    
    def load_file(self, 
                  file_content: bytes, 
                  filename: str) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
        """
        Load file content into DataFrame
        
        Args:
            file_content: Raw bytes of uploaded file
            filename: Original filename for type detection
            
        Returns:
            Tuple of (DataFrame, metadata) or (None, error_info)
        """
        try:
            logger.info(f"Loading file: {filename} ({len(file_content)} bytes)")
            
            # Detect file type
            if filename.endswith('.csv'):
                df = self._load_csv(file_content)
            elif filename.endswith(('.xlsx', '.xls')):
                df = self._load_excel(file_content)
            else:
                return None, {"error": f"Unsupported file type: {filename}"}
            
            # Validate the loaded data
            validation_result = self.validate_dataframe(df)
            
            if not validation_result["is_valid"]:
                logger.warning(f"Validation failed: {validation_result['errors']}")
                return None, validation_result
            
            # Generate metadata
            metadata = {
                "filename": filename,
                "rows_loaded": len(df),
                "columns_loaded": len(df.columns),
                "columns": list(df.columns),
                "date_range": self._extract_date_range(df),
                "validation": validation_result,
                "timestamp": datetime.now().isoformat()
            }
            
            logger.info(f"Successfully loaded {len(df)} rows, {len(df.columns)} columns")
            return df, metadata
            
        except Exception as e:
            logger.error(f"Failed to load file: {str(e)}", exc_info=True)
            return None, {"error": f"File loading failed: {str(e)}"}
    
    def _load_csv(self, content: bytes) -> pd.DataFrame:
        """Load CSV file with intelligent parsing"""
        try:
            # Try UTF-8 first
            df = pd.read_csv(
                io.BytesIO(content),
                encoding='utf-8',
                parse_dates=['InvoiceDate'],
                dayfirst=True,
                dtype_backend='numpy_nullable'
            )
        except UnicodeDecodeError:
            # Fallback to latin-1
            df = pd.read_csv(
                io.BytesIO(content),
                encoding='latin-1',
                parse_dates=['InvoiceDate'],
                dayfirst=True,
                dtype_backend='numpy_nullable'
            )
        return df
    
    def _load_excel(self, content: bytes) -> pd.DataFrame:
        """Load Excel file"""
        df = pd.read_excel(
            io.BytesIO(content),
            parse_dates=['InvoiceDate']
        )
        return df
    
    def validate_dataframe(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate DataFrame structure and content
        
        Returns:
            Dictionary with validation results
        """
        errors = []
        warnings = []
        
        # Check for empty DataFrame
        if df.empty:
            errors.append("File contains no data")
            return {"is_valid": False, "errors": errors, "warnings": warnings}
        
        # Check required columns
        missing_columns = set(self.EXPECTED_COLUMNS.keys()) - set(df.columns)
        if missing_columns:
            errors.append(f"Missing required columns: {list(missing_columns)}")
        
        # Check numeric columns
        numeric_columns = ['Quantity', 'UnitPrice']
        for col in numeric_columns:
            if col in df.columns:
                if df[col].isnull().all():
                    errors.append(f"Column '{col}' is completely empty")
                elif not pd.api.types.is_numeric_dtype(df[col]):
                    warnings.append(f"Column '{col}' is not numeric type")
        
        # Check date column
        if 'InvoiceDate' in df.columns:
            if df['InvoiceDate'].isnull().all():
                errors.append("InvoiceDate column is completely empty")
        
        # Check for duplicates
        duplicate_count = df.duplicated().sum()
        if duplicate_count > 0:
            warnings.append(f"Found {duplicate_count} duplicate rows")
        
        # Check for negative values
        if 'Quantity' in df.columns:
            negative_qty = (df['Quantity'] < 0).sum()
            if negative_qty > 0:
                warnings.append(f"Found {negative_qty} rows with negative quantities")
        
        if 'UnitPrice' in df.columns:
            negative_price = (df['UnitPrice'] < 0).sum()
            if negative_price > 0:
                errors.append(f"Found {negative_price} rows with negative prices")
        
        return {
            "is_valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
            "duplicate_rows": int(duplicate_count),
            "missing_columns": list(missing_columns) if missing_columns else []
        }
    
    def _extract_date_range(self, df: pd.DataFrame) -> Dict[str, Optional[str]]:
        """Extract date range from dataframe"""
        if 'InvoiceDate' not in df.columns or df['InvoiceDate'].isnull().all():
            return {"min": None, "max": None, "days": 0}
        
        try:
            min_date = df['InvoiceDate'].min()
            max_date = df['InvoiceDate'].max()
            days = (max_date - min_date).days if pd.notna(min_date) and pd.notna(max_date) else 0
            
            return {
                "min": min_date.strftime('%Y-%m-%d') if pd.notna(min_date) else None,
                "max": max_date.strftime('%Y-%m-%d') if pd.notna(max_date) else None,
                "days": int(days)
            }
        except Exception as e:
            logger.error("Date range extraction failed", exc_info=True)
            return {"min": None, "max": None, "days": 0}
    
    def generate_sample_data(self, n_rows: int = 10000) -> pd.DataFrame:
        """
        Generate realistic retail sample data for testing
        
        Args:
            n_rows: Number of rows to generate
            
        Returns:
            DataFrame with realistic retail transaction data
        """
        np.random.seed(42)
        
        # Product catalog
        products = [
            ("85123A", "WHITE HANGING HEART T-LIGHT HOLDER", 2.55),
            ("71053", "WHITE METAL LANTERN", 3.39),
            ("84406B", "CREAM CUPID HEARTS COAT HANGER", 2.75),
            ("84029G", "KNITTED UNION FLAG HOT WATER BOTTLE", 3.75),
            ("84030E", "KNITTED UNION FLAG HAT", 2.95),
            ("84879", "ASSORTED COLOUR BIRD ORNAMENT", 1.69),
            ("22745", "POPPY'S PLAYHOUSE BEDROOM", 2.10),
            ("22746", "POPPY'S PLAYHOUSE KITCHEN", 2.10),
            ("22747", "POPPY'S PLAYHOUSE LIVING ROOM", 2.10),
            ("22748", "POPPY'S PLAYHOUSE BATHROOM", 2.10),
            ("22749", "POTTERING BENCH", 2.10),
            ("22750", "FELT TREE TRUNK", 2.10)
        ]
        
        countries = ['United Kingdom', 'Germany', 'France', 'Spain', 'Italy', 'USA', 'Australia']
        customers = [f'CUST{str(i).zfill(5)}' for i in range(1, 501)]
        
        # Generate dates for 1 year
        dates = pd.date_range(start='2023-01-01', end='2023-12-31', periods=n_rows)
        
        data = {
            'InvoiceNo': [f'INV{str(i).zfill(7)}' for i in range(n_rows)],
            'StockCode': [np.random.choice([p[0] for p in products]) for _ in range(n_rows)],
            'Description': [np.random.choice([p[1] for p in products]) for _ in range(n_rows)],
            'Quantity': np.random.randint(1, 20, n_rows),
            'InvoiceDate': dates,
            'UnitPrice': [np.random.choice([p[2] for p in products]) for _ in range(n_rows)],
            'CustomerID': np.random.choice(customers, n_rows),
            'Country': np.random.choice(countries, n_rows)
        }
        
        # Add some returns (negative quantities)
        return_indices = np.random.choice(n_rows, size=int(n_rows * 0.02), replace=False)
        for idx in return_indices:
            data['Quantity'][idx] = -data['Quantity'][idx]
        
        # Add some missing customer IDs
        missing_indices = np.random.choice(n_rows, size=int(n_rows * 0.05), replace=False)
        for idx in missing_indices:
            data['CustomerID'][idx] = None
        
        df = pd.DataFrame(data)
        return df

# Singleton instance for application-wide use
data_ingestion = DataIngestion()