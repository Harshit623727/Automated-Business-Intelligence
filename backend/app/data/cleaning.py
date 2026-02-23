"""
Data Cleaning Module for Automated BI System
Handles null values, type conversion, and data standardization
"""
import pandas as pd
import numpy as np
import logging
from typing import Dict, Any, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)

class DataCleaner:
    """Cleans and prepares raw business data for analysis"""
    
    def __init__(self):
        self.cleaning_stats = {}
    
    def clean_dataset(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Apply comprehensive cleaning pipeline
        
        Args:
            df: Raw DataFrame to clean
            
        Returns:
            Tuple of (cleaned DataFrame, cleaning report)
        """
        original_shape = df.shape
        logger.info(f"Starting cleaning: {original_shape[0]} rows, {original_shape[1]} columns")
        
        # Initialize cleaning report
        cleaning_report = {
            "original_rows": int(original_shape[0]),
            "original_columns": int(original_shape[1]),
            "cleaning_steps": [],
            "warnings": [],
            "timestamp": datetime.now().isoformat()
        }
        
        # Create working copy
        cleaned_df = df.copy()
        
        # Step 1: Handle missing values
        cleaned_df, null_stats = self._handle_missing_values(cleaned_df)
        cleaning_report["cleaning_steps"].append({
            "step": "missing_value_handling",
            "stats": null_stats
        })
        
        # Step 2: Fix data types
        cleaned_df, type_stats = self._fix_data_types(cleaned_df)
        cleaning_report["cleaning_steps"].append({
            "step": "type_conversion",
            "stats": type_stats
        })
        
        # Step 3: Remove invalid rows
        cleaned_df, removal_stats = self._remove_invalid_rows(cleaned_df)
        cleaning_report["cleaning_steps"].append({
            "step": "invalid_row_removal",
            "stats": removal_stats
        })
        
        # Step 4: Standardize text columns
        cleaned_df, text_stats = self._standardize_text(cleaned_df)
        cleaning_report["cleaning_steps"].append({
            "step": "text_standardization",
            "stats": text_stats
        })
        
        # Step 5: Create derived columns
        cleaned_df, derived_stats = self._create_derived_columns(cleaned_df)
        cleaning_report["cleaning_steps"].append({
            "step": "derived_columns",
            "stats": derived_stats
        })
        
        # Step 6: Remove duplicates
        cleaned_df, duplicate_stats = self._remove_duplicates(cleaned_df)
        cleaning_report["cleaning_steps"].append({
            "step": "duplicate_removal",
            "stats": duplicate_stats
        })
        
        # Final statistics
        cleaning_report.update({
            "final_rows": int(len(cleaned_df)),
            "final_columns": int(len(cleaned_df.columns)),
            "rows_removed": int(original_shape[0] - len(cleaned_df)),
            "rows_removed_percentage": round((1 - len(cleaned_df)/original_shape[0]) * 100, 2) if original_shape[0] > 0 else 0,
            "cleaning_completed": True
        })
        
        logger.info(f"Cleaning completed. Removed {cleaning_report['rows_removed']} rows")
        
        return cleaned_df, cleaning_report
    
    def _handle_missing_values(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """Handle null values appropriately per column"""
        stats = {}
        
        # CustomerID - fill with 'Unknown'
        if 'CustomerID' in df.columns:
            missing = df['CustomerID'].isnull().sum()
            if missing > 0:
                df['CustomerID'] = df['CustomerID'].fillna('Unknown')
                stats['CustomerID_filled'] = int(missing)
        
        # Description - fill with generic
        if 'Description' in df.columns:
            missing = df['Description'].isnull().sum()
            if missing > 0:
                df['Description'] = df['Description'].fillna('Unknown Product')
                stats['Description_filled'] = int(missing)
        
        # Country - fill with mode
        if 'Country' in df.columns:
            missing = df['Country'].isnull().sum()
            if missing > 0:
                if df['Country'].notnull().any():
                    mode_country = df['Country'].mode()[0]
                else:
                    mode_country = 'Unknown'
                df['Country'] = df['Country'].fillna(mode_country)
                stats['Country_filled'] = int(missing)
        
        # Numeric columns - fill with 0
        for col in ['Quantity', 'UnitPrice']:
            if col in df.columns:
                missing = df[col].isnull().sum()
                if missing > 0:
                    df[col] = df[col].fillna(0)
                    stats[f'{col}_filled'] = int(missing)
        
        return df, stats
    
    def _fix_data_types(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """Ensure correct data types"""
        stats = {}
        
        # Convert to string
        for col in ['InvoiceNo', 'StockCode', 'CustomerID', 'Country']:
            if col in df.columns:
                df[col] = df[col].astype(str)
                stats[f'{col}_converted'] = "string"
        
        # Convert to numeric
        for col in ['Quantity', 'UnitPrice']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].fillna(0).astype(float)
                stats[f'{col}_converted'] = "float"
        
        # Convert to datetime
        if 'InvoiceDate' in df.columns:
            df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], errors='coerce')
            invalid_dates = df['InvoiceDate'].isnull().sum()
            if invalid_dates > 0:
                stats['invalid_dates_found'] = int(invalid_dates)
        
        return df, stats
    
    def _remove_invalid_rows(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """Remove rows that are invalid for business analysis"""
        initial_count = len(df)
        removal_reasons = {}
        
        # Remove rows with null InvoiceDate
        if 'InvoiceDate' in df.columns:
            invalid_dates = df['InvoiceDate'].isnull()
            if invalid_dates.any():
                df = df.dropna(subset=['InvoiceDate'])
                removal_reasons['invalid_dates'] = int(invalid_dates.sum())
        
        # Remove rows with zero or negative UnitPrice (data errors)
        if 'UnitPrice' in df.columns:
            invalid_prices = df['UnitPrice'] <= 0
            if invalid_prices.any():
                df = df[~invalid_prices]
                removal_reasons['invalid_prices'] = int(invalid_prices.sum())
        
        # Remove rows with zero Quantity (cancelled orders)
        if 'Quantity' in df.columns:
            zero_qty = df['Quantity'] == 0
            if zero_qty.any():
                df = df[~zero_qty]
                removal_reasons['zero_quantity'] = int(zero_qty.sum())
        
        removal_reasons['total_removed'] = int(initial_count - len(df))
        
        return df, removal_reasons
    
    def _standardize_text(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """Standardize text columns for consistency"""
        stats = {}
        
        if 'Country' in df.columns:
            df['Country'] = df['Country'].str.strip().str.title()
            stats['Country_standardized'] = True
        
        if 'Description' in df.columns:
            df['Description'] = df['Description'].str.strip()
            stats['Description_standardized'] = True
        
        return df, stats
    
    def _create_derived_columns(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """Create calculated columns with proper business logic"""
        stats = {}
        
        # =================================================================
        # REVENUE CALCULATION - FIXED
        # =================================================================
        if 'Quantity' in df.columns and 'UnitPrice' in df.columns:
            # Raw transaction value (can be negative)
            df['TransactionValue'] = df['Quantity'] * df['UnitPrice']
            
            # Separate sales from returns
            df['IsReturn'] = df['Quantity'] < 0
            
            # Sales revenue (positive only)
            df['SalesRevenue'] = df.apply(
                lambda row: row['TransactionValue'] if row['Quantity'] > 0 else 0,
                axis=1
            )
            
            # Returns value (absolute value for reporting)
            df['ReturnsValue'] = df.apply(
                lambda row: abs(row['TransactionValue']) if row['Quantity'] < 0 else 0,
                axis=1
            )
            
            # Net revenue (sales minus returns) - THIS is what matters
            df['NetRevenue'] = df['SalesRevenue'] - df['ReturnsValue']
            
            # For backward compatibility with other modules
            df['Revenue'] = df['NetRevenue']
            
            # Calculate return rate
            total_sales = df['SalesRevenue'].sum()
            total_returns = df['ReturnsValue'].sum()
            
            stats.update({
                'gross_sales': float(total_sales),
                'total_returns': float(total_returns),
                'net_revenue': float(total_sales - total_returns),
                'return_rate': round(
                    (total_returns / total_sales * 100) if total_sales > 0 else 0, 2
                )
            })
        
        # =================================================================
        # DATE COMPONENTS
        # =================================================================
        if 'InvoiceDate' in df.columns:
            df['Year'] = df['InvoiceDate'].dt.year
            df['Month'] = df['InvoiceDate'].dt.month
            df['MonthName'] = df['InvoiceDate'].dt.strftime('%B')
            df['Quarter'] = df['InvoiceDate'].dt.quarter
            df['Weekday'] = df['InvoiceDate'].dt.day_name()
            df['DayOfMonth'] = df['InvoiceDate'].dt.day
            df['Hour'] = df['InvoiceDate'].dt.hour
            df['Date'] = df['InvoiceDate'].dt.date
            
            stats['date_components_created'] = True
        
        # =================================================================
        # TRANSACTION ID - DON'T USE FOR DEDUP!
        # =================================================================
        if 'InvoiceNo' in df.columns and 'StockCode' in df.columns:
            # This is for reference only, NOT for deduplication
            df['TransactionRef'] = df['InvoiceNo'] + '_' + df['StockCode']
            stats['transaction_ref_created'] = True
        
        # =================================================================
        # DATA QUALITY FLAGS
        # =================================================================
        df['DataQualityScore'] = 1.0  # Start with perfect score
        
        # Flag suspicious entries but don't remove them
        if 'Quantity' in df.columns:
            # Unusually high quantities (potential data errors)
            qty_99th = df['Quantity'].quantile(0.99)
            df.loc[df['Quantity'] > qty_99th * 3, 'DataQualityScore'] *= 0.7
            df.loc[df['Quantity'] > qty_99th * 3, 'QualityFlag'] = 'Extreme quantity'
        
        if 'UnitPrice' in df.columns:
            # Unusually high prices
            price_99th = df['UnitPrice'].quantile(0.99)
            df.loc[df['UnitPrice'] > price_99th * 3, 'DataQualityScore'] *= 0.7
            df.loc[df['UnitPrice'] > price_99th * 3, 'QualityFlag'] = 'Extreme price'
        
        return df, stats
    
    def _remove_duplicates(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        Intelligent duplicate detection:
        - Removes EXACT duplicates (data entry errors)
        - Preserves legitimate multiple entries
        - Flags suspicious patterns for review
        """
        initial_count = len(df)
        stats = {}
        
        # =================================================================
        # STEP 1: Remove EXACT duplicates (all columns identical)
        # These are always safe to remove - they're data entry errors
        # =================================================================
        exact_duplicates = df.duplicated(keep='first')
        exact_dup_count = exact_duplicates.sum()
        
        if exact_dup_count > 0:
            df = df[~exact_duplicates]
            stats['exact_duplicates_removed'] = int(exact_dup_count)
            logger.info(f"Removed {exact_dup_count} exact duplicate rows")
        
        # =================================================================
        # STEP 2: Analyze legitimate multiple entries
        # =================================================================
        if 'InvoiceNo' in df.columns and 'StockCode' in df.columns:
            # Count occurrences per invoice-product combination
            combo_counts = df.groupby(['InvoiceNo', 'StockCode']).size().reset_index(name='Occurrences')
            
            # Find combinations with multiple entries
            multiple_entries = combo_counts[combo_counts['Occurrences'] > 1]
            
            if len(multiple_entries) > 0:
                stats['legitimate_multiple_entries'] = int(len(multiple_entries))
                stats['total_multiple_entry_rows'] = int(multiple_entries['Occurrences'].sum())
                
                logger.info(
                    f"Found {len(multiple_entries)} invoice-product combinations "
                    f"with multiple entries - PRESERVING (legitimate data)"
                )
        
        # =================================================================
        # STEP 3: Flag suspicious patterns (don't remove automatically)
        # =================================================================
        if 'InvoiceDate' in df.columns and 'InvoiceNo' in df.columns:
            # Check for same invoice at exactly same timestamp (suspicious)
            df['DateRoundMin'] = df['InvoiceDate'].dt.floor('min')
            
            # Find potential duplicate clusters
            suspicious = df.groupby(['InvoiceNo', 'DateRoundMin']).filter(
                lambda x: len(x) > 3  # More than 3 transactions in same minute?
            )
            
            if len(suspicious) > 0:
                stats['suspicious_transactions_for_review'] = int(len(suspicious))
                # Mark them for review but DON'T remove
                df.loc[suspicious.index, 'NeedsReview'] = True
            
            # Drop temporary column
            df = df.drop('DateRoundMin', axis=1)
        
        # =================================================================
        # STEP 4: Final stats
        # =================================================================
        stats.update({
            'final_rows': int(len(df)),
            'rows_removed': int(initial_count - len(df)),
            'removal_rate': round(
                (initial_count - len(df)) / initial_count * 100, 2
            ) if initial_count > 0 else 0
        })
        
        return df, stats

# =====================================================================
# SINGLETON INSTANCE - THIS MUST EXIST FOR THE IMPORT TO WORK!
# =====================================================================
data_cleaner = DataCleaner()