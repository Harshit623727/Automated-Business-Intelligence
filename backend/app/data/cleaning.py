import pandas as pd
import numpy as np
import logging
from typing import Dict, Any, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)
def _create_derived_columns(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    """Create calculated columns with proper business logic"""
    stats = {}
    
    # REVENUE CALCULATION - FIXED
    if 'Quantity' in df.columns and 'UnitPrice' in df.columns:
        # Raw transaction value (can be negative)
        df['TransactionValue'] = df['Quantity'] * df['UnitPrice']
        
        # Separate sales from returns
        df['IsReturn'] = df['Quantity'] < 0
        
        # Sales revenue (positive only)
        df['SalesRevenue'] = df['TransactionValue'].where(df['Quantity'] > 0, 0)
        
        # Returns value (absolute value for reporting)
        df['ReturnsValue'] = abs(df['TransactionValue']).where(df['Quantity'] < 0, 0)
        
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
    
    # DATE COMPONENTS

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
    
    # TRANSACTION ID - DON'T USE FOR DEDUP!

    if 'InvoiceNo' in df.columns and 'StockCode' in df.columns:
        # This is for reference only, NOT for deduplication
        df['TransactionRef'] = df['InvoiceNo'] + '_' + df['StockCode']
        stats['transaction_ref_created'] = True
    
    # DATA QUALITY FLAGS

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
    
    # STEP 1: Remove EXACT duplicates (all columns identical)
    # These are always safe to remove - they're data entry errors
    
    exact_duplicates = df.duplicated(keep='first')
    exact_dup_count = exact_duplicates.sum()
    
    if exact_dup_count > 0:
        df = df[~exact_duplicates]
        stats['exact_duplicates_removed'] = int(exact_dup_count)
        logger.info(f"Removed {exact_dup_count} exact duplicate rows")
    
    # STEP 2: Analyze legitimate multiple entries

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
    
    # STEP 3: Flag suspicious patterns (don't remove automatically)
    
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
    
    # STEP 4: Final stats
    
    stats.update({
        'final_rows': int(len(df)),
        'rows_removed': int(initial_count - len(df)),
        'removal_rate': round(
            (initial_count - len(df)) / initial_count * 100, 2
        ) if initial_count > 0 else 0
    })
    
    return df, stats