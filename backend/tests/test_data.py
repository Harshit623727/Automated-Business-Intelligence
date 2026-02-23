"""
Test suite for data processing modules
Tests only public interfaces with comprehensive edge cases
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from app.data.ingestion import DataIngestion
from app.data.cleaning import DataCleaner
from app.data.kpi_engine import KPICalculator

# ----------------------------------------------------------------------
# Test Data Ingestion
# ----------------------------------------------------------------------
class TestDataIngestion:
    """Test the data ingestion module"""
    
    def test_sample_data_generation(self):
        """Test sample data generation"""
        ingestion = DataIngestion()
        df = ingestion.generate_sample_data(n_rows=100)
        
        # Basic validation
        assert len(df) == 100
        assert all(col in df.columns for col in [
            'InvoiceNo', 'StockCode', 'Description', 'Quantity', 
            'InvoiceDate', 'UnitPrice', 'CustomerID', 'Country'
        ])
        
        # Check data types
        assert pd.api.types.is_datetime64_any_dtype(df['InvoiceDate'])
        assert pd.api.types.is_numeric_dtype(df['Quantity'])
        assert pd.api.types.is_numeric_dtype(df['UnitPrice'])
        
        # Check for expected characteristics
        assert (df['Quantity'] > 0).sum() > 0  # Has positive quantities
        assert (df['Quantity'] < 0).sum() > 0  # Has returns
        assert df['CustomerID'].isnull().sum() > 0  # Has missing customers
    
    def test_validate_dataframe_valid(self):
        """Test validation with valid DataFrame"""
        ingestion = DataIngestion()
        df = ingestion.generate_sample_data(n_rows=50)
        
        result = ingestion.validate_dataframe(df)
        assert result['is_valid'] is True
    
    def test_validate_dataframe_missing_columns(self):
        """Test validation with missing columns"""
        ingestion = DataIngestion()
        df = pd.DataFrame({'InvoiceNo': ['INV001'], 'Quantity': [5]})
        
        result = ingestion.validate_dataframe(df)
        assert result['is_valid'] is False
        assert len(result['missing_columns']) > 0

# ----------------------------------------------------------------------
# Test Data Cleaning
# ----------------------------------------------------------------------
class TestDataCleaner:
    """Test the data cleaning module"""
    
    def test_clean_dataset_basic(self):
        """Test complete cleaning pipeline with basic data"""
        cleaner = DataCleaner()
        
        # Create test data with issues
        test_data = {
            'InvoiceNo': ['INV001', 'INV002', None],
            'StockCode': ['SKU001', 'SKU002', 'SKU003'],
            'Description': ['Product A', None, 'Product C'],
            'Quantity': [5, -2, 10],
            'InvoiceDate': ['2023-01-01', '2023-01-02', 'invalid'],
            'UnitPrice': [10.0, 20.0, -5.0],
            'CustomerID': ['CUST001', None, 'CUST003'],
            'Country': ['UK', 'USA', None]
        }
        
        df = pd.DataFrame(test_data)
        cleaned_df, report = cleaner.clean_dataset(df)
        
        # Test derived columns created
        assert 'Revenue' in cleaned_df.columns
        assert 'NetRevenue' in cleaned_df.columns
        assert 'SalesRevenue' in cleaned_df.columns
        assert 'ReturnsValue' in cleaned_df.columns
        assert 'Year' in cleaned_df.columns
        assert 'Month' in cleaned_df.columns
        
        # Test cleaning report
        assert report['cleaning_completed'] is True
        assert report['rows_removed'] > 0
        assert 'cleaning_steps' in report
    
    def test_clean_dataset_with_returns(self):
        """Test proper handling of returns"""
        cleaner = DataCleaner()
        
        df = pd.DataFrame({
            'Quantity': [5, -2, 10],
            'UnitPrice': [10.0, 10.0, 20.0],
            'InvoiceDate': pd.date_range('2023-01-01', periods=3)
        })
        
        cleaned_df, report = cleaner.clean_dataset(df)
        
        # Sales should be sum of positive quantities
        assert cleaned_df['SalesRevenue'].sum() == (5 * 10) + (10 * 20)  # 50 + 200 = 250
        
        # Returns should be absolute value of negative transactions
        assert cleaned_df['ReturnsValue'].sum() == abs(-2 * 10)  # 20
        
        # Net revenue = sales - returns
        assert cleaned_df['NetRevenue'].sum() == 250 - 20  # 230
        
        # Check return rate calculation
        step_stats = next(s for s in report['cleaning_steps'] if s['step'] == 'derived_columns')
        assert step_stats['stats']['return_rate'] == (20 / 250 * 100)  # 8%
    
    def test_clean_dataset_with_extreme_values(self):
        """Test handling of extreme values"""
        cleaner = DataCleaner()
        
        df = pd.DataFrame({
            'Quantity': [5, 1000, 1],  # 1000 is extreme
            'UnitPrice': [10.0, 20.0, 1000.0],  # 1000 is extreme
            'InvoiceDate': pd.date_range('2023-01-01', periods=3)
        })
        
        cleaned_df, report = cleaner.clean_dataset(df)
        
        # Should flag extreme values but not remove them
        assert 'DataQualityScore' in cleaned_df.columns
        assert cleaned_df['DataQualityScore'].iloc[1] < 1.0  # Extreme quantity flagged
        assert cleaned_df['DataQualityScore'].iloc[2] < 1.0  # Extreme price flagged
    
    def test_clean_dataset_with_duplicates(self):
        """Test duplicate detection and handling"""
        cleaner = DataCleaner()
        
        # Create data with both exact duplicates and legitimate multiples
        df = pd.DataFrame({
            'InvoiceNo': ['INV001', 'INV001', 'INV001', 'INV002', 'INV002'],
            'StockCode': ['SKU001', 'SKU001', 'SKU002', 'SKU001', 'SKU001'],
            'Quantity': [5, 5, 3, 10, 10],  # First two are exact duplicates
            'UnitPrice': [10.0, 10.0, 15.0, 20.0, 20.0],
            'InvoiceDate': pd.date_range('2023-01-01', periods=5)
        })
        
        cleaned_df, report = cleaner.clean_dataset(df)
        
        # Should remove exact duplicates but keep legitimate multiples
        step_stats = next(s for s in report['cleaning_steps'] if s['step'] == 'duplicate_removal')
        
        # Should have removed 1 exact duplicate
        assert step_stats['stats'].get('exact_duplicates_removed', 0) == 1
        
        # Should have identified legitimate multiple entries
        assert step_stats['stats'].get('legitimate_multiple_entries', 0) > 0

# ----------------------------------------------------------------------
# Test KPI Calculator - PUBLIC INTERFACE ONLY
# ----------------------------------------------------------------------
class TestKPICalculator:
    """Test the KPI calculation module - ONLY public methods"""
    
    def create_test_dataframe(self):
        """Helper to create consistent test data"""
        return pd.DataFrame({
            'InvoiceNo': ['INV001', 'INV001', 'INV002', 'INV003', 'INV004'],
            'StockCode': ['SKU001', 'SKU002', 'SKU001', 'SKU003', 'SKU001'],
            'Description': ['Product A', 'Product B', 'Product A', 'Product C', 'Product A'],
            'Quantity': [5, 3, 10, 1, 2],
            'UnitPrice': [10.0, 20.0, 10.0, 100.0, 10.0],
            'CustomerID': ['C001', 'C001', 'C002', 'C003', 'C004'],
            'Country': ['UK', 'UK', 'USA', 'Germany', 'France'],
            'Revenue': [50, 60, 100, 100, 20]  # Pre-calculated for consistency
        })
    
    def test_calculate_all_metrics_basic(self):
        """Test complete metrics calculation with basic data"""
        calculator = KPICalculator()
        df = self.create_test_dataframe()
        
        # Add date column
        df['InvoiceDate'] = pd.date_range('2023-01-01', periods=len(df))
        df['Year'] = 2023
        df['Month'] = 1
        df['Weekday'] = 'Monday'
        
        metrics = calculator.calculate_all_metrics(df)
        
        # Test summary metrics - FIXED: Check actual values, not just existence
        summary = metrics.get('summary', {})
        assert summary.get('total_revenue') == 330  # 50+60+100+100+20
        assert summary.get('total_transactions') == 4  # Unique invoices: INV001, INV002, INV003, INV004
        assert summary.get('total_customers') == 4  # Unique customers: C001, C002, C003, C004
        assert summary.get('total_products') == 3  # Unique products: SKU001, SKU002, SKU003
        assert summary.get('avg_transaction_value') == pytest.approx(82.5, rel=1e-3)
        
        # Test revenue metrics
        revenue = metrics.get('revenue', {})
        revenue_dist = revenue.get('revenue_distribution', {})
        assert revenue_dist.get('mean') == pytest.approx(66.0, rel=1e-3)  # 330/5
        assert revenue_dist.get('min') == 20
        assert revenue_dist.get('max') == 100
        
        # Test customer metrics - FIXED: Correct count is 4, not 3 or 5
        customer = metrics.get('customer', {})
        assert customer.get('customer_count') == 4  # C001, C002, C003, C004
        
        # Test product metrics
        product = metrics.get('product', {})
        assert product.get('total_products') == 3
        
        # Test metadata
        assert metrics.get('_metadata', {}).get('total_rows_processed') == 5
    
    def test_calculate_metrics_with_returns(self):
        """Test metrics calculation with returns (negative quantities)"""
        calculator = KPICalculator()
        
        df = pd.DataFrame({
            'InvoiceNo': ['INV001', 'INV001', 'INV002'],
            'StockCode': ['SKU001', 'SKU001', 'SKU002'],
            'Quantity': [5, -2, 10],  # Return of 2 units
            'UnitPrice': [10.0, 10.0, 20.0],
            'CustomerID': ['C001', 'C001', 'C002'],
            'Country': ['UK', 'UK', 'USA'],
            'Revenue': [50, -20, 200],  # Net: 50 -20 + 200 = 230
            'InvoiceDate': pd.date_range('2023-01-01', periods=3),
            'Year': 2023,
            'Month': 1,
            'Weekday': 'Monday'
        })
        
        metrics = calculator.calculate_all_metrics(df)
        
        # Total revenue should be NET revenue
        summary = metrics.get('summary', {})
        assert summary.get('total_revenue') == 230  # 50 -20 + 200
        
        # Should still count returns as transactions
        assert summary.get('total_transactions') == 2  # Unique invoices: INV001, INV002
    
    def test_calculate_metrics_with_missing_data(self):
        """Test metrics calculation with missing/partial data"""
        calculator = KPICalculator()
        
        # Missing CustomerID, some nulls
        df = pd.DataFrame({
            'InvoiceNo': ['INV001', 'INV002', 'INV003'],
            'StockCode': ['SKU001', 'SKU002', 'SKU001'],
            'Quantity': [5, 10, 3],
            'UnitPrice': [10.0, 20.0, 10.0],
            'CustomerID': ['C001', None, None],
            'Country': ['UK', None, 'USA'],
            'Revenue': [50, 200, 30],
            'InvoiceDate': pd.date_range('2023-01-01', periods=3),
            'Year': 2023,
            'Month': 1,
            'Weekday': 'Monday'
        })
        
        metrics = calculator.calculate_all_metrics(df)
        
        # Should handle missing customers gracefully
        customer = metrics.get('customer', {})
        # 'Unknown' customers should be excluded from some metrics
        assert customer.get('customer_count', 0) >= 1  # At least C001
        
        # Should still calculate revenue correctly
        summary = metrics.get('summary', {})
        assert summary.get('total_revenue') == 280  # 50 + 200 + 30
    
    def test_calculate_metrics_with_edge_cases(self):
        """Test metrics calculation with various edge cases"""
        calculator = KPICalculator()
        
        # Test case: Zero prices
        df_zero_price = pd.DataFrame({
            'InvoiceNo': ['INV001'],
            'StockCode': ['SKU001'],
            'Quantity': [5],
            'UnitPrice': [0.0],  # Zero price
            'CustomerID': ['C001'],
            'Country': ['UK'],
            'Revenue': [0],
            'InvoiceDate': [pd.Timestamp('2023-01-01')],
            'Year': 2023,
            'Month': 1,
            'Weekday': 'Monday'
        })
        
        metrics = calculator.calculate_all_metrics(df_zero_price)
        assert metrics.get('summary', {}).get('total_revenue') == 0
        
        # Test case: Single transaction
        df_single = pd.DataFrame({
            'InvoiceNo': ['INV001'],
            'StockCode': ['SKU001'],
            'Quantity': [5],
            'UnitPrice': [10.0],
            'CustomerID': ['C001'],
            'Country': ['UK'],
            'Revenue': [50],
            'InvoiceDate': [pd.Timestamp('2023-01-01')],
            'Year': 2023,
            'Month': 1,
            'Weekday': 'Monday'
        })
        
        metrics = calculator.calculate_all_metrics(df_single)
        summary = metrics.get('summary', {})
        assert summary.get('total_transactions') == 1
        assert summary.get('avg_transaction_value') == 50.0
        
        # Test case: All customers unknown
        df_unknown = pd.DataFrame({
            'InvoiceNo': ['INV001', 'INV002'],
            'StockCode': ['SKU001', 'SKU002'],
            'Quantity': [5, 10],
            'UnitPrice': [10.0, 20.0],
            'CustomerID': [None, None],  # All unknown
            'Country': ['UK', 'USA'],
            'Revenue': [50, 200],
            'InvoiceDate': pd.date_range('2023-01-01', periods=2),
            'Year': 2023,
            'Month': 1,
            'Weekday': 'Monday'
        })
        
        metrics = calculator.calculate_all_metrics(df_unknown)
        customer = metrics.get('customer', {})
        # Should handle gracefully - might have 0 customers or exclude unknowns
        assert 'customer_count' in customer
    
    def test_calculate_metrics_with_date_edge_cases(self):
        """Test metrics calculation with date edge cases"""
        calculator = KPICalculator()
        
        # Create data spanning multiple months
        dates = [
            pd.Timestamp('2023-01-15'),
            pd.Timestamp('2023-02-20'),
            pd.Timestamp('2023-03-10')
        ]
        
        df = pd.DataFrame({
            'InvoiceNo': ['INV001', 'INV002', 'INV003'],
            'StockCode': ['SKU001', 'SKU002', 'SKU003'],
            'Quantity': [5, 10, 3],
            'UnitPrice': [10.0, 20.0, 15.0],
            'CustomerID': ['C001', 'C002', 'C003'],
            'Country': ['UK', 'USA', 'Germany'],
            'Revenue': [50, 200, 45],
            'InvoiceDate': dates,
            'Year': [d.year for d in dates],
            'Month': [d.month for d in dates],
            'Weekday': [d.strftime('%A') for d in dates]
        })
        
        metrics = calculator.calculate_all_metrics(df)
        
        # Check monthly revenue
        revenue = metrics.get('revenue', {})
        monthly = revenue.get('monthly_revenue', {})
        
        # Should have 3 months of data
        assert len(monthly) == 3
        
        # Check seasonality detection
        time_series = metrics.get('time_series', {})
        seasonality = time_series.get('seasonality', {})
        
        # Best day should be one of the weekdays in our data
        assert seasonality.get('best_day') in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    
    def test_empty_dataframe_with_structure(self):
        """Test with empty DataFrame that still has correct structure"""
        calculator = KPICalculator()
        
        # Create DataFrame with correct columns but no rows - FIXED: More realistic
        df = pd.DataFrame(columns=[
            'InvoiceNo', 'StockCode', 'Description', 'Quantity', 
            'InvoiceDate', 'UnitPrice', 'CustomerID', 'Country',
            'Revenue', 'Year', 'Month', 'Weekday'
        ])
        
        metrics = calculator.calculate_all_metrics(df)
        
        # Should not crash and return structured response
        assert '_metadata' in metrics
        assert metrics['_metadata']['total_rows_processed'] == 0
        assert metrics['_metadata'].get('error') == 'No data available'
        
        # All metric sections should exist but be empty
        assert 'summary' in metrics
        assert 'revenue' in metrics
        assert 'customer' in metrics
        assert 'product' in metrics
    
    def test_completely_empty_dataframe(self):
        """Test with completely empty DataFrame"""
        calculator = KPICalculator()
        
        df = pd.DataFrame()  # No columns, no rows
        
        metrics = calculator.calculate_all_metrics(df)
        
        # Should not crash
        assert '_metadata' in metrics
        assert metrics['_metadata']['total_rows_processed'] == 0

# ----------------------------------------------------------------------
# Integration Tests
# ----------------------------------------------------------------------
def test_full_pipeline_integration():
    """Test the complete pipeline from ingestion to metrics"""
    ingestion = DataIngestion()
    cleaner = DataCleaner()
    calculator = KPICalculator()
    
    # Generate sample data
    raw_df = ingestion.generate_sample_data(n_rows=100)
    
    # Clean data
    cleaned_df, cleaning_report = cleaner.clean_dataset(raw_df)
    
    # Calculate metrics
    metrics = calculator.calculate_all_metrics(cleaned_df)
    
    # Verify end-to-end
    assert cleaning_report['cleaning_completed'] is True
    assert metrics['_metadata']['total_rows_processed'] == len(cleaned_df)
    assert metrics['summary']['total_revenue'] > 0
    assert metrics['summary']['total_customers'] > 0

def test_data_quality_flags_integration():
    """Test that data quality flags propagate through pipeline"""
    ingestion = DataIngestion()
    cleaner = DataCleaner()
    
    # Create data with extreme values
    df = pd.DataFrame({
        'InvoiceNo': ['INV001', 'INV002'],
        'StockCode': ['SKU001', 'SKU002'],
        'Quantity': [5, 1000],  # Extreme quantity
        'UnitPrice': [10.0, 20.0],
        'CustomerID': ['C001', 'C002'],
        'Country': ['UK', 'USA'],
        'InvoiceDate': pd.date_range('2023-01-01', periods=2)
    })
    
    cleaned_df, report = cleaner.clean_dataset(df)
    
    # Should have quality flags
    assert 'DataQualityScore' in cleaned_df.columns
    assert 'QualityFlag' in cleaned_df.columns
    
    # Extreme row should have lower quality score
    assert cleaned_df['DataQualityScore'].iloc[1] < cleaned_df['DataQualityScore'].iloc[0]

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])