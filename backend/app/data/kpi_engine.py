"""
KPI and Metrics Engine for Automated BI System
Computes business metrics from cleaned transaction data
"""
import pandas as pd
import numpy as np
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class KPICalculator:
    """Calculates business KPIs and metrics from transaction data"""
    
    def __init__(self):
        self.calculation_time = None
    
    def calculate_all_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Calculate comprehensive set of business metrics
        
        Args:
            df: Cleaned DataFrame with transaction data
            
        Returns:
            Dictionary containing all calculated metrics
        """
        if df.empty:
            logger.warning("Empty DataFrame provided for KPI calculation")
            return self._get_empty_metrics()
        
        logger.info(f"Calculating metrics for {len(df)} transactions")
        self.calculation_time = datetime.now()
        
        metrics = {
            "summary": self._calculate_summary_metrics(df),
            "revenue": self._calculate_revenue_metrics(df),
            "customer": self._calculate_customer_metrics(df),
            "product": self._calculate_product_metrics(df),
            "time_series": self._calculate_time_series_metrics(df),
            "geographic": self._calculate_geographic_metrics(df),
            "calculated_at": self.calculation_time.isoformat()
        }
        
        # Calculate derived metrics
        metrics["growth"] = self._calculate_growth_rates(metrics)
        metrics["top_performers"] = self._identify_top_performers(metrics)
        metrics["health_scores"] = self._calculate_health_scores(metrics)
        
        # Add metadata
        metrics["_metadata"] = {
            "total_rows_processed": len(df),
            "calculation_duration_ms": (datetime.now() - self.calculation_time).total_seconds() * 1000,
            "metric_count": self._count_metrics(metrics)
        }
        
        logger.info(f"Calculated {metrics['_metadata']['metric_count']} metrics")
        
        return metrics
    
    def _calculate_summary_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate overall business summary metrics"""
        summary = {}
        
        # Basic counts
        summary["total_revenue"] = round(float(df['Revenue'].sum()), 2) if 'Revenue' in df.columns else 0
        summary["total_transactions"] = int(df['InvoiceNo'].nunique()) if 'InvoiceNo' in df.columns else 0
        summary["total_products"] = int(df['StockCode'].nunique()) if 'StockCode' in df.columns else 0
        summary["total_customers"] = int(df['CustomerID'].nunique()) if 'CustomerID' in df.columns else 0
        summary["total_items_sold"] = int(df['Quantity'].sum()) if 'Quantity' in df.columns else 0
        
        # Averages
        if summary["total_transactions"] > 0:
            summary["avg_transaction_value"] = round(
                summary["total_revenue"] / summary["total_transactions"], 2
            )
            summary["avg_items_per_transaction"] = round(
                summary["total_items_sold"] / summary["total_transactions"], 1
            )
        else:
            summary["avg_transaction_value"] = 0
            summary["avg_items_per_transaction"] = 0
        
        # Date range
        if 'InvoiceDate' in df.columns and not df['InvoiceDate'].empty:
            summary["date_range"] = {
                "start": df['InvoiceDate'].min().strftime('%Y-%m-%d'),
                "end": df['InvoiceDate'].max().strftime('%Y-%m-%d'),
                "days": (df['InvoiceDate'].max() - df['InvoiceDate'].min()).days
            }
        else:
            summary["date_range"] = {"start": None, "end": None, "days": 0}
        
        return summary
    
    def _calculate_revenue_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate revenue-related metrics"""
        revenue_metrics = {}
        
        if 'Revenue' not in df.columns or 'InvoiceDate' not in df.columns:
            return revenue_metrics
        
        # Monthly revenue
        monthly_revenue = df.groupby(['Year', 'Month'])['Revenue'].sum().reset_index()
        monthly_revenue['Period'] = monthly_revenue['Year'].astype(str) + '-' + \
                                   monthly_revenue['Month'].astype(str).str.zfill(2)
        revenue_metrics["monthly_revenue"] = dict(zip(
            monthly_revenue['Period'], 
            monthly_revenue['Revenue'].round(2)
        ))
        
        # Quarterly revenue
        if 'Quarter' in df.columns:
            quarterly_revenue = df.groupby(['Year', 'Quarter'])['Revenue'].sum().reset_index()
            quarterly_revenue['Period'] = 'Q' + quarterly_revenue['Quarter'].astype(str) + \
                                         ' ' + quarterly_revenue['Year'].astype(str)
            revenue_metrics["quarterly_revenue"] = dict(zip(
                quarterly_revenue['Period'],
                quarterly_revenue['Revenue'].round(2)
            ))
        
        # Revenue by weekday
        if 'Weekday' in df.columns:
            weekday_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            weekday_revenue = df.groupby('Weekday')['Revenue'].sum()
            # Reindex to ensure order
            weekday_revenue = weekday_revenue.reindex(weekday_order).fillna(0)
            revenue_metrics["weekday_revenue"] = weekday_revenue.round(2).to_dict()
        
        # Revenue distribution
        revenue_metrics["revenue_distribution"] = {
            "min": round(float(df['Revenue'].min()), 2),
            "max": round(float(df['Revenue'].max()), 2),
            "mean": round(float(df['Revenue'].mean()), 2),
            "median": round(float(df['Revenue'].median()), 2),
            "std": round(float(df['Revenue'].std()), 2),
            "percentiles": {
                "25": round(float(df['Revenue'].quantile(0.25)), 2),
                "50": round(float(df['Revenue'].quantile(0.50)), 2),
                "75": round(float(df['Revenue'].quantile(0.75)), 2),
                "90": round(float(df['Revenue'].quantile(0.90)), 2)
            }
        }
        
        return revenue_metrics
    
    def _calculate_customer_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate customer-related metrics"""
        customer_metrics = {}
        
        if 'CustomerID' not in df.columns:
            return customer_metrics
        
        # Remove Unknown customers for analysis
        customer_df = df[df['CustomerID'] != 'Unknown']
        
        if customer_df.empty:
            customer_metrics["customer_count"] = 0
            customer_metrics["avg_customer_value"] = 0
            return customer_metrics
        
        # Customer spending analysis
        customer_spending = customer_df.groupby('CustomerID').agg({
            'Revenue': 'sum',
            'InvoiceNo': 'nunique',
            'Quantity': 'sum'
        }).rename(columns={
            'Revenue': 'total_spent',
            'InvoiceNo': 'transaction_count',
            'Quantity': 'total_items'
        })
        
        customer_spending['avg_transaction_value'] = (
            customer_spending['total_spent'] / customer_spending['transaction_count'].replace(0, np.nan)).fillna(0)
        
        # Customer segments using quartiles
        try:
            customer_spending['segment'] = pd.qcut(
                customer_spending['total_spent'], 
                q=4, 
                labels=['Low', 'Medium', 'High', 'VIP'],
                duplicates='drop'
            )
        except ValueError:
            # Fallback if qcut fails
            customer_spending['segment'] = 'Medium'
        
        # RFM Analysis
        max_date = customer_df['InvoiceDate'].max()
        rfm = customer_df.groupby('CustomerID').agg({
            'InvoiceDate': lambda x: (max_date - x.max()).days,
            'InvoiceNo': 'nunique',
            'Revenue': 'sum'
        }).rename(columns={
            'InvoiceDate': 'recency',
            'InvoiceNo': 'frequency',
            'Revenue': 'monetary'
        })
        
        customer_metrics = {
            "customer_count": int(len(customer_spending)),
            "active_customers": int(len(customer_spending[customer_spending['transaction_count'] > 1])),
            "one_time_customers": int(len(customer_spending[customer_spending['transaction_count'] == 1])),
            "avg_customer_value": round(float(customer_spending['total_spent'].mean()), 2),
            "median_customer_value": round(float(customer_spending['total_spent'].median()), 2),
            "segment_distribution": customer_spending['segment'].value_counts().to_dict(),
            "rfm_summary": {
                "avg_recency": round(float(rfm['recency'].mean()), 1),
                "avg_frequency": round(float(rfm['frequency'].mean()), 1),
                "avg_monetary": round(float(rfm['monetary'].mean()), 2)
            }
        }
        
        # Top customers
        top_customers = customer_spending.nlargest(10, 'total_spent')[
            ['total_spent', 'transaction_count', 'avg_transaction_value']
        ]
        
        customer_metrics["top_customers"] = {
            str(idx): {
                "total_spent": round(float(row['total_spent']), 2),
                "transactions": int(row['transaction_count']),
                "avg_value": round(float(row['avg_transaction_value']), 2)
            }
            for idx, row in top_customers.iterrows()
        }
        
        return customer_metrics
    
    def _calculate_product_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate product-related metrics"""
        product_metrics = {}
        
        if 'StockCode' not in df.columns or 'Revenue' not in df.columns:
            return product_metrics
        
        # Product performance
        product_performance = df.groupby(['StockCode', 'Description']).agg({
            'Revenue': 'sum',
            'Quantity': 'sum',
            'InvoiceNo': 'nunique'
        }).rename(columns={
            'Revenue': 'total_revenue',
            'Quantity': 'total_quantity',
            'InvoiceNo': 'transaction_count'
        }).reset_index()
        
        product_performance['avg_price'] = (
            product_performance['total_revenue'] /
            product_performance['total_quantity'].replace(0, np.nan)
            ).fillna(0)
        
        product_metrics = {
            "total_products": int(len(product_performance)),
            "unique_products_sold": int(product_performance['StockCode'].nunique()),
            "avg_product_price": round(float(product_performance['avg_price'].mean()), 2),
            "median_product_price": round(float(product_performance['avg_price'].median()), 2)
        }
        
        # Top products
        top_products = product_performance.nlargest(10, 'total_revenue')
        product_metrics["top_products"] = {
            str(idx): {
                "description": row['Description'],
                "total_revenue": round(float(row['total_revenue']), 2),
                "total_quantity": int(row['total_quantity']),
                "transaction_count": int(row['transaction_count']),
                "avg_price": round(float(row['avg_price']), 2)
            }
            for idx, row in top_products.iterrows()
        }
        
        # Price distribution
        product_metrics["price_distribution"] = {
            "min": round(float(product_performance['avg_price'].min()), 2),
            "max": round(float(product_performance['avg_price'].max()), 2),
            "mean": round(float(product_performance['avg_price'].mean()), 2),
            "median": round(float(product_performance['avg_price'].median()), 2)
        }
        
        return product_metrics
    
    def _calculate_time_series_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate time-based trends and patterns"""
        time_metrics = {}
        
        if 'InvoiceDate' not in df.columns or 'Revenue' not in df.columns:
            return time_metrics
        
        # Daily trends
        df_temp = df.copy()
        df_temp['Date'] = df_temp['InvoiceDate'].dt.date

        daily_trends = df_temp.groupby('Date').agg({
            'Revenue': 'sum',
            'InvoiceNo': 'nunique',
            'CustomerID': 'nunique',
            'Quantity': 'sum'
        }).rename(columns={
            'Revenue': 'daily_revenue',
            'InvoiceNo': 'daily_transactions',
            'CustomerID': 'daily_customers',
            'Quantity': 'daily_items'
        })
        
        # Calculate moving averages
        daily_trends['revenue_7d_ma'] = \
            daily_trends['daily_revenue'].rolling(window=7).mean().fillna(0)
        
        daily_trends['revenue_30d_ma'] = \
            daily_trends['daily_revenue'].rolling(window=30).mean().fillna(0)
        
        # Detect seasonality
        if 'Weekday' in df.columns:
            weekday_pattern = df.groupby('Weekday')['Revenue'].mean()
            weekday_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            weekday_pattern = weekday_pattern.reindex(weekday_order)
            
            best_day = weekday_pattern.idxmax() if not weekday_pattern.empty else None
            worst_day = weekday_pattern.idxmin() if not weekday_pattern.empty else None
            
            time_metrics["seasonality"] = {
                "weekly_pattern": weekday_pattern.round(2).to_dict(),
                "best_day": best_day,
                "worst_day": worst_day,
                "weekly_variation": round(
                    (weekday_pattern.max() - weekday_pattern.min()) / weekday_pattern.mean() * 100, 2
                ) if not weekday_pattern.empty and weekday_pattern.mean() > 0 else 0
            }
        
        return time_metrics
    
    def _calculate_geographic_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate geographic distribution metrics"""
        geo_metrics = {}
        
        if 'Country' not in df.columns or 'Revenue' not in df.columns:
            return geo_metrics
        
        country_performance = df.groupby('Country').agg({
            'Revenue': 'sum',
            'InvoiceNo': 'nunique',
            'CustomerID': 'nunique',
            'Quantity': 'sum'
        }).rename(columns={
            'Revenue': 'total_revenue',
            'InvoiceNo': 'transaction_count',
            'CustomerID': 'customer_count',
            'Quantity': 'total_quantity'
        }).sort_values('total_revenue', ascending=False)
        
        geo_metrics = {
            "countries_covered": int(len(country_performance)),
            "top_countries": {
                str(idx): {
                    "total_revenue": round(float(row['total_revenue']), 2),
                    "transactions": int(row['transaction_count']),
                    "customers": int(row['customer_count'])
                }
                for idx, row in country_performance.head(5).iterrows()
            }
        }
        
        # Calculate international percentage
        if len(country_performance) > 1:
            top_country_revenue = country_performance.iloc[0]['total_revenue']
            total_revenue = country_performance['total_revenue'].sum()
            geo_metrics["international_percentage"] = round(
                (1 - top_country_revenue / total_revenue) * 100, 2
            )
        else:
            geo_metrics["international_percentage"] = 0
        
        return geo_metrics
    
    def _calculate_growth_rates(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate growth metrics"""
        growth = {}
        
        monthly_revenue = metrics.get('revenue', {}).get('monthly_revenue', {})
        if len(monthly_revenue) >= 2:
            import pandas as pd

            df_growth = pd.DataFrame(
                list(monthly_revenue.items()),
                columns=['Period', 'Revenue']
            )
            df_growth['Period'] = pd.to_datetime(df_growth['Period'])
            df_growth = df_growth.sort_values('Period')
            current_month = df_growth.iloc[-1]['Revenue']
            previous_month = df_growth.iloc[-2]['Revenue']
            if previous_month > 0:
                mom_growth = ((current_month - previous_month) / previous_month) * 100
                growth['revenue_mom'] = round(mom_growth, 2)
                growth['revenue_trend'] = 'increasing' if mom_growth > 0 else 'decreasing'
            else:
                growth['revenue_mom'] = None
                growth['revenue_trend'] = 'stable'
        
        return growth
    
    def _identify_top_performers(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Identify top performing entities"""
        top_performers = {}
        
        # Top products
        if 'product' in metrics and 'top_products' in metrics['product']:
            top_products = list(metrics['product']['top_products'].keys())[:5]
            top_performers['products'] = top_products
        
        # Top customers
        if 'customer' in metrics and 'top_customers' in metrics['customer']:
            top_customers = list(metrics['customer']['top_customers'].keys())[:5]
            top_performers['customers'] = top_customers
        
        # Top countries
        if 'geographic' in metrics and 'top_countries' in metrics['geographic']:
            top_countries = list(metrics['geographic']['top_countries'].keys())[:3]
            top_performers['countries'] = top_countries
        
        return top_performers
    
    def _calculate_health_scores(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate business health scores (0-100)"""
        scores = {}
        
        # Revenue health
        revenue_score = 50
        summary = metrics.get('summary', {})
        growth = metrics.get('growth', {})
        
        if summary.get('total_revenue', 0) > 10000:
            revenue_score += 10
        if summary.get('total_transactions', 0) > 100:
            revenue_score += 10
        if growth.get('revenue_mom', 0) > 0:
            revenue_score += min(20, growth['revenue_mom'])
        elif growth.get('revenue_mom', 0) < 0:
            revenue_score -= min(10, abs(growth['revenue_mom']))
        
        scores['revenue_health'] = max(0, min(100, revenue_score))
        
        # Customer health
        customer_score = 50
        customer = metrics.get('customer', {})
        
        if customer.get('customer_count', 0) > 50:
            customer_score += 20
        elif customer.get('customer_count', 0) > 10:
            customer_score += 10
        
        if customer.get('one_time_customers', 0) < customer.get('customer_count', 0) * 0.5:
            customer_score += 10
        
        scores['customer_health'] = max(0, min(100, customer_score))
        
        # Product health
        product_score = 50
        product = metrics.get('product', {})
        
        if product.get('total_products', 0) > 20:
            product_score += 20
        elif product.get('total_products', 0) > 5:
            product_score += 10
        
        scores['product_health'] = max(0, min(100, product_score))
        
        # Overall health
        scores['overall_health'] = round(
            scores['revenue_health'] * 0.5 +
            scores['customer_health'] * 0.3 +
            scores['product_health'] * 0.2,
            1
        )
        
        return scores
    
    def _get_empty_metrics(self) -> Dict[str, Any]:
        """Return empty metrics structure"""
        return {
            "summary": {},
            "revenue": {},
            "customer": {},
            "product": {},
            "time_series": {},
            "geographic": {},
            "growth": {},
            "top_performers": {},
            "health_scores": {},
            "calculated_at": datetime.now().isoformat(),
            "_metadata": {
                "total_rows_processed": 0,
                "calculation_duration_ms": 0,
                "metric_count": 0,
                "error": "No data available"
            }
        }
    
    def _count_metrics(self, metrics: Dict[str, Any]) -> int:
        """Count total number of calculated metrics"""
        count = 0
        for key, value in metrics.items():
            if key.startswith('_'):
                continue
            if isinstance(value, dict):
                count += self._count_metrics(value)
            elif isinstance(value, (int, float, str)) and not key.startswith('_'):
                count += 1
        return count

# Singleton instance
kpi_calculator = KPICalculator()