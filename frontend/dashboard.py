"""
Streamlit Dashboard for Automated BI System
Customer-facing interface for the BI platform
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import requests
import json
from datetime import datetime
import time
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ----------------------------------------------------------------------
# Page Configuration
# ----------------------------------------------------------------------
st.set_page_config(
    page_title="Automated BI Platform",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ----------------------------------------------------------------------
# Stripe-Inspired Dark Theme
# ----------------------------------------------------------------------
st.markdown("""
<style>

/* Global background */
html, body, [data-testid="stAppViewContainer"] {
    background-color: #0A0F1C;
    color: #E6E8EC;
}

/* Remove default Streamlit padding */
.block-container {
    padding-top: 2rem;
    padding-bottom: 2rem;
}

/* Main Header */
.main-header {
    font-size: 2.4rem;
    font-weight: 700;
    color: #FFFFFF;
    margin-bottom: 0.5rem;
}

.sub-header {
    font-size: 1.4rem;
    font-weight: 600;
    color: #C7CDD8;
    margin-top: 2rem;
    margin-bottom: 1rem;
}

/* KPI Cards */
.kpi-card {
    background: linear-gradient(145deg, #111827, #0F172A);
    padding: 1.6rem;
    border-radius: 18px;
    border: 1px solid rgba(255,255,255,0.05);
    transition: all 0.25s ease;
}

.kpi-card:hover {
    transform: translateY(-6px);
    box-shadow: 0px 20px 40px rgba(0,0,0,0.45);
}

.metric-value {
    font-size: 2rem;
    font-weight: 700;
    color: #FFFFFF;
}

.metric-label {
    font-size: 0.85rem;
    color: #9CA3AF;
    text-transform: uppercase;
    letter-spacing: 0.08em;
}

/* Insight Cards */
.insight-card {
    background: #111827;
    padding: 1.4rem;
    border-radius: 16px;
    border-left: 4px solid #635BFF; /* Stripe purple */
    margin-bottom: 1rem;
    color: #E5E7EB;
}

/* Risk Card */
.risk-card {
    background: #1F2937;
    padding: 1.4rem;
    border-radius: 16px;
    border-left: 4px solid #EF4444;
    margin-bottom: 1rem;
    color: #E5E7EB;
}

/* Success Card */
.success-card {
    background: #0F172A;
    padding: 1.4rem;
    border-radius: 16px;
    border-left: 4px solid #22C55E;
    margin-bottom: 1rem;
    color: #E5E7EB;
}

/* Recommendation Card */
.recommendation-card {
    background: #111827;
    padding: 1rem;
    border-radius: 14px;
    border: 1px solid rgba(255,255,255,0.05);
    margin-bottom: 0.7rem;
}

/* Divider */
.custom-divider {
    margin: 2rem 0;
    border: 0;
    height: 1px;
    background: linear-gradient(to right, transparent, #1F2937, transparent);
}

/* Sidebar */
section[data-testid="stSidebar"] {
    background-color: #0F172A;
    border-right: 1px solid rgba(255,255,255,0.05);
}

/* Buttons */
button[kind="primary"] {
    background: linear-gradient(135deg, #635BFF, #7C3AED);
    border-radius: 12px;
    border: none;
    font-weight: 600;
}

button[kind="secondary"] {
    border-radius: 12px;
}

/* Tabs */
[data-baseweb="tab"] {
    font-weight: 600;
    color: #9CA3AF;
}

[data-baseweb="tab"][aria-selected="true"] {
    color: #FFFFFF;
}

/* Tables */
[data-testid="stDataFrame"] {
    background-color: #111827;
    border-radius: 12px;
}

/* Metrics */
[data-testid="metric-container"] {
    background: #111827;
    padding: 1rem;
    border-radius: 12px;
    border: 1px solid rgba(255,255,255,0.05);
}

</style>
""", unsafe_allow_html=True)

# ----------------------------------------------------------------------
# Configuration with Validation
# ----------------------------------------------------------------------
def get_backend_url():
    """
    Get backend URL from environment with validation
    Returns:
        str: Backend URL or None if not configured
    """
    url = os.getenv("BACKEND_URL")
    
    # For development, allow localhost fallback
    if not url:
        url = "http://localhost:8000"
        # Show warning in production-like environments
        if os.getenv("ENVIRONMENT") == "production":
            st.warning("⚠️ BACKEND_URL not set in environment. Using default localhost.")
    
    # Remove trailing slash if present
    if url.endswith('/'):
        url = url[:-1]
    
    return url

BACKEND_URL = get_backend_url()
API_BASE = f"{BACKEND_URL}/api/v1"

# ----------------------------------------------------------------------
# Session State Initialization
# ----------------------------------------------------------------------
if 'dataset_id' not in st.session_state:
    st.session_state.dataset_id = None
if 'filename' not in st.session_state:
    st.session_state.filename = None
if 'metrics' not in st.session_state:
    st.session_state.metrics = None
if 'insights' not in st.session_state:
    st.session_state.insights = None
if 'upload_timestamp' not in st.session_state:
    st.session_state.upload_timestamp = None
if 'api_error' not in st.session_state:
    st.session_state.api_error = None

# ----------------------------------------------------------------------
# Helper Functions
# ----------------------------------------------------------------------
def check_backend_health():
    """Check if backend API is available"""
    try:
        response = requests.get(f"{BACKEND_URL}/health", timeout=3)
        return response.status_code == 200
    except requests.exceptions.ConnectionError:
        return False
    except requests.exceptions.Timeout:
        return False
    except Exception:
        return False

def safe_api_call(func, *args, **kwargs):
    """
    SAFE wrapper for API calls with comprehensive error handling
    Returns:
        tuple: (response_data, error_message)
    """
    try:
        response = func(*args, **kwargs)
        
        # Check HTTP status code
        if response.status_code == 200:
            return response.json(), None
        elif response.status_code == 404:
            return None, f"Resource not found (404)"
        elif response.status_code == 400:
            error_detail = response.json().get('detail', 'Bad request')
            return None, f"Bad request: {error_detail}"
        elif response.status_code == 500:
            return None, "Server error (500). Please try again later."
        else:
            return None, f"Unexpected error (HTTP {response.status_code})"
            
    except requests.exceptions.ConnectionError:
        return None, f"Cannot connect to backend at {BACKEND_URL}. Is the server running?"
    except requests.exceptions.Timeout:
        return None, "Request timed out. The server might be busy."
    except requests.exceptions.JSONDecodeError:
        return None, "Server returned invalid JSON response"
    except Exception as e:
        return None, f"Unexpected error: {str(e)}"

def format_currency(value):
    """Safely format number as currency"""
    if value is None:
        return "$0.00"
    try:
        return f"${float(value):,.2f}"
    except (ValueError, TypeError):
        return "$0.00"

def format_number(value):
    """Safely format number with commas"""
    if value is None:
        return "0"
    try:
        return f"{int(value):,}"
    except (ValueError, TypeError):
        return "0"

def safe_string(value, default=""):
    """Safely convert to string, handling None"""
    if value is None:
        return default
    return str(value)

def safe_slice(value, start, end):
    """Safely slice a string, handling None"""
    if value is None:
        return ""
    value_str = str(value)
    if len(value_str) > end:
        return value_str[start:end]
    return value_str

# ----------------------------------------------------------------------
# API Functions with SAFE error handling
# ----------------------------------------------------------------------
def upload_file(file):
    """Safely upload file to backend"""
    files = {'file': (file.name, file.getvalue())}
    return safe_api_call(
        lambda: requests.post(f"{API_BASE}/upload", files=files, timeout=120)
    )

def use_sample_data():
    """Safely use sample dataset"""
    return safe_api_call(
        lambda: requests.post(f"{API_BASE}/upload?use_sample=true", timeout=30)
    )

def fetch_metrics(dataset_id):
    """Safely fetch metrics from backend"""
    return safe_api_call(
        lambda: requests.get(f"{API_BASE}/metrics/{dataset_id}", timeout=10)
    )

def fetch_insights(dataset_id, refresh=False):
    """Safely fetch insights from backend"""
    url = f"{API_BASE}/insights/{dataset_id}"
    if refresh:
        url += "?refresh=true"
    return safe_api_call(
        lambda: requests.get(url, timeout=15)  # Insights take longer
    )

def list_datasets():
    """Safely list all datasets"""
    return safe_api_call(
        lambda: requests.get(f"{API_BASE}/datasets", timeout=10)
    )

def delete_dataset(dataset_id):
    """Safely delete a dataset"""
    return safe_api_call(
        lambda: requests.delete(f"{API_BASE}/datasets/{dataset_id}", timeout=10)
    )

# ----------------------------------------------------------------------
# Dashboard Components
# ----------------------------------------------------------------------
def render_header():
    """Render dashboard header with backend status"""
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.markdown('<h1 class="main-header">📊 Automated Business Intelligence</h1>', 
                   unsafe_allow_html=True)
        st.markdown("""
        Upload your business data and get **automated KPIs, AI insights, and recommendations**.
        No manual analysis required.
        """)
    
    with col2:
        # Health indicator with better styling
        is_healthy = check_backend_health()
        if is_healthy:
            st.markdown(f"""
            <div style="background: #D1FAE5; padding: 0.75rem; border-radius: 0.5rem; text-align: center;">
                <span style="color: #059669;">✅ Connected</span><br>
                <small style="color: #6B7280;">{BACKEND_URL}</small>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
            <div style="background: #FEE2E2; padding: 0.75rem; border-radius: 0.5rem; text-align: center;">
                <span style="color: #DC2626;">❌ Disconnected</span><br>
                <small style="color: #6B7280;">{BACKEND_URL}</small>
            </div>
            """, unsafe_allow_html=True)

def render_sidebar():
    """Render sidebar with upload controls"""
    with st.sidebar:
        st.markdown("## 📁 Data Source")
        
        # Upload option
        upload_option = st.radio(
            "Choose data source:",
            ["Upload CSV/Excel", "Use Sample Data"],
            label_visibility="collapsed"
        )
        
        if upload_option == "Upload CSV/Excel":
            uploaded_file = st.file_uploader(
                "Choose a file",
                type=['csv', 'xlsx', 'xls'],
                help="Upload your business data (CSV or Excel format)"
            )
            
            if uploaded_file and st.button("🚀 Process Data", type="primary", use_container_width=True):
                with st.spinner("Uploading and processing data..."):
                    response_data, error = upload_file(uploaded_file)
                    
                    if error:
                        st.session_state.api_error = error
                        st.error(f"❌ Upload failed: {error}")
                    else:
                        st.session_state.dataset_id = response_data['dataset_id']
                        st.session_state.filename = response_data['filename']
                        st.session_state.upload_timestamp = response_data['uploaded_at']
                        st.session_state.metrics = None
                        st.session_state.insights = None
                        st.session_state.api_error = None
                        st.success(f"✅ Success! {response_data['rows_cleaned']} rows processed")
                        st.rerun()
        
        else:  # Sample Data
            st.info("📊 Using sample retail dataset")
            if st.button("🔄 Load Sample Data", use_container_width=True, type="primary"):
                with st.spinner("Loading sample data..."):
                    response_data, error = use_sample_data()
                    
                    if error:
                        st.session_state.api_error = error
                        st.error(f"❌ Failed to load sample: {error}")
                    else:
                        st.session_state.dataset_id = response_data['dataset_id']
                        st.session_state.filename = "Sample Retail Data"
                        st.session_state.upload_timestamp = response_data['uploaded_at']
                        st.session_state.metrics = None
                        st.session_state.insights = None
                        st.session_state.api_error = None
                        st.success(f"✅ Sample data loaded! {response_data['rows_cleaned']} rows")
                        st.rerun()
        
        # Dataset Management
        if st.session_state.dataset_id:
            st.markdown("---")
            st.markdown("## 🔧 Active Dataset")
            
            st.info(f"""
            **Dataset:** {st.session_state.filename}
            **ID:** `{safe_slice(st.session_state.dataset_id, 0, 8)}...`
            """)
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("🔄 Refresh", use_container_width=True):
                    st.session_state.metrics = None
                    st.session_state.insights = None
                    st.rerun()
            with col2:
                if st.button("🗑️ Clear", use_container_width=True):
                    if st.session_state.dataset_id:
                        _, error = delete_dataset(st.session_state.dataset_id)
                        if error:
                            st.warning(f"Note: {error}")
                    
                    for key in ['dataset_id', 'filename', 'metrics', 'insights', 'upload_timestamp', 'api_error']:
                        if key in st.session_state:
                            del st.session_state[key]
                    st.rerun()
        
        # Show any API errors
        if st.session_state.api_error:
            st.markdown("---")
            st.markdown(f'<div class="error-message">⚠️ {st.session_state.api_error}</div>', 
                       unsafe_allow_html=True)
        
        # About
        st.markdown("---")
        st.markdown("""
        ### ℹ️ About
        
        **Automated BI** transforms raw data into actionable insights.
        
        **Features:**
        - Automatic data cleaning
        - 20+ business KPIs
        - AI-powered insights
        - Interactive dashboard
        
        **Data Format:**
        - InvoiceNo, StockCode
        - Quantity, UnitPrice  
        - InvoiceDate, CustomerID
        - Country
        """)

def render_kpi_cards(metrics):
    """Safely render KPI cards at top of dashboard"""
    if not metrics:
        return
    
    summary = metrics.get('metrics', {}).get('summary', {})
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown('<div class="kpi-card">', unsafe_allow_html=True)
        st.markdown(f'<div class="metric-value">{format_currency(summary.get("total_revenue"))}</div>', 
                   unsafe_allow_html=True)
        st.markdown('<div class="metric-label">Total Revenue</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="kpi-card">', unsafe_allow_html=True)
        st.markdown(f'<div class="metric-value">{format_number(summary.get("total_transactions"))}</div>', 
                   unsafe_allow_html=True)
        st.markdown('<div class="metric-label">Transactions</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="kpi-card">', unsafe_allow_html=True)
        st.markdown(f'<div class="metric-value">{format_number(summary.get("total_customers"))}</div>', 
                   unsafe_allow_html=True)
        st.markdown('<div class="metric-label">Customers</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col4:
        st.markdown('<div class="kpi-card">', unsafe_allow_html=True)
        st.markdown(f'<div class="metric-value">{format_currency(summary.get("avg_transaction_value"))}</div>', 
                   unsafe_allow_html=True)
        st.markdown('<div class="metric-label">Avg. Transaction</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

def render_revenue_charts(metrics):
    """Safely render revenue visualization charts"""
    if not metrics:
        st.warning("No revenue data available")
        return
    
    revenue_data = metrics.get('metrics', {}).get('revenue', {})
    
    st.markdown('<h2 class="sub-header">📈 Revenue Analytics</h2>', unsafe_allow_html=True)
    
    tab1, tab2, tab3 = st.tabs(["Monthly Trend", "Weekly Pattern", "Distribution"])
    
    with tab1:
        monthly_revenue = revenue_data.get('monthly_revenue', {})
        if monthly_revenue:
            df = pd.DataFrame({
                'Month': list(monthly_revenue.keys()),
                'Revenue': list(monthly_revenue.values())
            })
            
            fig = px.bar(
                df, 
                x='Month', 
                y='Revenue',
                title="Monthly Revenue",
                color_discrete_sequence=['#3B82F6']
            )
            fig.update_layout(height=400, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No monthly revenue data available")
    
    with tab2:
        weekday_revenue = revenue_data.get('weekday_revenue', {})
        if weekday_revenue:
            days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            values = [weekday_revenue.get(day, 0) for day in days]
            
            fig = px.bar(
                x=days,
                y=values,
                title="Revenue by Day of Week",
                labels={'x': 'Day', 'y': 'Revenue ($)'},
                color_discrete_sequence=['#10B981']
            )
            fig.update_layout(height=400, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No weekday revenue data available")
    
    with tab3:
        dist = revenue_data.get('revenue_distribution', {})
        if dist:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Average", format_currency(dist.get('mean', 0)))
                st.metric("Median", format_currency(dist.get('median', 0)))
            
            with col2:
                st.metric("Minimum", format_currency(dist.get('min', 0)))
                st.metric("Maximum", format_currency(dist.get('max', 0)))
            
            with col3:
                st.metric("Std Dev", format_currency(dist.get('std', 0)))
                
            # Percentile chart
            percentiles = dist.get('percentiles', {})
            if percentiles:
                p_df = pd.DataFrame({
                    'Percentile': ['25th', '50th', '75th', '90th'],
                    'Value': [
                        percentiles.get('25', 0),
                        percentiles.get('50', 0),
                        percentiles.get('75', 0),
                        percentiles.get('90', 0)
                    ]
                })
                
                fig = px.line(
                    p_df,
                    x='Percentile',
                    y='Value',
                    title="Revenue Distribution",
                    markers=True
                )
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No distribution data available")

def render_customer_analytics(metrics):
    """Safely render customer analytics section"""
    if not metrics:
        return
    
    customer_data = metrics.get('metrics', {}).get('customer', {})
    
    st.markdown('<h2 class="sub-header">👥 Customer Analytics</h2>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Customer segments
        segments = customer_data.get('segment_distribution', {})
        if segments:
            fig = px.pie(
                values=list(segments.values()),
                names=list(segments.keys()),
                title="Customer Segments",
                color_discrete_sequence=px.colors.qualitative.Set3,
                hole=0.4
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No customer segment data")
    
    with col2:
        # RFM metrics
        rfm = customer_data.get('rfm_summary', {})
        if rfm:
            st.markdown("### RFM Analysis")
            
            metric_col1, metric_col2, metric_col3 = st.columns(3)
            metric_col1.metric("Recency", f"{rfm.get('avg_recency', 0):.0f} days")
            metric_col2.metric("Frequency", f"{rfm.get('avg_frequency', 0):.1f}")
            metric_col3.metric("Monetary", format_currency(rfm.get('avg_monetary', 0)))
            
            # Customer count metrics
            st.markdown("### Customer Base")
            metric_col1, metric_col2 = st.columns(2)
            metric_col1.metric("Total Customers", format_number(customer_data.get('customer_count', 0)))
            metric_col2.metric("One-time Buyers", format_number(customer_data.get('one_time_customers', 0)))
            
            # Top customers table
            top_customers = customer_data.get('top_customers', {})
            if top_customers:
                st.markdown("### Top Customers")
                customers_df = pd.DataFrame([
                    {
                        'Customer': safe_slice(cust_id, 0, 8) + '...',
                        'Spent': format_currency(data.get('total_spent', 0)),
                        'Transactions': data.get('transactions', 0)
                    }
                    for cust_id, data in list(top_customers.items())[:5]
                ])
                st.dataframe(customers_df, use_container_width=True, hide_index=True)
        else:
            st.info("No customer RFM data available")

def render_product_analytics(metrics):
    """Safely render product analytics section"""
    if not metrics:
        return
    
    product_data = metrics.get('metrics', {}).get('product', {})
    
    st.markdown('<h2 class="sub-header">📦 Product Analytics</h2>', unsafe_allow_html=True)
    
    # Top products chart
    top_products = product_data.get('top_products', {})
    if top_products:
        # Convert to DataFrame for visualization
        products_list = []
        for prod_id, data in top_products.items():
            description = safe_string(data.get('description', 'Unknown'), 'Unknown Product')
            products_list.append({
                'Product': description[:30] + '...' if len(description) > 30 else description,
                'Revenue': data.get('total_revenue', 0),
                'Quantity': data.get('total_quantity', 0),
                'Transactions': data.get('transaction_count', 0)
            })
        
        products_df = pd.DataFrame(products_list)
        
        fig = px.bar(
            products_df,
            y='Product',
            x='Revenue',
            title="Top 10 Products by Revenue",
            orientation='h',
            color='Revenue',
            color_continuous_scale='Blues'
        )
        fig.update_layout(height=500, yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig, use_container_width=True)
        
        # Product metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Products", format_number(product_data.get('total_products', 0)))
        with col2:
            st.metric("Avg Price", format_currency(product_data.get('avg_product_price', 0)))
        with col3:
            st.metric("Median Price", format_currency(product_data.get('median_product_price', 0)))
    else:
        st.info("No product performance data available")

def render_ai_insights(insights):
    """Safely render AI-generated insights"""
    if not insights:
        return
    
    insights_data = insights.get('insights', {})
    
    st.markdown('<h2 class="sub-header">🤖 AI-Powered Insights</h2>', unsafe_allow_html=True)
    
    # AI Status
    metadata = insights.get('metadata', {})
    if metadata.get('ai_enabled', False):
        st.success(f"✨ Insights generated using {metadata.get('ai_model', 'OpenAI')}")
    else:
        st.info("📝 Using intelligent mock insights (configure OpenAI API for real AI)")
    
    # Executive Summary
    exec_summary = insights_data.get('executive_summary', '')
    if exec_summary:
        st.markdown("### Executive Summary")
        st.markdown(f'<div class="insight-card">{exec_summary}</div>', unsafe_allow_html=True)
    
    # Key Insights
    key_insights = insights_data.get('key_insights', [])
    if key_insights:
        st.markdown("### Key Insights")
        
        for insight in key_insights[:5]:
            # Color code by impact
            impact_color = {
                'high': '#EF4444',
                'medium': '#F59E0B',
                'low': '#10B981'
            }.get(insight.get('impact', 'medium'), '#6B7280')
            
            confidence = insight.get('confidence', 0.8) * 100
            
            st.markdown(f"""
<div class="insight-card" style="border-left: 4px solid {impact_color};">
    <strong style="font-size: 1.1rem;">🎯 {insight.get('title', 'Insight')}</strong><br>
    <span style="color: {impact_color}; font-size: 0.85rem;">
        Impact: {insight.get('impact', 'medium').upper()} | Confidence: {confidence:.0f}%
    </span><br>
    <p style="margin: 0.6rem 0; color:#D1D5DB;">
        {insight.get('description', '')}
    </p>
    <em style="color:#9CA3AF;">
        💡 Recommendation: {insight.get('recommendation', '')}
    </em>
</div>
""", unsafe_allow_html=True)
    
    # Growth Opportunities and Risks in columns
    col1, col2 = st.columns(2)
    
    with col1:
        opportunities = insights_data.get('growth_opportunities', [])
        if opportunities:
            st.markdown("### 📈 Growth Opportunities")
            for opp in opportunities[:3]:
                st.markdown(f"""
                <div class="success-card">
                    <strong>{opp.get('opportunity', 'Opportunity')}</strong><br>
                    <span>Impact: {opp.get('potential_impact', 'N/A')}</span><br>
                    <span>Effort: {opp.get('effort_required', 'medium').upper()}</span>
                </div>
                """, unsafe_allow_html=True)
    
    with col2:
        risks = insights_data.get('risk_warnings', [])
        if risks:
            st.markdown("### ⚠️ Risk Warnings")
            for risk in risks[:3]:
                severity_color = {
                    'high': '#EF4444',
                    'medium': '#F59E0B',
                    'low': '#6B7280'
                }.get(risk.get('severity', 'medium'), '#6B7280')
                
                st.markdown(f"""
                <div class="risk-card">
                    <strong style="color: {severity_color};">{risk.get('risk', 'Risk')}</strong><br>
                    <span>Severity: {risk.get('severity', 'medium').upper()}</span><br>
                    <span>Mitigation: {risk.get('mitigation', 'N/A')}</span>
                </div>
                """, unsafe_allow_html=True)
    
    # Top Recommendations
    recommendations = insights_data.get('top_recommendations', [])
    if recommendations:
        st.markdown("### ✅ Recommended Actions")
        for i, rec in enumerate(recommendations[:3], 1):
            st.markdown(f"""
            <div style="background: #2b2c4d; padding: 1rem; border-radius: 0.5rem; margin-bottom: 0.5rem;">
                <strong>{i}. {rec}</strong>
            </div>
            """, unsafe_allow_html=True)

def render_export_section(metrics, insights):
    """Render data export section"""
    if not metrics or not insights:
        return
    
    st.markdown('<hr class="custom-divider">', unsafe_allow_html=True)
    st.markdown("### 💾 Export Data")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("📊 Export Metrics (JSON)", use_container_width=True):
            metrics_json = json.dumps(metrics, indent=2, default=str)
            st.download_button(
                label="Download Metrics",
                data=metrics_json,
                file_name=f"metrics_{safe_slice(st.session_state.dataset_id, 0, 8)}.json",
                mime="application/json",
                key="download_metrics"
            )
    
    with col2:
        if st.button("📝 Export Insights (JSON)", use_container_width=True):
            insights_json = json.dumps(insights, indent=2, default=str)
            st.download_button(
                label="Download Insights",
                data=insights_json,
                file_name=f"insights_{safe_slice(st.session_state.dataset_id, 0, 8)}.json",
                mime="application/json",
                key="download_insights"
            )
    
    with col3:
        if st.button("📋 Copy Dataset ID", use_container_width=True):
            st.code(st.session_state.dataset_id, language="text")


# Main Dashboard
def main():
    """Main dashboard application"""
    
    # Render header and sidebar
    render_header()
    render_sidebar()
    
    st.markdown(
    "<span style='color:#9CA3AF;'>Upload your business data and get automated KPIs, AI insights and recommendations — instantly.</span>",
    unsafe_allow_html=True
)
    
    # Check if we have an active dataset
    if not st.session_state.dataset_id:
        # Welcome screen
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.markdown("""
            <div style="text-align: center; padding: 3rem;">
                <h2 style="color: #1F2937; margin-bottom: 1rem;">👋 Welcome to Automated BI</h2>
                <p style="color: #6B7280; font-size: 1.1rem; margin-bottom: 2rem;">
                    Upload your business data or use our sample dataset to get started.
                    The system will automatically clean your data, calculate KPIs, and generate AI insights.
                </p>
                <div style="background: #F3F4F6; padding: 2rem; border-radius: 1rem;">
                    <h3 style="color: #374151;">Getting Started:</h3>
                    <ol style="text-align: left; color: #4B5563;">
                        <li style="margin-bottom: 0.5rem;">1. Use the sidebar to upload a CSV/Excel file or load sample data</li>
                        <li style="margin-bottom: 0.5rem;">2. Wait for automatic processing (10-30 seconds)</li>
                        <li style="margin-bottom: 0.5rem;">3. Explore revenue, customer, and product analytics</li>
                        <li style="margin-bottom: 0.5rem;">4. Review AI-powered insights and recommendations</li>
                        <li style="margin-bottom: 0.5rem;">5. Export reports for your team</li>
                    </ol>
                </div>
            </div>
            """, unsafe_allow_html=True)
        return
    
    # Fetch metrics and insights if needed
    if not st.session_state.metrics:
        with st.spinner("📊 Calculating business metrics..."):
            metrics_response, error = fetch_metrics(st.session_state.dataset_id)
            if error:
                st.session_state.api_error = error
                st.error(f"❌ Failed to load metrics: {error}")
            else:
                st.session_state.metrics = metrics_response
                st.session_state.api_error = None
    
    if st.session_state.metrics and not st.session_state.insights:
        with st.spinner("🤖 Generating AI insights..."):
            insights_response, error = fetch_insights(st.session_state.dataset_id)
            if error:
                st.session_state.api_error = error
                st.warning(f"⚠️ Insights unavailable: {error}")
            else:
                st.session_state.insights = insights_response
    
    # Check if we have metrics
    if not st.session_state.metrics:
        st.warning("Unable to load metrics. Please try again.")
        return
    
    # Render dashboard tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "📈 Revenue Analytics",
        "👥 Customer Analytics",
        "📦 Product Analytics",
        "🤖 AI Insights"
    ])
    
    with tab1:
        render_kpi_cards(st.session_state.metrics)
        render_revenue_charts(st.session_state.metrics)
    
    with tab2:
        render_customer_analytics(st.session_state.metrics)
    
    with tab3:
        render_product_analytics(st.session_state.metrics)
    
    with tab4:
        render_ai_insights(st.session_state.insights)
    
    # Export section
    render_export_section(st.session_state.metrics, st.session_state.insights)
    
    # Footer with SAFE string handling
    st.markdown('<hr class="custom-divider">', unsafe_allow_html=True)
    
    # SAFELY format the timestamp (FIXED: no crash if None)
    timestamp_display = "Unknown"
    if st.session_state.upload_timestamp:
        try:
            timestamp_display = safe_slice(st.session_state.upload_timestamp, 0, 10)
        except:
            timestamp_display = safe_string(st.session_state.upload_timestamp)
    
    st.markdown(
        f"""
        <div style="text-align: center; color: #6B7280; padding: 1rem;">
            Automated BI Platform | Dataset: {safe_string(st.session_state.filename, 'Unknown')} | 
            Last Updated: {timestamp_display}
        </div>
        """,
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()