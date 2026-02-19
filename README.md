🚧 Project under active development


# Automated Business Intelligence SaaS Platform

![Version](https://img.shields.io/badge/version-1.0.0-blue)
![Python](https://img.shields.io/badge/python-3.11+-green)
![FastAPI](https://img.shields.io/badge/FastAPI-0.104.1-teal)
![Streamlit](https://img.shields.io/badge/Streamlit-1.28.1-red)

## 📋 Overview

**Automated BI** is a production-grade SaaS platform that transforms raw business data into actionable insights automatically. Unlike traditional BI tools requiring manual report building, this system performs end-to-end analysis:

1. **Ingests** raw CSV/Excel files
2. **Cleans** data automatically (nulls, types, duplicates)
3. **Calculates** business KPIs (revenue, growth, customer metrics)
4. **Generates** AI-powered insights using LLMs
5. **Visualizes** results in a professional dashboard
6. **Exposes** REST APIs for integration

## 🏗️ Architecture


## ✨ Features

✅ **Zero-touch data processing** - Upload and forget  
✅ **20+ automated KPIs** - Revenue, growth, customer segments, product performance  
✅ **AI-generated insights** - Plain English explanations with recommendations  
✅ **Interactive dashboard** - Real-time visualizations with Plotly  
✅ **REST API** - Integration-ready endpoints  
✅ **Production deployment** - Render/Streamlit Cloud ready  

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| Backend | FastAPI | REST API framework |
| Data Processing | Pandas, NumPy | Data cleaning, KPI calculation |
| AI Insights | OpenAI GPT | Natural language insights |
| Database | SQLite/PostgreSQL | Data persistence |
| Frontend | Streamlit | Interactive dashboard |
| Visualization | Plotly | Business charts |
| Deployment | Render, Streamlit Cloud | Hosting |

## 🚀 Quick Start

### Prerequisites
- Python 3.11+
- pip
- (Optional) OpenAI API key

### Installation

```bash
# 1. Clone repository
git clone https://github.com/yourusername/automated-bi-saas
cd automated-bi-saas

# 2. Set up virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 3. Install backend dependencies
pip install -r backend/requirements.txt

# 4. Set up environment variables
cp .env.example .env
# Edit .env with your configuration

# 5. Run backend server
cd backend
uvicorn app.main:app --reload

# 6. In new terminal: Run frontend
cd frontend
pip install -r requirements.txt
streamlit run dashboard.py