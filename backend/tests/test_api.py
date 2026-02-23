"""
Test suite for API endpoints
Uses isolated test database for each test run
"""
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import io
import pandas as pd
import os
import tempfile
import shutil

from app.main import app
from app.db.database import Base, get_db
from app.core.config import settings

# ----------------------------------------------------------------------
# Test Database Setup - ISOLATED FOR EACH TEST RUN
# ----------------------------------------------------------------------
@pytest.fixture(scope="function")
def test_db():
    """
    Create a temporary test database for each test
    This ensures complete isolation between tests
    """
    # Create temporary directory for test DB
    temp_dir = tempfile.mkdtemp()
    test_db_path = os.path.join(temp_dir, "test.db")
    test_db_url = f"sqlite:///{test_db_path}"
    
    # Create test engine
    engine = create_engine(
        test_db_url,
        connect_args={"check_same_thread": False}
    )
    
    # Create tables
    Base.metadata.create_all(bind=engine)
    
    # Create test session factory
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    
    # Override dependency
    def override_get_db():
        try:
            db = TestingSessionLocal()
            yield db
        finally:
            db.close()
    
    app.dependency_overrides[get_db] = override_get_db
    
    yield  # Run the test
    
    # Cleanup
    app.dependency_overrides.clear()
    shutil.rmtree(temp_dir)

@pytest.fixture
def client(test_db):
    """Create test client with isolated database"""
    return TestClient(app)

# ----------------------------------------------------------------------
# Helper Functions
# ----------------------------------------------------------------------
def create_sample_csv():
    """Create a sample CSV file for testing"""
    df = pd.DataFrame({
        'InvoiceNo': ['INV001', 'INV002', 'INV003'],
        'StockCode': ['SKU001', 'SKU002', 'SKU003'],
        'Description': ['Product A', 'Product B', 'Product C'],
        'Quantity': [5, 10, 3],
        'InvoiceDate': ['2023-01-01', '2023-01-02', '2023-01-03'],
        'UnitPrice': [10.0, 20.0, 15.0],
        'CustomerID': ['C001', 'C002', 'C003'],
        'Country': ['UK', 'USA', 'Germany']
    })
    return df.to_csv(index=False)

def create_malformed_csv():
    """Create a malformed CSV with missing columns"""
    df = pd.DataFrame({
        'InvoiceNo': ['INV001', 'INV002'],
        'Quantity': [5, 10]
        # Missing other columns
    })
    return df.to_csv(index=False)

# ----------------------------------------------------------------------
# Test Health Endpoint
# ----------------------------------------------------------------------
def test_health_check(client):
    """Test health check endpoint"""
    response = client.get("/health")
    assert response.status_code == 200
    
    data = response.json()
    assert data["status"] == "healthy"
    assert "timestamp" in data
    assert "service" in data
    assert "version" in data

def test_root_endpoint(client):
    """Test root endpoint"""
    response = client.get("/")
    assert response.status_code == 200
    
    data = response.json()
    assert "service" in data
    assert "version" in data
    assert "endpoints" in data
    assert data["service"] == "Automated BI API"

# ----------------------------------------------------------------------
# Test Upload Endpoint - DETERMINISTIC
# ----------------------------------------------------------------------
def test_upload_sample_data(client):
    """Test uploading sample data"""
    response = client.post("/api/v1/upload?use_sample=true")
    assert response.status_code == 200
    
    data = response.json()
    assert "dataset_id" in data
    assert data["status"] == "success"
    assert data["rows_cleaned"] > 0
    assert data["filename"] == "sample_retail_data.csv"
    
    # Verify structure
    assert "next_steps" in data
    assert len(data["next_steps"]) == 2
    
    # Store dataset_id for potential follow-up tests
    return data["dataset_id"]

def test_upload_csv_file(client):
    """Test uploading CSV file"""
    # Create sample CSV
    csv_content = create_sample_csv()
    
    files = {'file': ('test.csv', csv_content, 'text/csv')}
    response = client.post("/api/v1/upload", files=files)
    
    assert response.status_code == 200
    data = response.json()
    
    assert "dataset_id" in data
    assert data["status"] == "success"
    assert data["filename"] == "test.csv"
    assert data["rows_original"] == 3
    assert data["rows_cleaned"] == 3
    assert "uploaded_at" in data

def test_upload_excel_file(client):
    """Test uploading Excel file"""
    # Create sample Excel in memory
    df = pd.DataFrame({
        'InvoiceNo': ['INV001', 'INV002'],
        'StockCode': ['SKU001', 'SKU002'],
        'Description': ['Product A', 'Product B'],
        'Quantity': [5, 10],
        'InvoiceDate': ['2023-01-01', '2023-01-02'],
        'UnitPrice': [10.0, 20.0],
        'CustomerID': ['C001', 'C002'],
        'Country': ['UK', 'USA']
    })
    
    excel_buffer = io.BytesIO()
    df.to_excel(excel_buffer, index=False)
    excel_buffer.seek(0)
    
    files = {'file': ('test.xlsx', excel_buffer.getvalue(), 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')}
    response = client.post("/api/v1/upload", files=files)
    
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert data["rows_original"] == 2

def test_upload_invalid_file_type(client):
    """Test uploading invalid file type - FIXED: Check structured error"""
    files = {'file': ('test.txt', b'fake data', 'text/plain')}
    response = client.post("/api/v1/upload", files=files)
    
    assert response.status_code == 400
    data = response.json()
    assert "Unsupported file type" in data["detail"]

def test_upload_malformed_csv(client):
    """Test uploading malformed CSV (missing columns)"""
    csv_content = create_malformed_csv()
    
    files = {'file': ('malformed.csv', csv_content, 'text/csv')}
    response = client.post("/api/v1/upload", files=files)
    
    assert response.status_code == 400
    data = response.json()
    assert "Missing required columns" in data["detail"]

def test_upload_no_file(client):
    """Test upload with no file"""
    response = client.post("/api/v1/upload")
    
    assert response.status_code == 400
    data = response.json()
    assert "No file uploaded" in data["detail"]

# ----------------------------------------------------------------------
# Test Metrics Endpoint - WITH PROPER ISOLATION
# ----------------------------------------------------------------------
def test_get_metrics_success(client):
    """Test getting metrics for existing dataset"""
    # First upload a dataset
    upload_response = client.post("/api/v1/upload?use_sample=true")
    assert upload_response.status_code == 200
    dataset_id = upload_response.json()["dataset_id"]
    
    # Get metrics
    response = client.get(f"/api/v1/metrics/{dataset_id}")
    assert response.status_code == 200
    
    data = response.json()
    assert data["dataset_id"] == dataset_id
    assert "metrics" in data
    assert "summary" in data
    assert "health_scores" in data
    
    # Verify metrics structure
    summary = data.get("summary", {})
    assert "total_revenue" in summary
    assert "total_transactions" in summary
    assert "total_customers" in summary

def test_get_metrics_nonexistent(client):
    """Test getting metrics for non-existent dataset"""
    response = client.get("/api/v1/metrics/nonexistent-id")
    assert response.status_code == 404
    
    data = response.json()
    assert "Dataset not found" in data["detail"]

def test_get_metrics_after_delete(client):
    """Test getting metrics after dataset is deleted"""
    # Upload and get dataset_id
    upload_response = client.post("/api/v1/upload?use_sample=true")
    dataset_id = upload_response.json()["dataset_id"]
    
    # Delete it
    delete_response = client.delete(f"/api/v1/datasets/{dataset_id}")
    assert delete_response.status_code == 200
    
    # Try to get metrics
    response = client.get(f"/api/v1/metrics/{dataset_id}")
    assert response.status_code == 404

# ----------------------------------------------------------------------
# Test Insights Endpoint - DETERMINISTIC (No [200,404] ambiguity)
# ----------------------------------------------------------------------
def test_get_insights_success(client):
    """Test getting insights for dataset - should always work after upload"""
    # Upload creates metrics automatically, so insights should work
    upload_response = client.post("/api/v1/upload?use_sample=true")
    assert upload_response.status_code == 200
    dataset_id = upload_response.json()["dataset_id"]
    
    # Get insights
    response = client.get(f"/api/v1/insights/{dataset_id}")
    assert response.status_code == 200  # FIXED: Deterministic expectation
    
    data = response.json()
    assert data["dataset_id"] == dataset_id
    assert "insights" in data
    assert "metadata" in data
    
    insights = data.get("insights", {})
    assert "executive_summary" in insights
    assert "key_insights" in insights
    assert "top_recommendations" in insights
    
    metadata = data.get("metadata", {})
    assert "ai_model" in metadata
    assert "cached" in metadata

def test_get_insights_with_refresh(client):
    """Test getting insights with refresh parameter"""
    upload_response = client.post("/api/v1/upload?use_sample=true")
    dataset_id = upload_response.json()["dataset_id"]
    
    # Get insights with refresh
    response = client.get(f"/api/v1/insights/{dataset_id}?refresh=true")
    assert response.status_code == 200
    
    data = response.json()
    metadata = data.get("metadata", {})
    assert metadata.get("cached") is False

def test_get_insights_nonexistent(client):
    """Test getting insights for non-existent dataset"""
    response = client.get("/api/v1/insights/nonexistent-id")
    assert response.status_code == 404
    
    data = response.json()
    assert "Dataset not found" in data["detail"]

# ----------------------------------------------------------------------
# Test Dataset Management
# ----------------------------------------------------------------------
def test_list_datasets(client):
    """Test listing datasets"""
    # Upload a couple datasets
    client.post("/api/v1/upload?use_sample=true")
    client.post("/api/v1/upload?use_sample=true")
    
    response = client.get("/api/v1/datasets")
    assert response.status_code == 200
    
    data = response.json()
    assert "datasets" in data
    assert "total" in data
    assert "skip" in data
    assert "limit" in data
    
    # Should have at least the datasets we uploaded
    assert len(data["datasets"]) >= 2
    
    # Check dataset structure
    for dataset in data["datasets"]:
        assert "dataset_id" in dataset
        assert "filename" in dataset
        assert "uploaded_at" in dataset
        assert "has_metrics" in dataset
        assert "has_insights" in dataset

def test_list_datasets_with_pagination(client):
    """Test listing datasets with pagination"""
    # Upload multiple datasets
    for _ in range(5):
        client.post("/api/v1/upload?use_sample=true")
    
    # Test pagination
    response = client.get("/api/v1/datasets?skip=2&limit=2")
    assert response.status_code == 200
    
    data = response.json()
    assert data["skip"] == 2
    assert data["limit"] == 2
    assert len(data["datasets"]) <= 2

def test_delete_dataset(client):
    """Test deleting a dataset"""
    # Upload a dataset
    upload_response = client.post("/api/v1/upload?use_sample=true")
    dataset_id = upload_response.json()["dataset_id"]
    
    # Delete it
    response = client.delete(f"/api/v1/datasets/{dataset_id}")
    assert response.status_code == 200
    
    data = response.json()
    assert data["status"] == "deleted"
    assert data["dataset_id"] == dataset_id
    assert "deleted_at" in data
    
    # Verify it's gone
    get_response = client.get(f"/api/v1/metrics/{dataset_id}")
    assert get_response.status_code == 404

def test_delete_nonexistent_dataset(client):
    """Test deleting non-existent dataset"""
    response = client.delete("/api/v1/datasets/nonexistent-id")
    assert response.status_code == 404
    
    data = response.json()
    assert "Dataset not found" in data["detail"]

# ----------------------------------------------------------------------
# Test Complete Flow Integration
# ----------------------------------------------------------------------
def test_complete_api_flow(client):
    """Test complete API flow from upload to insights"""
    
    # Step 1: Upload
    upload_response = client.post("/api/v1/upload?use_sample=true")
    assert upload_response.status_code == 200
    dataset_id = upload_response.json()["dataset_id"]
    
    # Step 2: Verify in list
    list_response = client.get("/api/v1/datasets")
    assert list_response.status_code == 200
    datasets = list_response.json()["datasets"]
    assert any(d["dataset_id"] == dataset_id for d in datasets)
    
    # Step 3: Get metrics
    metrics_response = client.get(f"/api/v1/metrics/{dataset_id}")
    assert metrics_response.status_code == 200
    metrics_data = metrics_response.json()
    assert metrics_data["summary"]["total_revenue"] > 0
    
    # Step 4: Get insights
    insights_response = client.get(f"/api/v1/insights/{dataset_id}")
    assert insights_response.status_code == 200
    insights_data = insights_response.json()
    assert len(insights_data["insights"]["key_insights"]) > 0
    
    # Step 5: Delete
    delete_response = client.delete(f"/api/v1/datasets/{dataset_id}")
    assert delete_response.status_code == 200

# ----------------------------------------------------------------------
# Test Error Handling
# ----------------------------------------------------------------------
def test_metrics_endpoint_with_invalid_id_format(client):
    """Test metrics endpoint with malformed ID"""
    response = client.get("/api/v1/metrics/!!!invalid!!!")
    # Should still return 404, not crash
    assert response.status_code == 404

def test_concurrent_requests(client):
    """Test handling multiple requests (simulate concurrency)"""
    # Make multiple upload requests
    responses = []
    for _ in range(3):
        response = client.post("/api/v1/upload?use_sample=true")
        responses.append(response)
    
    # All should succeed
    for response in responses:
        assert response.status_code == 200

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])