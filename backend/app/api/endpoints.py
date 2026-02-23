"""
API Endpoints for Automated BI System
All REST API routes for the platform
"""
from fastapi import APIRouter, UploadFile, File, HTTPException, Depends, Query, Request
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
import pandas as pd
import uuid
import time
from datetime import datetime
import logging
from sqlalchemy import text
from typing import Dict, Any, List, Optional

from app.data.ingestion import data_ingestion
from app.data.cleaning import data_cleaner  # This should now work!
from app.data.kpi_engine import kpi_calculator
from app.ai.insight_generator import insight_generator
from app.db.database import get_db, create_tables
from app.db import crud, models
from app.utils.logger import log_api_request, log_data_processing, get_logger

router = APIRouter()
logger = get_logger(__name__)

# Create database tables on startup
create_tables()

@router.post("/upload")
async def upload_dataset(
    request: Request,
    file: Optional[UploadFile] = File(None),
    use_sample: bool = Query(False, description="Use sample data instead of file"),
    db: Session = Depends(get_db)
):
    """
    Upload a CSV or Excel file for analysis
    
    - **file**: CSV or Excel file
    - **use_sample**: Set to true to use sample retail data
    """
    start_time = time.time()
    dataset_id = str(uuid.uuid4())
    client_host = request.client.host if request.client else None
    user_agent = request.headers.get("user-agent")
    
    try:
        if use_sample:
            # Use sample data
            logger.info(f"Generating sample data for dataset {dataset_id}")
            df = data_ingestion.generate_sample_data(n_rows=10000)
            filename = "sample_retail_data.csv"
            file_type = "sample"
            original_rows = len(df)
            
        else:
            # Validate file upload
            if not file:
                log_api_request(
                    endpoint="/upload",
                    method="POST",
                    status_code=400,
                    response_time_ms=(time.time() - start_time) * 1000,
                    user_agent=user_agent,
                    ip_address=client_host,
                    error="No file uploaded"
                )
                raise HTTPException(status_code=400, detail="No file uploaded")
            
            logger.info(f"Processing upload: {file.filename}")
            content = await file.read()
            filename = file.filename
            
            # Load and validate file
            df, load_result = data_ingestion.load_file(content, filename)
            
            if df is None:
                error_msg = load_result.get('error', load_result.get('errors', ['Unknown error']))
                
                log_api_request(
                    endpoint="/upload",
                    method="POST",
                    status_code=400,
                    response_time_ms=(time.time() - start_time) * 1000,
                    user_agent=user_agent,
                    ip_address=client_host,
                    error=f"File validation failed: {error_msg}"
                )
                
                raise HTTPException(status_code=400, detail=f"File validation failed: {error_msg}")
            
            file_type = "csv" if filename.endswith('.csv') else "excel"
            original_rows = len(df)
        
        # Clean the data
        clean_start = time.time()
        cleaned_df, cleaning_report = data_cleaner.clean_dataset(df)
        clean_time = (time.time() - clean_start) * 1000
        
        # Log data processing
        log_data_processing(
            operation="clean",
            dataset_id=dataset_id,
            rows_processed=original_rows,
            duration_ms=clean_time,
            success=True,
            metadata={
                "rows_removed": original_rows - len(cleaned_df),
                "removal_rate": cleaning_report.get('rows_removed_percentage', 0)
            }
        )
        
        # Store in database
        dataset_data = {
            "dataset_id": dataset_id,
            "filename": filename,
            "original_rows": original_rows,
            "cleaned_rows": len(cleaned_df),
            "file_type": file_type,
            "cleaning_report": cleaning_report
        }
        
        db_dataset = crud.create_dataset(db, dataset_data)
        
        # Calculate metrics immediately
        metrics_start = time.time()
        metrics = kpi_calculator.calculate_all_metrics(cleaned_df)
        metrics_time = (time.time() - metrics_start) * 1000
        
        metrics_data = {
            "dataset_id": dataset_id,
            "metrics": metrics
        }
        
        crud.create_metrics(db, metrics_data)
        
        # Log metrics calculation
        log_data_processing(
            operation="metrics",
            dataset_id=dataset_id,
            rows_processed=len(cleaned_df),
            duration_ms=metrics_time,
            success=True,
            metadata={"metric_count": metrics.get('_metadata', {}).get('metric_count', 0)}
        )
        
        # Calculate total response time
        response_time = (time.time() - start_time) * 1000
        
        log_api_request(
            endpoint="/upload",
            method="POST",
            status_code=200,
            response_time_ms=response_time,
            user_agent=user_agent,
            ip_address=client_host,
            dataset_id=dataset_id
        )
        
        return {
            "dataset_id": dataset_id,
            "status": "success",
            "filename": filename,
            "rows_original": original_rows,
            "rows_cleaned": len(cleaned_df),
            "rows_removed": original_rows - len(cleaned_df),
            "removal_rate": cleaning_report.get('rows_removed_percentage', 0),
            "uploaded_at": datetime.now().isoformat(),
            "cleaning_summary": cleaning_report.get("cleaning_steps", []),
            "next_steps": [
                {"endpoint": f"/api/v1/metrics/{dataset_id}", "description": "View calculated metrics"},
                {"endpoint": f"/api/v1/insights/{dataset_id}", "description": "Generate AI insights"}
            ]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Upload failed: {str(e)}", exc_info=True)
        response_time = (time.time() - start_time) * 1000
        
        log_api_request(
            endpoint="/upload",
            method="POST",
            status_code=500,
            response_time_ms=response_time,
            user_agent=user_agent,
            ip_address=client_host,
            error=str(e)
        )
        
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

# ... rest of the endpoints file remains the same as previously provided

@router.get("/metrics/{dataset_id}")
async def get_metrics(
    request: Request,
    dataset_id: str,
    db: Session = Depends(get_db)
):
    """
    Get calculated business metrics for a dataset
    
    - **dataset_id**: ID of uploaded dataset
    """
    start_time = time.time()
    client_host = request.client.host if request.client else None
    user_agent = request.headers.get("user-agent")
    
    try:
        # Check if dataset exists
        dataset = crud.get_dataset(db, dataset_id)
        if not dataset:
            log_api_request(
                endpoint=f"/metrics/{dataset_id}",
                method="GET",
                status_code=404,
                response_time_ms=(time.time() - start_time) * 1000,
                user_agent=user_agent,
                ip_address=client_host,
                error="Dataset not found"
            )
            raise HTTPException(status_code=404, detail="Dataset not found")
        
        # Get latest metrics
        metrics = crud.get_latest_metrics(db, dataset_id)
        
        if not metrics:
            log_api_request(
                endpoint=f"/metrics/{dataset_id}",
                method="GET",
                status_code=404,
                response_time_ms=(time.time() - start_time) * 1000,
                user_agent=user_agent,
                ip_address=client_host,
                error="Metrics not calculated"
            )
            raise HTTPException(status_code=404, detail="Metrics not calculated. Upload dataset first.")
        
        response_time = (time.time() - start_time) * 1000
        
        log_api_request(
            endpoint=f"/metrics/{dataset_id}",
            method="GET",
            status_code=200,
            response_time_ms=response_time,
            user_agent=user_agent,
            ip_address=client_host,
            dataset_id=dataset_id
        )
        
        return {
            "dataset_id": dataset_id,
            "calculated_at": metrics.calculated_at.isoformat(),
            "metrics": metrics.metrics_details,
            "summary": metrics.metrics_summary,
            "health_scores": metrics.health_scores
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Metrics retrieval failed: {str(e)}", exc_info=True)
        response_time = (time.time() - start_time) * 1000
        
        log_api_request(
            endpoint=f"/metrics/{dataset_id}",
            method="GET",
            status_code=500,
            response_time_ms=response_time,
            user_agent=user_agent,
            ip_address=client_host,
            error=str(e)
        )
        
        raise HTTPException(status_code=500, detail=f"Metrics retrieval failed: {str(e)}")

@router.get("/insights/{dataset_id}")
async def get_insights(
    request: Request,
    dataset_id: str,
    refresh: bool = Query(False, description="Force regenerate insights"),
    db: Session = Depends(get_db)
):
    """
    Get AI-generated insights for a dataset
    
    - **dataset_id**: ID of uploaded dataset
    - **refresh**: Set to true to regenerate insights
    """
    start_time = time.time()
    client_host = request.client.host if request.client else None
    user_agent = request.headers.get("user-agent")
    
    try:
        # Check if dataset exists
        dataset = crud.get_dataset(db, dataset_id)
        if not dataset:
            log_api_request(
                endpoint=f"/insights/{dataset_id}",
                method="GET",
                status_code=404,
                response_time_ms=(time.time() - start_time) * 1000,
                user_agent=user_agent,
                ip_address=client_host,
                error="Dataset not found"
            )
            raise HTTPException(status_code=404, detail="Dataset not found")
        
        # Check for existing insights
        if not refresh:
            existing_insights = crud.get_latest_insights(db, dataset_id)
            if existing_insights:
                response_time = (time.time() - start_time) * 1000
                
                log_api_request(
                    endpoint=f"/insights/{dataset_id}",
                    method="GET",
                    status_code=200,
                    response_time_ms=response_time,
                    user_agent=user_agent,
                    ip_address=client_host,
                    dataset_id=dataset_id
                )
                
                return {
                    "dataset_id": dataset_id,
                    "generated_at": existing_insights.generated_at.isoformat(),
                    "insights": {
                        "executive_summary": existing_insights.executive_summary,
                        "key_insights": existing_insights.key_insights,
                        "top_recommendations": existing_insights.recommendations,
                        "risk_warnings": existing_insights.risk_warnings,
                        "growth_opportunities": existing_insights.growth_opportunities
                    },
                    "metadata": {
                        "ai_model": existing_insights.ai_model_used,
                        "confidence": existing_insights.confidence_score,
                        "cached": True
                    }
                }
        
        # Get metrics first
        metrics_record = crud.get_latest_metrics(db, dataset_id)
        
        if not metrics_record:
            log_api_request(
                endpoint=f"/insights/{dataset_id}",
                method="GET",
                status_code=404,
                response_time_ms=(time.time() - start_time) * 1000,
                user_agent=user_agent,
                ip_address=client_host,
                error="Metrics not found"
            )
            raise HTTPException(
                status_code=404, 
                detail="Metrics not found. Calculate metrics first using /metrics/{dataset_id}"
            )
        
        # Prepare dataset info
        dataset_info = {
            "rows": dataset.cleaned_rows,
            "filename": dataset.filename,
            "uploaded_at": dataset.uploaded_at.isoformat()
        }
        
        # Generate insights
        insights_start = time.time()
        insights = insight_generator.generate_insights(
            metrics_record.metrics_details,
            dataset_info
        )
        insights_time = (time.time() - insights_start) * 1000
        
        # Log insights generation
        log_data_processing(
            operation="insights",
            dataset_id=dataset_id,
            rows_processed=dataset.cleaned_rows,
            duration_ms=insights_time,
            success=True,
            metadata={
                "ai_enabled": insight_generator.use_real_llm,
                "insight_count": len(insights.get('key_insights', []))
            }
        )
        
        # Store in database
        insights_data = {
            "dataset_id": dataset_id,
            "insights": insights,
            "metadata": {
                "has_ai": insight_generator.use_real_llm,
                "refresh": refresh
            }
        }
        
        db_insights = crud.create_insights(db, insights_data)
        
        response_time = (time.time() - start_time) * 1000
        
        log_api_request(
            endpoint=f"/insights/{dataset_id}",
            method="GET",
            status_code=200,
            response_time_ms=response_time,
            user_agent=user_agent,
            ip_address=client_host,
            dataset_id=dataset_id
        )
        
        return {
            "dataset_id": dataset_id,
            "generated_at": db_insights.generated_at.isoformat(),
            "insights": insights,
            "metadata": {
                "ai_model": db_insights.ai_model_used,
                "confidence": db_insights.confidence_score,
                "cached": False,
                "ai_enabled": insight_generator.use_real_llm,
                "generation_time_ms": round(insights_time, 2)
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Insights generation failed: {str(e)}", exc_info=True)
        response_time = (time.time() - start_time) * 1000
        
        log_api_request(
            endpoint=f"/insights/{dataset_id}",
            method="GET",
            status_code=500,
            response_time_ms=response_time,
            user_agent=user_agent,
            ip_address=client_host,
            error=str(e)
        )
        
        raise HTTPException(status_code=500, detail=f"Insights generation failed: {str(e)}")

@router.get("/datasets")
async def list_datasets(
    request: Request,
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=100),
    db: Session = Depends(get_db)
):
    """List all uploaded datasets"""
    start_time = time.time()
    client_host = request.client.host if request.client else None
    user_agent = request.headers.get("user-agent")
    
    try:
        datasets = crud.get_datasets(db, skip=skip, limit=limit)
        
        result = []
        for dataset in datasets:
            has_metrics = crud.get_latest_metrics(db, dataset.id) is not None
            has_insights = crud.get_latest_insights(db, dataset.id) is not None
            
            result.append({
                "dataset_id": dataset.id,
                "filename": dataset.filename,
                "uploaded_at": dataset.uploaded_at.isoformat(),
                "rows": dataset.cleaned_rows,
                "has_metrics": has_metrics,
                "has_insights": has_insights
            })
        
        response_time = (time.time() - start_time) * 1000
        
        log_api_request(
            endpoint="/datasets",
            method="GET",
            status_code=200,
            response_time_ms=response_time,
            user_agent=user_agent,
            ip_address=client_host
        )
        
        return {
            "total": len(result),
            "skip": skip,
            "limit": limit,
            "datasets": result
        }
        
    except Exception as e:
        logger.error(f"List datasets failed: {str(e)}", exc_info=True)
        response_time = (time.time() - start_time) * 1000
        
        log_api_request(
            endpoint="/datasets",
            method="GET",
            status_code=500,
            response_time_ms=response_time,
            user_agent=user_agent,
            ip_address=client_host,
            error=str(e)
        )
        
        raise HTTPException(status_code=500, detail=f"Failed to list datasets: {str(e)}")

@router.delete("/datasets/{dataset_id}")
async def delete_dataset(
    request: Request,
    dataset_id: str,
    db: Session = Depends(get_db)
):
    """Delete a dataset and all associated data"""
    start_time = time.time()
    client_host = request.client.host if request.client else None
    user_agent = request.headers.get("user-agent")
    
    try:
        success = crud.delete_dataset(db, dataset_id)
        
        if not success:
            log_api_request(
                endpoint=f"/datasets/{dataset_id}",
                method="DELETE",
                status_code=404,
                response_time_ms=(time.time() - start_time) * 1000,
                user_agent=user_agent,
                ip_address=client_host,
                error="Dataset not found"
            )
            raise HTTPException(status_code=404, detail="Dataset not found")
        
        response_time = (time.time() - start_time) * 1000
        
        log_api_request(
            endpoint=f"/datasets/{dataset_id}",
            method="DELETE",
            status_code=200,
            response_time_ms=response_time,
            user_agent=user_agent,
            ip_address=client_host,
            dataset_id=dataset_id
        )
        
        return {
            "status": "deleted",
            "dataset_id": dataset_id,
            "deleted_at": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Delete dataset failed: {str(e)}", exc_info=True)
        response_time = (time.time() - start_time) * 1000
        
        log_api_request(
            endpoint=f"/datasets/{dataset_id}",
            method="DELETE",
            status_code=500,
            response_time_ms=response_time,
            user_agent=user_agent,
            ip_address=client_host,
            error=str(e)
        )
        
        raise HTTPException(status_code=500, detail=f"Delete failed: {str(e)}")

@router.get("/health")
async def health_check(
    request: Request,
    db: Session = Depends(get_db)
):
    """Health check endpoint for monitoring"""
    start_time = time.time()
    
    try:
        # Test database connection
        db.execute(text("SELECT 1"))
        db_status = "healthy"
    except Exception as e:
        db_status = f"unhealthy: {str(e)}"
    
    response_time = (time.time() - start_time) * 1000
    
    # Log health check (but don't use log_api_request to avoid circular dependency)
    logger.info(f"Health check completed in {response_time:.2f}ms")
    
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "automated-bi-api",
        "version": "1.0.0",
        "database": db_status,
        "ai_enabled": insight_generator.use_real_llm,
        "response_time_ms": round(response_time, 2),
        "environment": {
            "database_type": "sqlite" if "sqlite" in str(db.get_bind().url) else "postgresql",
            "debug_mode": __debug__
        }
    }