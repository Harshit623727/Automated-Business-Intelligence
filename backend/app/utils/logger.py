"""
Logging Configuration for Automated BI System
Structured logging with JSON format for production
"""
import logging
import sys
import json
from datetime import datetime
from typing import Dict, Any, Optional
import traceback
from pathlib import Path

class JSONFormatter(logging.Formatter):
    """Format log records as JSON for structured logging"""
    
    def format(self, record: logging.LogRecord) -> str:
        """Convert log record to JSON string"""
        log_object = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "message": record.getMessage(),
        }
        
        # Add exception details if present
        if record.exc_info:
            log_object["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": traceback.format_exception(*record.exc_info)
            }
        
        # Add custom fields
        for key, value in record.__dict__.items():
            if key not in (
                "name", "msg", "args", "levelname", "levelno", "pathname",
                "filename", "module", "exc_info", "exc_text", "stack_info",
                "lineno", "funcName", "created", "msecs", "relativeCreated",
                 "thread", "threadName", "processName", "process"
            ):
                log_object[key] = value
            
        
        return json.dumps(log_object)

def setup_logging(level: str = "INFO", log_file: Optional[Path] = None) -> None:
    """
    Configure application logging
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_file: Optional file path for persistent logs
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))
    
    # Clear existing handlers
    root_logger.handlers.clear()
    
    # Console handler with JSON formatting for production
    console_handler = logging.StreamHandler(sys.stdout)
    
    if level.upper() == "DEBUG":
        # Human-readable format for development
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
    else:
        # JSON format for production
        formatter = JSONFormatter()
    
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # Optional file handler
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    # Set third-party loggers to WARNING
    logging.getLogger("uvicorn").setLevel(logging.WARNING)
    logging.getLogger("fastapi").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)

def get_logger(name: str) -> logging.Logger:
    """Get configured logger instance"""
    return logging.getLogger(name)

# =====================================================================
# NEW FUNCTION: API Request Logging (Previously Missing!)
# =====================================================================
def log_api_request(
    endpoint: str,
    method: str,
    status_code: int,
    response_time_ms: float,
    user_agent: Optional[str] = None,
    ip_address: Optional[str] = None,
    dataset_id: Optional[str] = None,
    error: Optional[str] = None
) -> None:
    """
    Log API request details for monitoring and debugging
    
    Args:
        endpoint: API endpoint called
        method: HTTP method (GET, POST, etc.)
        status_code: HTTP response status code
        response_time_ms: Response time in milliseconds
        user_agent: Client user agent string
        ip_address: Client IP address
        dataset_id: Optional dataset ID for context
        error: Optional error message if request failed
    """
    logger = get_logger("api")
    
    log_data = {
        "type": "api_request",
        "endpoint": endpoint,
        "method": method,
        "status_code": status_code,
        "response_time_ms": round(response_time_ms, 2),
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }
    
    # Add optional fields if provided
    if user_agent:
        log_data["user_agent"] = user_agent[:100]  # Truncate to prevent log bloat
    
    if ip_address:
        log_data["ip_address"] = ip_address
    
    if dataset_id:
        log_data["dataset_id"] = dataset_id
    
    if error:
        log_data["error"] = error
    
    # Log at appropriate level based on status code
    if status_code >= 500:
        logger.error(f"API error: {method} {endpoint} - {status_code}", extra=log_data)
    elif status_code >= 400:
        logger.warning(f"API client error: {method} {endpoint} - {status_code}", extra=log_data)
    else:
        logger.info(f"API request: {method} {endpoint} - {status_code}", extra=log_data)

def log_data_processing(
    operation: str,
    dataset_id: str,
    rows_processed: int,
    duration_ms: float,
    success: bool,
    error: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None
) -> None:
    """
    Log data processing operations (upload, clean, metrics, insights)
    
    Args:
        operation: Type of operation (upload, clean, metrics, insights)
        dataset_id: Dataset identifier
        rows_processed: Number of rows processed
        duration_ms: Processing duration in milliseconds
        success: Whether operation succeeded
        error: Error message if failed
        metadata: Additional operation-specific metadata
    """
    logger = get_logger("data_processing")
    
    log_data = {
        "type": "data_processing",
        "operation": operation,
        "dataset_id": dataset_id,
        "rows_processed": rows_processed,
        "duration_ms": round(duration_ms, 2),
        "success": success,
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }
    
    if error:
        log_data["error"] = error
    
    if metadata:
        log_data["metadata"] = metadata
    
    if success:
        logger.info(f"Data processing: {operation} completed", extra=log_data)
    else:
        logger.error(f"Data processing: {operation} failed", extra=log_data)