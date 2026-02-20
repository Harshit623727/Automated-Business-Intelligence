"""
Data Validation Utilities for Automated BI System
Reusable validation functions for data processing
"""
import re
from typing import Dict, List, Any, Optional
from datetime import datetime

def validate_email(email: str) -> bool:
    """Validate email format"""
    pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    return bool(re.match(pattern, email))

def validate_date(date_str: str, formats: List[str] = None) -> bool:
    """
    Validate if string can be parsed as date
    
    Args:
        date_str: Date string to validate
        formats: List of expected formats (default: common formats)
    """
    if formats is None:
        formats = [
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%m/%d/%Y",
            "%Y-%m-%d %H:%M:%S",
            "%d/%m/%Y %H:%M:%S"
        ]
    
    for fmt in formats:
        try:
            datetime.strptime(date_str, fmt)
            return True
        except ValueError:
            continue
    return False

def validate_numeric(value: Any) -> bool:
    """Check if value can be converted to numeric"""
    try:
        float(value)
        return True
    except (ValueError, TypeError):
        return False

def validate_required_columns(df_columns: List[str], 
                            required_columns: List[str]) -> Dict[str, Any]:
    """
    Validate that all required columns are present
    
    Returns:
        Dict with validation result and missing columns
    """
    missing = [col for col in required_columns if col not in df_columns]
    
    return {
        "is_valid": len(missing) == 0,
        "missing_columns": missing,
        "present_columns": [col for col in required_columns if col in df_columns]
    }

def sanitize_text(text: str) -> str:
    """Remove potentially dangerous characters from text"""
    if not isinstance(text, str):
        return str(text) if text is not None else ""
    
    # Remove control characters
    cleaned = re.sub(r"[\x00-\x1f\x7f]", "", text)
    # Trim whitespace
    cleaned = cleaned.strip()
    # Normalize whitespace
    cleaned = re.sub(r"\s+", " ", cleaned)
    
    return cleaned

def validate_positive_number(value: float, allow_zero: bool = False) -> bool:
    """Validate if number is positive"""
    if allow_zero:
        return value >= 0
    return value > 0