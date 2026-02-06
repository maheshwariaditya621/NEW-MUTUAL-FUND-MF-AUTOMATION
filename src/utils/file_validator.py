"""
File format validator for downloaded files.

Ensures file extensions match actual file formats to prevent Excel errors.
"""

import os
from pathlib import Path
from typing import Optional
from src.config import logger


def get_actual_file_format(file_path: Path) -> Optional[str]:
    """
    Detect actual file format by reading file signature (magic bytes).
    
    Args:
        file_path: Path to file
        
    Returns:
        File extension (.xls, .xlsx, .pdf, .zip) or None if unknown
    """
    try:
        with open(file_path, 'rb') as f:
            header = f.read(8)
        
        # Excel formats
        if header[:2] == b'PK':  # ZIP-based format
            return '.xlsx'  # Modern Excel (Office Open XML)
        elif header[:8] == b'\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1':
            return '.xls'   # Old Excel (OLE2/CFB)
        
        # PDF
        elif header[:4] == b'%PDF':
            return '.pdf'
        
        # ZIP
        elif header[:2] == b'PK':
            return '.zip'
        
        return None
    
    except Exception as e:
        logger.warning(f"Could not detect file format: {str(e)}")
        return None


def validate_and_fix_extension(file_path: Path) -> Path:
    """
    Validate file extension matches actual format, rename if mismatch.
    
    Args:
        file_path: Path to file
        
    Returns:
        Path to file (possibly renamed)
    """
    if not file_path.exists():
        logger.warning(f"File does not exist: {file_path}")
        return file_path
    
    # Get current extension
    current_ext = file_path.suffix.lower()
    
    # Detect actual format
    actual_ext = get_actual_file_format(file_path)
    
    if not actual_ext:
        logger.debug(f"Could not detect format for {file_path.name}, keeping as-is")
        return file_path
    
    # Check for mismatch
    if current_ext != actual_ext:
        logger.warning(f"Extension mismatch: {file_path.name} is actually {actual_ext} format")
        
        # Rename file
        new_path = file_path.with_suffix(actual_ext)
        
        # Handle duplicate names
        if new_path.exists():
            logger.warning(f"Target file already exists: {new_path.name}")
            # Add counter
            counter = 1
            while new_path.exists():
                stem = file_path.stem
                new_path = file_path.parent / f"{stem}_{counter}{actual_ext}"
                counter += 1
        
        os.rename(file_path, new_path)
        logger.info(f"Renamed: {file_path.name} → {new_path.name}")
        
        return new_path
    
    logger.debug(f"Extension correct: {file_path.name}")
    return file_path
