"""
Project Foundation Demo
========================
Demonstrates the logging system and configuration management.

Run this script to see:
- Colorized log output
- Configuration validation
- Example of how modules will use the logging system
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from log_system.logger import get_logger
from config.settings import config

def main():
    """Run the demonstration."""
    
    print("\n" + "="*80)
    print("MUTUAL FUND DATA PLATFORM - FOUNDATION DEMO")
    print("="*80 + "\n")
    
    # ============================================================
    # 1. CONFIGURATION DEMO
    # ============================================================
    print("📋 CONFIGURATION")
    print("-" * 80)
    print(config)
    print()
    
    # Validate configuration
    errors = config.validate()
    if errors:
        print("⚠️  Configuration Validation Warnings:")
        for error in errors:
            print(f"  - {error}")
    else:
        print("✅ Configuration validation passed")
    
    print()
    
    # ============================================================
    # 2. LOGGING DEMO
    # ============================================================
    print("📝 LOGGING SYSTEM DEMO")
    print("-" * 80)
    print("Simulating a typical pipeline execution...\n")
    
    # Create loggers for different modules
    pipeline_logger = get_logger("ingestion.pipeline")
    extractor_logger = get_logger("extractors.hdfc")
    validation_logger = get_logger("validation.rules")
    loader_logger = get_logger("loaders.postgres")
    
    # Simulate pipeline execution
    pipeline_logger.info("Starting monthly portfolio ingestion for HDFC MF")
    pipeline_logger.info("Processing file: hdfc_jan_2026.xlsx")
    
    extractor_logger.info("Reading Excel file...")
    extractor_logger.info("Extracting data from sheet: Portfolio Holdings")
    extractor_logger.success("Successfully extracted 1,245 rows from Excel")
    
    pipeline_logger.info("Standardizing data...")
    pipeline_logger.success("Data standardization completed")
    
    validation_logger.info("Running validation checks...")
    validation_logger.warning("3 rows have missing ISIN codes, flagged for review")
    validation_logger.warning("2 rows have unusual portfolio percentages")
    validation_logger.info("Validation completed: 1,240 rows passed, 5 rows flagged")
    
    # Simulate a decision point
    pipeline_logger.info("Strict validation mode: ENABLED")
    pipeline_logger.warning("5 rows failed validation - rejecting entire batch")
    pipeline_logger.error("Pipeline stopped: validation failed")
    
    print()
    print("-" * 80)
    print("Demo completed! This is how logs will appear during actual execution.")
    print()
    
    # ============================================================
    # 3. PROJECT STRUCTURE
    # ============================================================
    print("📁 PROJECT STRUCTURE")
    print("-" * 80)
    print("The following modules are ready:")
    print()
    
    modules = [
        ("ingestion", "Pipeline orchestration"),
        ("extractors", "AMC-specific Excel parsers"),
        ("standardisation", "Data transformation rules"),
        ("validation", "Quality validation rules"),
        ("loaders", "Database loading logic"),
        ("database", "Schema & migrations"),
        ("config", "Environment configuration"),
        ("log_system", "Centralized logging system"),
        ("alerts", "Telegram notifications"),
        ("analytics", "Reporting & insights"),
        ("streamlit_app", "Web-based UI"),
        ("scripts", "Utility scripts"),
        ("tests", "Unit & integration tests"),
        ("docs", "Documentation"),
    ]
    
    for module_name, description in modules:
        module_path = project_root / module_name
        status = "✅" if module_path.exists() else "❌"
        print(f"  {status} {module_name:20s} - {description}")
    
    print()
    print("="*80)
    print("Foundation is ready! Next: Implement business logic.")
    print("="*80)
    print()


if __name__ == "__main__":
    main()
