"""
Test clean_company_name logic with actual ICICI data.
"""
import pandas as pd
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent))

from src.extractors.icici_extractor_v1 import ICICIExtractorV1

extractor = ICICIExtractorV1()

# Test data
test_names = [
    "Larsen & Toubro Ltd.",
    "ITC Ltd.",
    "State Bank Of India",
    "N/A",
    None,
    "",
    "   ",
    "nan"
]

print("Testing clean_company_name():\n")
for name in test_names:
    cleaned = extractor.clean_company_name(name)
    print(f"  Input: '{name}' → Output: '{cleaned}'")
