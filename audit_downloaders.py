import os
import re
from pathlib import Path

DOWNLOADERS_DIR = Path(r"d:\CODING\NEW MUTUAL FUND MF AUTOMATION\src\downloaders")
BASE_DOWNLOADER = "base_downloader.py"
HDFC_DOWNLOADER = "hdfc_downloader.py"

def audit_downloaders():
    violations = []
    
    for file_path in DOWNLOADERS_DIR.glob("*.py"):
        if file_path.name in [BASE_DOWNLOADER, HDFC_DOWNLOADER, "__init__.py"]:
            continue
            
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
            
        # Check for constructed filenames used in saving
        # Playwright pattern: dl.save_as(some_path)
        # Requests pattern: with open(some_path, "wb") as f:
        
        # Look for save_as or open with a name that isn't 'name' or 'original_filename'
        # This is a bit fuzzy, so we'll look for strings like 'f"AMC_...' or 'f"{amc}_...'
        
        renaming_patterns = [
            r'fname\s*=\s*f"',
            r'filename\s*=\s*f"',
            r'path\s*=\s*target_dir\s*/\s*f"',
            r'save_as\(.*f".*\)',
            r'open\(.*f".*,\s*"wb"\)'
        ]
        
        found_violations = []
        for pattern in renaming_patterns:
            matches = re.finditer(pattern, content)
            for match in matches:
                # Get line number
                line_no = content.count('\n', 0, match.start()) + 1
                found_violations.append(f"Line {line_no}: {match.group(0)}")
        
        if found_violations:
            violations.append((file_path.name, found_violations))
            
    return violations

if __name__ == "__main__":
    results = audit_downloaders()
    for filename, v_list in results:
        print(f"--- {filename} ---")
        for v in v_list:
            print(v)
        print()
