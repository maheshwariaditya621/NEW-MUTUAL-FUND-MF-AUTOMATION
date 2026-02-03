"""
Test HSBC Corruption Recovery
"""

import shutil
from pathlib import Path
from src.downloaders.hsbc_downloader import HSBCDownloader

print("=" * 70)
print("HSBC CORRUPTION RECOVERY TEST")
print("=" * 70)
print()

# Setup: Create incomplete folder for Oct 2024
test_month_folder = Path("data/raw/hsbc/2024_10")

# Clean up any existing folder
if test_month_folder.exists():
    print(f"Removing existing folder: {test_month_folder}")
    shutil.rmtree(test_month_folder)

# Create incomplete folder with fake files
test_month_folder.mkdir(parents=True, exist_ok=True)
(test_month_folder / "fake_file_1.xlsx").write_text("incomplete data")
(test_month_folder / "fake_file_2.xlsx").write_text("incomplete data")

print(f"✅ Created incomplete folder: {test_month_folder}")
print(f"   Files: {[f.name for f in test_month_folder.iterdir()]}")
print()

# Verify no _SUCCESS.json
success_marker = test_month_folder / "_SUCCESS.json"
print(f"_SUCCESS.json exists: {success_marker.exists()}")
print()

print("=" * 70)
print("RUNNING DOWNLOADER (Oct 2024)")
print("=" * 70)
print("Expected behavior:")
print("1. Detect incomplete folder (no _SUCCESS.json)")
print("2. Move to _corrupt/2024_10_TIMESTAMP/")
print("3. Download fresh")
print("4. Create _SUCCESS.json")
print()

# Run downloader
downloader = HSBCDownloader()
result = downloader.download(year=2024, month=10)

print()
print("=" * 70)
print("RESULT")
print("=" * 70)
print(f"Status: {result['status']}")
print(f"Files downloaded: {result.get('files_downloaded', 0)}")
print(f"Duration: {result.get('duration', 0):.2f}s")
print()

# Check for corrupt folder
corrupt_dir = Path("data/raw/hsbc/_corrupt")
if corrupt_dir.exists():
    corrupt_folders = list(corrupt_dir.glob("2024_10_*"))
    if corrupt_folders:
        print(f"✅ Corruption detected and handled!")
        print(f"   Corrupt folder: {corrupt_folders[0]}")
        print(f"   Files in corrupt: {[f.name for f in corrupt_folders[0].iterdir()]}")
    else:
        print("⚠️  No corrupt folder found")
else:
    print("⚠️  _corrupt directory doesn't exist")

print()

# Check new folder
if test_month_folder.exists():
    new_files = list(test_month_folder.glob("*.xlsx"))
    success_exists = success_marker.exists()
    print(f"New folder exists: {test_month_folder.exists()}")
    print(f"Files in new folder: {len(new_files)}")
    print(f"_SUCCESS.json created: {success_exists}")
    
    if result['status'] == 'success' and success_exists:
        print()
        print("✅ CORRUPTION RECOVERY TEST PASSED")
    elif result['status'] == 'not_published':
        print()
        print("⚠️  Month not published yet (Oct 2024)")
    else:
        print()
        print(f"❌ Test result: {result['status']}")
else:
    print("Folder was cleaned up (likely not_published)")

print()
print("=" * 70)
