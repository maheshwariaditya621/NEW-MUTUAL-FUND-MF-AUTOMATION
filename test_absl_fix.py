from src.downloaders.absl_downloader import ABSLDownloader
import traceback
import sys

# Redirect output to file
with open('test_absl_output.txt', 'w') as f:
    sys.stdout = f
    sys.stderr = f
    
    downloader = ABSLDownloader()
    
    try:
        result = downloader.download(2024, 11)
        print("\n=== RESULT ===")
        print(result)
    except Exception as e:
        print("\n=== ERROR ===")
        print(f"Error: {e}")
        print("\n=== TRACEBACK ===")
        traceback.print_exc()

print("Output written to test_absl_output.txt")
