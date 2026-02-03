from src.downloaders.hsbc_downloader import HSBCDownloader

print("Testing HSBC Downloader (HTML Scraping)")
print("=" * 70)

downloader = HSBCDownloader()
result = downloader.download_all()

print("\n" + "=" * 70)
print("RESULT:")
print(f"Status: {result['status']}")
print(f"Funds processed: {result.get('funds_processed', 0)}")
print(f"Files downloaded: {result.get('files_downloaded', 0)}")
print(f"Files skipped: {result.get('files_skipped', 0)}")
print(f"Files failed: {result.get('files_failed', 0)}")
print(f"Duration: {result.get('duration', 0):.2f}s")

if result['status'] == 'failed':
    print(f"Reason: {result.get('reason', 'Unknown')}")
