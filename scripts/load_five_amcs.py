from src.extractors.orchestrator import ExtractionOrchestrator

def main():
    orchestrator = ExtractionOrchestrator()
    amcs = ['abakkus', 'boi', 'edelweiss', 'iti', 'samco']
    year = 2026
    month = 9

    print("=" * 60)
    print("STEP 1: DRY RUN EXTRACTION ON OCI")
    print("=" * 60)
    for amc in amcs:
        print(f"\n--- Running Dry-Run for {amc.upper()} ({year}-{month:02d}) ---")
        try:
            res = orchestrator.process_amc_month(amc, year, month, dry_run=True, redo=True)
            print(f"Result {amc}: {res}")
        except Exception as e:
            print(f"Error {amc}: {e}")

    print("\n" + "=" * 60)
    print("STEP 2: LIVE DATABASE LOAD ON OCI")
    print("=" * 60)
    for amc in amcs:
        print(f"\n--- Loading {amc.upper()} into Database ({year}-{month:02d}) ---")
        try:
            res = orchestrator.process_amc_month(amc, year, month, dry_run=False, redo=True)
            print(f"Result {amc}: {res}")
        except Exception as e:
            print(f"Error {amc}: {e}")

if __name__ == "__main__":
    main()
