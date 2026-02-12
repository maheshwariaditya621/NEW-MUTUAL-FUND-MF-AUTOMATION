def analyze():
    log_path = "debug_sbi.log"
    print(f"Analyzing {log_path}...")
    
    try:
        # Try UTF-16LE first (PowerShell default for >)
        with open(log_path, "r", encoding="utf-16le") as f:
            lines = f.readlines()
    except:
        # Fallback
        with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
            
    first_abort_idx = -1
    for i, line in enumerate(lines):
        if "current transaction is aborted" in line:
            first_abort_idx = i
            break
            
    if first_abort_idx != -1:
        print(f"First Abort found at line {first_abort_idx}")
        start = max(0, first_abort_idx - 50)
        end = first_abort_idx + 5
        print("--- CONTEXT ---")
        for j in range(start, end):
            prefix = ">> " if j == first_abort_idx else "   "
            print(f"{prefix}{j}: {lines[j].strip()}")
    else:
        print("No 'current transaction is aborted' found.")

if __name__ == "__main__":
    analyze()
