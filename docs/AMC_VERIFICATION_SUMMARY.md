# AMC Verification Summary Dashboard

Generated at: 2026-02-14 12:05:59
Source CSV: /Users/yash.zanwar/NEW-MUTUAL-FUND-MF-AUTOMATION/docs/AMC_VERIFICATION_RESULTS_AUTO.csv

## Overall

- Total AMCs: 10
- Pass: 9
- Fail: 1
- Skipped: 0
- Missing Input: 0
- Unknown: 0

## AMC Status Table

| AMC | Year | Month | Merged File | Dry Run | Loader | Final Status | Primary Error/Note |
|---|---:|---:|---|---|---|---|---|
| hdfc | 2026 | 01 | YES | failed | - | FAIL | BaseExtractor.validate_nav_completeness() missing 1 required positional argument: 'scheme_name' |
| sbi | 2026 | 01 | YES | success | - | PASS | - |
| icici | 2026 | 01 | YES | success | - | PASS | - |
| hsbc | 2026 | 01 | YES | success | - | PASS | - |
| kotak | 2026 | 01 | YES | success | - | PASS | - |
| ppfas | 2026 | 01 | YES | success | - | PASS | - |
| axis | 2026 | 01 | YES | success | - | PASS | - |
| bajaj | 2026 | 01 | YES | success | - | PASS | - |
| absl | 2026 | 01 | YES | success | - | PASS | - |
| angelone | 2026 | 01 | YES | success | - | PASS | - |

## Action Queue

### Failed Runs
- hdfc: BaseExtractor.validate_nav_completeness() missing 1 required positional argument: 'scheme_name'
