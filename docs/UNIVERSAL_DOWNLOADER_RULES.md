# 🔒 UNIVERSAL DOWNLOADER RULES (NON-NEGOTIABLE)

**Applies to ALL AMC Downloaders** — API-based and Playwright-based

---

## 1️⃣ Filename Immutability (ABSOLUTE RULE)

### Rule
**You MUST NOT rename any downloaded file under any circumstance.**

### Requirements
- ✅ Preserve the **exact original filename** provided by the AMC
- ✅ Same name, extension, casing, spacing — **no modification at all**
- ✅ No prefixes, suffixes, normalization, or inferred naming
- ✅ Applies to **ALL AMCs** (API-based and Playwright-based)

### Enforcement
- ❌ Renaming is **strictly forbidden** in the downloader layer
- ✅ Any normalization is allowed **only downstream** (extraction / processed data)

### Implementation

#### API-Based Downloaders (e.g., HDFC, Quantum)
```python
# ✅ CORRECT: Use original filename from API response
file_obj = file_item["file"]
url = file_obj["url"]
original_filename = file_obj["filename"]  # or "OriginalFileName"
save_path = target_dir / original_filename  # NO MODIFICATION

# Save with exact original name
with open(save_path, "wb") as fp:
    fp.write(response.content)
```

#### Playwright-Based Downloaders (e.g., PPFAS, LIC, Wealth Company)
```python
# ✅ CORRECT: Use suggested_filename from download
with page.expect_download(timeout=60000) as download_info:
    link_element.click()

download = download_info.value
original_filename = download.suggested_filename  # EXACT NAME FROM AMC
save_path = target_dir / original_filename  # NO MODIFICATION

download.save_as(str(save_path))
```

### ❌ VIOLATIONS (Examples from Current Codebase)

```python
# ❌ WRONG: PPFAS renaming (Line 165 in ppfas_downloader.py)
filename = f"PPFAS_Monthly_Portfolio_Report_{target_month_name}_30_{target_year}{ext}"

# ❌ WRONG: Wealth Company renaming (Line 272 in wealth_company_downloader.py)
fname = f"WEALTH_COMPANY_{month_abbr}_{target_year}_{idx+1:02d}_{base_name}{ext}"

# ❌ WRONG: Any form of normalization, prefixing, or suffixing
```

---

## 2️⃣ No Duplicate Files Per (AMC, Year, Month)

### Rule
**For a given AMC + year + month, the downloader MUST ensure no duplicate files exist from our end.**

### Requirements
- ✅ The same filename must **NOT be written twice** into the same `YYYY_MM` folder
- ✅ If a filename already exists in the target month folder:
  - **Do not download** or write it again
  - **Do not create renamed variants**
  - **Log and skip safely**

### Clarification
This rule is about **our system not creating duplicates**, regardless of AMC behavior.

### Implementation

```python
# Before downloading each file
original_filename = download.suggested_filename  # or from API
save_path = target_dir / original_filename

# ✅ DUPLICATE CHECK
if save_path.exists():
    logger.warning(f"File already exists, skipping: {original_filename}")
    continue  # Skip this file, do NOT rename or overwrite

# Proceed with download only if file doesn't exist
download.save_as(str(save_path))
logger.success(f"Saved: {original_filename}")
```

### Edge Cases

#### Multiple Downloads in Same Session
If the AMC returns the same filename multiple times in the same API response or page:
- ✅ Download only the **first occurrence**
- ✅ Skip subsequent duplicates with a warning log
- ❌ Do NOT create `filename_1.xlsx`, `filename_2.xlsx`, etc.

#### Retry Logic
If a download fails and is retried:
- ✅ The duplicate check will prevent re-downloading if the file was partially saved
- ✅ Corruption handling (missing `_SUCCESS.json`) will move incomplete folders to `_corrupt/`
- ✅ On retry, the folder is clean, so no duplicates

---

## 3️⃣ Architectural Boundaries (DO NOT CHANGE)

### Immutable Components
The following are **NOT up for modification** as part of this compliance effort:

- ✅ Folder structure remains unchanged: `data/raw/{amc}/YYYY_MM/`
- ✅ `_SUCCESS.json` semantics remain unchanged
- ✅ Idempotency, atomicity, retry logic, corruption handling remain unchanged
- ✅ Scheduler behavior remains unchanged
- ✅ Downloader remains **raw ingestion only** (no transformation)

### Scope
These rules are **additive constraints only**, not permission to refactor.

---

## 4️⃣ Final Enforcement Statement

### Compliance Criteria
Any downloader that:
1. **Renames files**, or
2. **Creates duplicate files** for the same month

...is **non-compliant** and **MUST be rejected**.

### Audit Checklist
For each downloader, verify:
- [ ] Original filename is preserved exactly (no renaming)
- [ ] Duplicate check exists before saving each file
- [ ] Duplicate files are skipped (not renamed or overwritten)
- [ ] Logging indicates when duplicates are skipped

---

## 5️⃣ Migration Guide for Existing Downloaders

### Step 1: Identify Violations
Search for patterns like:
```python
# Renaming patterns
f"{AMC_NAME}_{month}_{year}_{filename}"
f"normalized_{filename}"
filename.replace(...)
```

### Step 2: Remove Renaming Logic
Replace with:
```python
original_filename = download.suggested_filename  # Playwright
# or
original_filename = file_obj["filename"]  # API
save_path = target_dir / original_filename  # NO CHANGES
```

### Step 3: Add Duplicate Detection
Before every file save:
```python
if save_path.exists():
    logger.warning(f"Duplicate file skipped: {original_filename}")
    continue
```

### Step 4: Test
- Run downloader for a month that has already been downloaded
- Verify no new files are created
- Verify no renamed variants appear
- Check logs for duplicate skip messages

---

## 6️⃣ Gold Standard Reference: HDFC Downloader

The HDFC downloader is the **reference implementation** for these rules.

### Key Compliance Points in HDFC

#### Filename Preservation (Line 336)
```python
name = file_obj["filename"]  # Original from API
path = target_dir / name      # No modification
```

#### Implicit Duplicate Handling
HDFC's API returns a deterministic list of files. If run twice:
- First run: Downloads all files, creates `_SUCCESS.json`
- Second run: Skips entirely due to existing `_SUCCESS.json` (idempotency)

This is **folder-level duplicate prevention**, which is already implemented.

### What's New
The new rules add **file-level duplicate prevention** for cases where:
- A downloader might be interrupted mid-download
- An AMC returns duplicate filenames in the same response
- Retry logic is triggered after partial completion

---

## 7️⃣ Examples

### ✅ COMPLIANT: HDFC Downloader
```python
for file_item in files:
    file_obj = file_item["file"]
    url = file_obj["url"]
    name = file_obj["filename"]  # ✅ Original filename
    path = target_dir / name      # ✅ No renaming
    
    # Download
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    
    with open(path, "wb") as fp:
        fp.write(r.content)
    
    saved_files.append(str(path))
    logger.success(f"Saved: {path.name}")
```

### ❌ NON-COMPLIANT: PPFAS Downloader (Current)
```python
download = download_info.value
ext = os.path.splitext(download.suggested_filename)[1]
# ❌ VIOLATION: Renaming the file
filename = f"PPFAS_Monthly_Portfolio_Report_{target_month_name}_30_{target_year}{ext}"
save_path = download_folder / filename
download.save_as(str(save_path))
```

### ✅ COMPLIANT: PPFAS Downloader (Fixed)
```python
download = download_info.value
original_filename = download.suggested_filename  # ✅ Use exact original
save_path = download_folder / original_filename  # ✅ No renaming

# ✅ Duplicate check
if save_path.exists():
    logger.warning(f"File already exists, skipping: {original_filename}")
    return None  # or continue if in loop

download.save_as(str(save_path))
logger.success(f"Saved: {original_filename}")
```

---

## 8️⃣ Testing & Validation

### Manual Test
1. Run downloader for a specific month (e.g., `2025-12`)
2. Verify files are saved with **exact AMC-provided names**
3. Delete `_SUCCESS.json` (simulate incomplete download)
4. Run downloader again for the same month
5. Verify:
   - Existing files are **not re-downloaded**
   - No renamed variants created (e.g., `file_1.xlsx`)
   - Logs show "File already exists, skipping" messages

### Automated Test (Future)
```python
def test_no_renaming():
    # Mock AMC response with known filename
    # Run downloader
    # Assert saved filename == original filename

def test_no_duplicates():
    # Create a file in target folder
    # Run downloader
    # Assert file count unchanged
    # Assert no renamed variants
```

---

## 9️⃣ Summary

| Rule | Requirement | Enforcement |
|------|-------------|-------------|
| **Filename Immutability** | Preserve exact original filename | No renaming, prefixing, suffixing, or normalization |
| **No Duplicates** | Same filename not written twice per month | Check existence before download, skip if exists |
| **Architectural Boundaries** | Existing structure unchanged | Additive constraints only |
| **Compliance** | All downloaders must pass audit | Non-compliant downloaders rejected |

---

**This is tight, enforceable, and future-proof.**
