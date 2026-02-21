from src.db.connection import get_connection
import re

def fix_mojibake(text):
    if not text: return ""
    replacements = {"â€“": "-", "â€”": "-", "â€¦": "...", "â€˜": "'"}
    fixed_text = text
    for bad, good in replacements.items():
        fixed_text = fixed_text.replace(bad, good)
    return fixed_text

def parse_verbose_scheme_name_MOCK(raw_name: str):
    decoded_name = fix_mojibake(raw_name)
    name_clean = decoded_name.replace("_", " ").strip()
    parts = [p.strip() for p in name_clean.split(" - ")]
    
    share_class_keywords = ["DIRECT", "REGULAR", "GROWTH", "IDCW", "DIVIDEND", "PAYOUT", "REINVEST"]
    legal_desc_keywords = ["AN OPEN ENDED", "A CLOSE ENDED", "A DYNAMIC", "A CONCENTRATED", "INVESTING IN", "REPLICATING/TRACKING"]
    
    remaining_name_parts = []
    metadata_parts = []
    
    for i, part in enumerate(parts):
        p_upper = part.upper()
        if i == 0:
            remaining_name_parts.append(part)
            continue
            
        is_class_metadata = any(kw in p_upper for kw in share_class_keywords)
        is_legal_desc = any(kw in p_upper for kw in legal_desc_keywords)
        is_protected_plan = any(kw in p_upper for kw in ["EQUITY PLAN", "DEBT PLAN", "HYBRID PLAN", "GOLD PLAN"])
        
        if (is_class_metadata or is_legal_desc) and not is_protected_plan:
            metadata_parts.append(part)
        else:
            remaining_name_parts.append(part)

    main_name = " - ".join(remaining_name_parts)
    return main_name

def run_safety_check():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT scheme_name FROM schemes")
    all_names = [row[0] for row in cur.fetchall()]
    
    # Add synthetic test cases to verify logic
    test_cases = [
        "360 ONE BALANCED HYBRID FUND - AN OPEN ENDED BALANCED SCHEME INVESTING IN EQUITY AND DEBT INSTRUMENTS",
        "HDFC DYNAMIC DEBT FUND", # Should NOT change because no " - "
        "ICICI PRUDENTIAL BLUECHIP FUND - Regular Plan - Growth", # Should NOT change identity
        "SAMCO ACTIVE MOMENTUM FUND - AN OPEN ENDED EQUITY SCHEME FOLLOWING MOMENTUM THEME"
    ]
    
    all_names.extend(test_cases)
    
    changed = []
    for name in all_names:
        parsed_name = parse_verbose_scheme_name_MOCK(name)
        if parsed_name != name:
            changed.append((name, parsed_name))
            
    print(f"Total schemes checked (including tests): {len(all_names)}")
    print(f"Total schemes impacted: {len(changed)}")
    print("\nImpact Analysis:")
    for original, parsed in changed:
        print(f"Original: {original}")
        print(f"Parsed:   {parsed}")
        print("-" * 20)

if __name__ == "__main__":
    run_safety_check()
