
import os

files_to_delete = [
    "analyze_nippon.py",
    "check_nippon_names.py",
    "clean_check_cc.py",
    "collisions.txt",
    "debug_naming.py",
    "debug_naming_af.py",
    "diagnostic_nippon.py",
    "final_db_check.py",
    "find_collision.py",
    "find_collision_manual.py",
    "find_collisions_v2.py",
    "find_exact_collision.py",
    "find_lost.py",
    "map_nippon.py",
    "nippon_mapping.csv",
    "verify_all_names.py",
    "verify_nippon_names.py",
    "verify_retirement.py",
    "debug_nippon_ne.csv",
    "audit_nippon.py",
    "debug_nippon_cols.py"
]

for file in files_to_delete:
    path = os.path.join(os.getcwd(), file)
    if os.path.exists(path):
        try:
            os.remove(path)
            print(f"Deleted: {file}")
        except Exception as e:
            print(f"Error deleting {file}: {e}")
    else:
        print(f"Not found: {file}")
