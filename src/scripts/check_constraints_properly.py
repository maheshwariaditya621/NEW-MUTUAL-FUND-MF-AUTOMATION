from src.db.connection import get_connection

def check_constraints():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT conname, pg_get_constraintdef(oid) 
        FROM pg_constraint 
        WHERE conrelid = 'public.equity_holdings'::regclass;
    """)
    constraints = cur.fetchall()
    print("Constraints for 'equity_holdings':")
    for con in constraints:
        print(f" - {con[0]}: {con[1]}")
    
    cur.execute("""
        SELECT conname, pg_get_constraintdef(oid) 
        FROM pg_constraint 
        WHERE conrelid = 'public.scheme_snapshots'::regclass;
    """)
    constraints = cur.fetchall()
    print("\nConstraints for 'scheme_snapshots':")
    for con in constraints:
        print(f" - {con[0]}: {con[1]}")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    check_constraints()
