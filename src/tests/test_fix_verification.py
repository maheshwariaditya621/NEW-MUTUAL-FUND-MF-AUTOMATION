import threading
import time
import sys
import os

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.db.connection import get_pool_connection, release_pool_connection, get_pool
from src.services.pricing_service import PricingService

def test_db_pool():
    print("\n[DB POOL TEST]")
    connections = []
    try:
        print("Fetching 5 connections from pool...")
        for i in range(5):
            conn = get_pool_connection()
            connections.append(conn)
            print(f"  Connection {i+1} acquired")
        
        pool = get_pool()
        print(f"Pool status: {pool.minconn} min, {pool.maxconn} max")
        print(f"Current used: {len(pool._used)}")
        
    finally:
        print("Releasing connections...")
        for conn in connections:
            release_pool_connection(conn)
        print("All connections released.")

def test_pricing_lock():
    print("\n[PRICING SERVICE LOCK TEST]")
    ps = PricingService()
    
    # Mocking _is_configured to always return True for testing
    ps._is_configured = lambda: True
    
    def simulate_login(thread_id):
        print(f"Thread {thread_id} attempting login...")
        # This will hit the lock
        success = ps._login()
        print(f"Thread {thread_id} login result: {success}")

    threads = []
    for i in range(3):
        t = threading.Thread(target=simulate_login, args=(i,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

if __name__ == "__main__":
    try:
        test_db_pool()
        # Note: Pricing lock test might fail to actually log in without real credentials, 
        # but we can see the locking logic and logs.
        test_pricing_lock()
        print("\nVerification script finished.")
    except Exception as e:
        print(f"\nVerification failed: {e}")
