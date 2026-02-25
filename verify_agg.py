import os
import psycopg2
from dotenv import load_dotenv
from src.api.routers.chatbot import _get_amc_holdings_of_stock, _get_industry_total_stake

load_dotenv()

def verify_aggregation():
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )
    cur = conn.cursor()
    
    print("--- Testing AMC Aggregation (HDFC on Reliance) ---")
    res1 = _get_amc_holdings_of_stock("HDFC", "RELIANCE", cur)
    print("RESULT:", res1)
    
    print("\n--- Testing Industry Total Stake (Reliance) ---")
    res2 = _get_industry_total_stake("RELIANCE", cur)
    print("RESULT:", res2)
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    verify_aggregation()
