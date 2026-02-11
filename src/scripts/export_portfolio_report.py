import csv
import sys
from datetime import datetime
from src.db.connection import get_cursor, close_connection
from pathlib import Path

def export_portfolio_report(amc_name_filter=None, year=None, month=None):
    """
    Exports the portfolio_details_view to a CSV file.
    """
    cursor = get_cursor()
    
    query = "SELECT * FROM portfolio_details_view"
    conditions = []
    params = []
    
    if amc_name_filter:
        conditions.append("amc_name ILIKE %s")
        params.append(f"%{amc_name_filter}%")
    
    if year:
        conditions.append("year = %s")
        params.append(year)
        
    if month:
        conditions.append("month = %s")
        params.append(month)
        
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
        
    query += " ORDER BY year DESC, month DESC, amc_name, scheme_name, market_value_inr DESC"
    
    print(f"Executing Query: {query} with params {params}")
    cursor.execute(query, tuple(params))
    
    rows = cursor.fetchall()
    headers = [desc[0] for desc in cursor.description]
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"reports/portfolio_dump_{timestamp}.csv"
    Path("reports").mkdir(exist_ok=True)
    
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
        
    print(f"Successfully exported {len(rows)} rows to {filename}")
    close_connection()
    return filename

if __name__ == "__main__":
    # Default behavior: HDFC Dec 2025
    export_portfolio_report(amc_name_filter="HDFC", year=2025, month=12)
