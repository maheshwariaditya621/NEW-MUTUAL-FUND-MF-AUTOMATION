try:
    from nselib import capital_market
    import pandas as pd
    
    def test_nselib():
        print("Testing nselib...")
        try:
            # Check if we can get index data
            # Try distinct names for TRI
            try:
                print("Attempting to fetch 'Nifty 50 TRI'...")
                data = capital_market.index_data(index='Nifty 50 TRI', from_date='01-01-2024', to_date='05-01-2024')
                print("Fetched TRI data?")
                print(data.head())
            except Exception as e:
                print(f"Failed to fetch TRI directly: {e}")
                
            # Try standard
            print("\nFetching standard 'Nifty 50'...")
            data = capital_market.index_data(index='Nifty 50', from_date='01-01-2024', to_date='05-01-2024')
            print("Successfully fetched data via nselib:")
            print(data.head())
            print(f"Columns: {data.columns}")
        except Exception as e:
            print(f"nselib error: {e}")

    if __name__ == "__main__":
        test_nselib()
except ImportError:
    print("nselib not installed. Please install it.")
