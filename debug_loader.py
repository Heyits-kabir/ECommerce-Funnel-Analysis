import pandas as pd
import os
import traceback
from sqlalchemy import create_engine, text
from pathlib import Path

DB_CONFIG = {
    'user': 'root',
    'password': 'Unique4565',  
    'host': 'localhost',
    'port': '3306',
    'database': 'ecommerce_funnel'
}

# Paths
BASE_DIR = Path(__file__).parent
CLEANED_DIR = BASE_DIR / 'data/data/cleaned'
FILE_TO_TEST = 'cleaned_olist_sellers_dataset.csv'
TABLE_NAME = 'dim_sellers'

def test_connection():
    print("\n--- 1. Testing Database Connection ---")
    conn_str = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    try:
        engine = create_engine(conn_str)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            print("   -> Connection SUCCESS!")
        return engine
    except Exception:
        print("   -> Connection FAILED.")
        traceback.print_exc()
        return None

def debug_load(engine):
    print(f"\n--- 2. Attempting to Load {TABLE_NAME} ---")
    file_path = CLEANED_DIR / FILE_TO_TEST
    
    if not file_path.exists():
        print(f"   -> CRITICAL: File not found at {file_path}")
        return

    try:
        print("   -> Reading CSV...")
        df = pd.read_csv(file_path)
        print(f"   -> Read {len(df)} rows. Columns: {list(df.columns)}")
        
        #Verify columns match SQL expectation 
        print("   -> Pushing to MySQL...")
        
        
        df.to_sql(TABLE_NAME, con=engine, if_exists='append', index=False, chunksize=100)
        
        print("   -> SUCCESS! The table loaded.")
        
    except Exception:
        print("\n!!!!!!!!!!!!!! ERROR DETAILS !!!!!!!!!!!!!!")
        
        traceback.print_exc()
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n")

if __name__ == "__main__":
    engine = test_connection()
    if engine:
        debug_load(engine)
