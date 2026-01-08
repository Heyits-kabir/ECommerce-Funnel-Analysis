import pandas as pd
import numpy as np
import os
import traceback
from sqlalchemy import create_engine, text
from pathlib import Path

# ==========================================
# CONFIGURATION
# ==========================================
DB_CONFIG = {
    'user': 'root',
    'password': 'Unique4565',  
    'host': 'localhost',
    'port': '3306',
    'database': 'ecommerce_funnel'
}

# PATHS
BASE_DIR = Path(__file__).parent
CLEANED_DIR = BASE_DIR / 'data/data/cleaned' 
SCHEMA_PATH = BASE_DIR / 'schema.sql'

# MAPPING: CSV Filename -> SQL Table Name
files_to_load = [
    ('cleaned_olist_products_dataset.csv', 'dim_products'),
    ('cleaned_olist_sellers_dataset.csv', 'dim_sellers'),
    ('cleaned_olist_customers_dataset.csv', 'dim_customers'),
    ('cleaned_olist_geolocation_dataset.csv', 'dim_geo'),
    ('cleaned_flipkart_com-ecommerce_sample.csv', 'competitor_flipkart'),
    ('cleaned_olist_orders_dataset.csv', 'fact_orders'),
    ('cleaned_olist_order_items_dataset.csv', 'fact_order_items'),
    ('cleaned_olist_order_payments_dataset.csv', 'fact_payments'),
    ('cleaned_olist_order_reviews_dataset.csv', 'fact_reviews')
]

# EXACT DATE COLUMNS
DATE_COLS_MAP = {
    'fact_orders': [
        'order_purchase_timestamp', 'order_approved_at', 
        'order_delivered_carrier_date', 'order_delivered_customer_date', 
        'order_estimated_delivery_date'
    ],
    'fact_order_items': ['shipping_limit_date'],
    'fact_reviews': ['review_creation_date', 'review_answer_timestamp'],
    'competitor_flipkart': ['crawl_timestamp']
}

def get_engine():
    conn_str = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}"
    return create_engine(conn_str)

def init_database(engine):
    print("\n--- Initializing Database Schema ---")
    if not SCHEMA_PATH.exists():
        print(f"ERROR: schema.sql not found at {SCHEMA_PATH}")
        return False
    try:
        root_engine = create_engine(f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}")
        with root_engine.connect() as conn:
            conn.execute(text("DROP DATABASE IF EXISTS ecommerce_funnel;"))
            conn.execute(text("CREATE DATABASE ecommerce_funnel;"))
            conn.execute(text("USE ecommerce_funnel;"))
        
        with engine.connect() as conn:
            conn.execute(text("USE ecommerce_funnel;"))
            with open(SCHEMA_PATH, 'r') as file:
                sql_script = file.read()
            commands = sql_script.split(';')
            for command in commands:
                if command.strip():
                    conn.execute(text(command))
            conn.commit()
            print("Database and Tables created successfully.")
            return True
    except Exception:
        traceback.print_exc()
        return False

def load_data(engine):
    print("\n--- Starting Data Load ---")
    
    db_engine = create_engine(f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")

    with db_engine.connect() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
        conn.commit()

    for filename, table_name in files_to_load:
        file_path = CLEANED_DIR / filename
        
        if not file_path.exists():
            print(f"Skipping {table_name}: File not found ({filename})")
            continue

        print(f"Loading {table_name}...")
        
        try:
            df = pd.read_csv(file_path)
            
            # 1. Handle Nulls
            df = df.replace({np.nan: None})
            
            # 2. Date Conversion
            if table_name in DATE_COLS_MAP:
                for col in DATE_COLS_MAP[table_name]:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], errors='coerce')

            # 3. SPECIAL HANDLING: Filter Flipkart Columns
            # This ensures we only upload columns that actually exist in the SQL Schema
            if table_name == 'competitor_flipkart':
                allowed_cols = [
                    'uniq_id', 'crawl_timestamp', 'product_name', 'main_category', 
                    'pid', 'retail_price', 'discounted_price', 'discount_pct', 
                    'brand', 'product_rating', 'overall_rating'
                ]
                # Keep only columns that are in both the CSV and the allowed list
                final_cols = [c for c in allowed_cols if c in df.columns]
                df = df[final_cols]

            # 4. Load to SQL
            df.to_sql(table_name, con=db_engine, if_exists='append', index=False, chunksize=1000)
            print(f"   -> Success: Loaded {len(df)} rows.")
            
        except Exception:
            print(f"   -> ERROR loading {table_name}")
            traceback.print_exc()

    with db_engine.connect() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
        conn.commit()
    
    print("\n--- Data Load Complete ---")

if __name__ == "__main__":
    engine = get_engine()
    if init_database(engine):
        load_data(engine)