import pandas as pd
import numpy as np
import os
import ast  # For parsing the Flipkart category string
from pathlib import Path

# ==========================================
# CONFIGURATION
# ==========================================
BASE_DIR = Path(__file__).parent
RAW_DIR = BASE_DIR / 'raw'
CLEANED_DIR = BASE_DIR / 'data/cleaned'
CLEANED_DIR.mkdir(parents=True, exist_ok=True)

class EcommerceCleaner:
    def __init__(self):
        print("Initializing Cleaner...")

    def load_csv(self, filename):
        """Robust CSV loader"""
        path = RAW_DIR / filename
        if not path.exists():
            print(f"!! WARNING: {filename} not found in {RAW_DIR}")
            return None
        print(f"\nProcessing: {filename}")
        try:
            return pd.read_csv(path)
        except Exception as e:
            print(f"Error reading {filename}: {e}")
            return None

    def save_csv(self, df, filename):
        """Saves cleaned data"""
        if df is not None:
            save_path = CLEANED_DIR / f"cleaned_{filename}"
            df.to_csv(save_path, index=False)
            print(f"Saved: {save_path} | Shape: {df.shape}")

    def clean_text_columns(self, df):
        """Generic text cleaner: strip whitespace, lowercase"""
        for col in df.select_dtypes(include=['object']):
            # specific skip for IDs if you want to keep them case-sensitive
            # but usually lowercasing everything is safer for joining
            df[col] = df[col].astype(str).str.strip().str.lower()
            # Restore proper NaNs for empty strings
            df[col] = df[col].replace({'nan': np.nan, '': np.nan})
        return df

    # ==========================================
    # SPECIFIC CLEANING FUNCTIONS
    # ==========================================

    def process_olist_orders(self):
        filename = 'olist_orders_dataset.csv'
        df = self.load_csv(filename)
        if df is None: return

        # 1. Date Conversions
        date_cols = ['order_purchase_timestamp', 'order_approved_at', 
                     'order_delivered_carrier_date', 'order_delivered_customer_date', 
                     'order_estimated_delivery_date']
        
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], errors='coerce')

        # 2. Derived Column: Delivery Time (Days)
        # Handle NaT (Not a Time) errors safely
        df['delivery_days'] = (df['order_delivered_customer_date'] - df['order_purchase_timestamp']).dt.days

        # 3. Derived Column: Delay Status
        df['is_late'] = np.where(
            (df['order_delivered_customer_date'] > df['order_estimated_delivery_date']), 
            1, 0
        )

        # 4. Handle Nulls in critical dates (Don't drop, just flag)
        # We keep rows because a null delivery date = "In Progress" or "Cancelled"
        
        self.save_csv(df, filename)

    def process_olist_items(self):
        filename = 'olist_order_items_dataset.csv'
        df = self.load_csv(filename)
        if df is None: return

        # Basic text cleaning
        df = self.clean_text_columns(df)
        
        # Ensure prices are positive
        df = df[df['price'] >= 0]
        
        self.save_csv(df, filename)

    def process_olist_products(self):
        # We load both products and the translation file to merge them NOW
        prod_file = 'olist_products_dataset.csv'
        trans_file = 'product_category_name_translation.csv'
        
        df_prod = self.load_csv(prod_file)
        df_trans = self.load_csv(trans_file)
        
        if df_prod is None: return

        # Merge Translations
        if df_trans is not None:
            # Ensure keys match for merge
            df_prod['product_category_name'] = df_prod['product_category_name'].astype(str).str.strip().str.lower()
            df_trans['product_category_name'] = df_trans['product_category_name'].astype(str).str.strip().str.lower()
            
            df_merged = pd.merge(df_prod, df_trans, on='product_category_name', how='left')
            
            # Fill missing English names with the Portuguese one (fallback)
            df_merged['product_category_name_english'] = df_merged['product_category_name_english'].fillna(df_merged['product_category_name'])
            
            # Drop the original portuguese column to clean up schema
            df_merged = df_merged.drop(columns=['product_category_name'])
            df_merged = df_merged.rename(columns={'product_category_name_english': 'category_name'})
        else:
            df_merged = df_prod.rename(columns={'product_category_name': 'category_name'})

        self.save_csv(df_merged, prod_file)

    def process_olist_reviews(self):
        filename = 'olist_order_reviews_dataset.csv'
        df = self.load_csv(filename)
        if df is None: return

        # Clean text
        df['review_comment_message'] = df['review_comment_message'].astype(str).str.strip()
        # Replace actual string "nan" with empty string for SQL compatibility
        df['review_comment_message'] = df['review_comment_message'].replace('nan', '')
        
        self.save_csv(df, filename)

    def process_flipkart(self):
        filename = 'flipkart_com-ecommerce_sample.csv'
        df = self.load_csv(filename)
        if df is None: return

        # 1. Parse timestamps
        df['crawl_timestamp'] = pd.to_datetime(df['crawl_timestamp'], errors='coerce')

        # 2. Clean Category Tree
        # Format is ["Clothing >> Women's Clothing"] -> We want "Clothing"
        def extract_main_category(tree_str):
            try:
                # It looks like a list string, e.g. '["Cats >> Subcats"]'
                if pd.isna(tree_str): return "Uncategorized"
                # Remove brackets and quotes if simple string manipulation works faster/safer
                clean_str = str(tree_str).replace('["', '').replace('"]', '').replace("['", "").replace("']", "")
                main_cat = clean_str.split(' >> ')[0]
                return main_cat.lower()
            except:
                return "error"

        df['main_category'] = df['product_category_tree'].apply(extract_main_category)

        # 3. Clean Prices (Ensure they are numeric)
        df['retail_price'] = pd.to_numeric(df['retail_price'], errors='coerce').fillna(0)
        df['discounted_price'] = pd.to_numeric(df['discounted_price'], errors='coerce').fillna(0)
        
        # 4. Calculate Discount Percentage
        # Avoid division by zero
        df['discount_pct'] = np.where(
            df['retail_price'] > 0,
            ((df['retail_price'] - df['discounted_price']) / df['retail_price']) * 100,
            0
        )
        df['discount_pct'] = df['discount_pct'].round(2)

        self.save_csv(df, filename)

    def process_others(self):
        """Standard clean for the remaining files"""
        files = [
            'olist_customers_dataset.csv',
            'olist_geolocation_dataset.csv',
            'olist_sellers_dataset.csv',
            'olist_order_payments_dataset.csv'
        ]
        for f in files:
            df = self.load_csv(f)
            if df is not None:
                df = self.clean_text_columns(df)
                self.save_csv(df, f)

    def run_all(self):
        print("--- STARTING CLEANING PROCESS ---")
        self.process_olist_orders()
        self.process_olist_items()
        self.process_olist_products()
        self.process_olist_reviews()
        self.process_flipkart()
        self.process_others()
        print("\n--- CLEANING COMPLETE. CHECK /data/cleaned FOLDER ---")

if __name__ == "__main__":
    cleaner = EcommerceCleaner()
    cleaner.run_all()