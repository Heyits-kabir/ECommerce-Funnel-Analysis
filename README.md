# 🛒 E-commerce Sales & Funnel Analytics (Olist + Flipkart Data)

This project demonstrates a full end-to-end Data Analytics workflow, transforming messy, raw e-commerce data (Olist) and competitor data (Flipkart) into actionable business intelligence using **Python (Pandas), SQL (MySQL), and Power BI**.

## 🚀 Architecture Overview (The Data Pipeline)
The workflow follows an ELT (Extract, Load, Transform) approach:

1. **Extraction/Cleaning (Python)**: Raw data is loaded, cleaned, denormalized (e.g., date formatting, category translation), and derived metrics (delivery time, discount %) are created.
2. **Database Modeling (SQL)**: A Star Schema  is implemented in MySQL to support scalable analytics, defining Facts (`fact_orders`, `fact_order_items`) and Dimensions (`dim_customers`, `dim_products`).
3. **Loading (Python)**: A SQLAlchemy script handles connection, schema creation, and ordered data insertion, ensuring Foreign Key integrity.
4. **Analysis & Visualization (SQL & Power BI)**: Complex SQL queries extract core metrics, which are then visualized in Power BI for business consumption.

## 💾 Database Schema (ERD)

The core structure is anchored around the `fact_orders` table, linking key dimensions:

```sql
-- Main Tables Defined in schema.sql
-- dim_customers: PK (customer_id)
-- dim_products: PK (product_id)
-- fact_orders: FK (customer_id)
-- fact_order_items: FK (order_id, product_id, seller_id)