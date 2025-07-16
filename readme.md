# Global Superstore Data Warehouse
---

## Dataset Overview

The dataset is sourced from **Kaggle** and represents a **Global Superstore's sales data**.
It contains **27** columns covering key domains like **customer information**, **order** and **product details**, **shipping logistics**, and **market locations**.
The data was heavily denormalized, with redundant and repeated fields—making it ideal for restructuring into a well-designed data warehouse schema.


## Data Cleaning Summary

- Verified that the dataset had `zero` **null** values.

- Dropped columns that lacked business value or meaningful data.

- Created a `delivery_time` column by subtracting `order_date` from `ship_date`.

- Checked for invalid entries like **quantity < 0** and **delivery_time < 0**.

- Counted transactions with *negative* profit to highlight sales.

- Applied IQR method for outlier detection but chose to **retain outliers** as they can occur in real-world business cases.

- Used `SQLAlchemy` with `to_sql()` to load the cleaned data into a *PostgreSQL* database for further modeling.

---
## Data warehouse design

- Modeled a star schema with *one* central fact table and *four* supporting dimension tables.

- *fact_superstore* : contains transaction-level data including foreign keys to all dimensions to establish relations with other dimension tables. created a relationship using `date_key` from dim_date table with both `order_date_key` and `ship_date_key` for analysis which requires both date (e.g. avg difference between order date and shipping date by region).

- **dim_location** : captures business-specific geographic information (market, country, state, city) using a composite unique key to maintain business-level granularity. These unique constraints helped to control and validate incoming data from the source system, while data insertion. Used `ON CONFLICT (...) DO UPDATE SET ..` clause alongside `EXCLUDED` keyword to safely update existing records when a conflict occurs, allowing new or corrected data to be inserted without violating constraints.

- **dim_product** : used `product_id` as a business key sourced directly from the dataset, representing real product identities across the business. To maintain integrity and avoid duplication, the `ON CONFLICT (product_id) DO UPDATE SET` ... clause is used to update associated fields (like product name, sub-category, and category) whenever a conflict arises. This ensures that if updated product details appear in new records, the existing dimension entry remains up to date without creating duplicates.

- **dim_customer** : Records customer specific details (customer name) from all transactions. Used `customer_id` as a business key to represent real customer details across the business. This table also follows a similar design pattern of *dim_product* to maintain data integrity and avoid duplications of datas. 

- **dim_date** : created a custom date_table for better powerBI integration for future dashboard analysis. Used `generate_series` in postgreSQL to generate a date table ranging from earliest transaction date to latest transaction date. 

