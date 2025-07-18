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

## Key Features


**Star Schema Design** : Implemented a classic star schema using PostgreSQL. This design choice was made to:

- *Facilitate Business Intelligence (BI) Integration*: Star schemas are highly optimized for direct connectivity with BI tools like Power BI, enabling intuitive drag-and-drop report building and self-service analytics.

- *Enhance Query Performance*: By denormalizing data into a central fact table and smaller, related dimension tables, complex analytical queries can join fewer tables, significantly reducing query execution time compared to a highly normalized operational database.

### The design involves:

A central `FactSales` table capturing key sales measures (Sales, Quantity, Discount, Profit and all the foreign keys to establish relations with dim tables).

Associated `Dimension` Tables (Dim_Product, Dim_Customer, Dim_location, Dim_Date) providing descriptive context for analysis.


### Data model design patterns

![ERD diagram](./pngs/star%20schema%20diagram.png)


### Improvements Achieved by Star Schema

- **Enhanced Business User Experience**: The intuitive and denormalized structure of a star schema makes it easier for non-technical users to understand the data model and build their own reports using self-service BI tools.

- **Streamlined BI Tool Integration**: Tools like Power BI can directly map to a star schema with minimal setup, enabling efficient data exploration and dashboard creation without requiring complex data transformations within the BI tool itself.

- **Reduced Data Redundancy**: While dimension tables are denormalized for performance, the overall design still reduces analytical data redundancy compared to flat files or highly repeated data in a single table, improving data consistency for reporting.

---

