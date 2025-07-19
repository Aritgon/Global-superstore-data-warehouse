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

![ERD diagram](./pngs/star%20schema%20ERD.png)


### Improvements Achieved by Star Schema

- **Enhanced Business User Experience**: The intuitive and denormalized structure of a star schema makes it easier for non-technical users to understand the data model and build their own reports using self-service BI tools.

- **Streamlined BI Tool Integration**: Tools like Power BI can directly map to a star schema with minimal setup, enabling efficient data exploration and dashboard creation without requiring complex data transformations within the BI tool itself.

- **Reduced Data Redundancy**: While dimension tables are denormalized for performance, the overall design still reduces analytical data redundancy compared to flat files or highly repeated data in a single table, improving data consistency for reporting.

---

## Analysis with SQL 

1. How YoY growth of profit and sales looks like for each market? Which year has achieved most profit surpassing total sales that year? Which market has gained more profit?

> SQl Query:
```
with cte as (select
	b.market,
	extract(year from a.order_date) as selling_year,
	sum(a.sales) as total_sales,
	sum(a.profit) as total_profit
from fact_superstore as a
join dim_location as b on b.location_key = a.location_key
group by 1,2),

second_cte as (select
	market,
	selling_year,
	total_sales,
	total_profit,
	lag(total_sales) over (partition by market order by total_sales) as prev_year_sales,
	lag(total_profit) over (partition by market order by total_profit) as prev_year_profit
from cte),

final_cte as (select
	market,
	selling_year,
	round(((total_sales - prev_year_sales) * 100 / prev_year_sales)::numeric , 2) as YoY_sales_growth,
	round(((total_profit - prev_year_profit) * 100 / prev_year_profit)::numeric , 2) as YoY_profit_growth
from second_cte
where prev_year_sales is not null and prev_year_profit is not null)

select
	market,
	selling_year,
	YoY_sales_growth,
	YoY_profit_growth,
	case
		when YoY_profit_growth > YoY_sales_growth then 'High Profit Margin Year'
		else 'Positive Profit Margin Year'
	end as profit_bin
from final_cte;
```

**Insight** : <i>*Several regions like Canada (2012), EMEA (2013–2014), and Africa (2013–2014) experienced exceptionally high profit margins, marking them as standout years.
Most regions maintained positive margins across all years, reflecting stable performance.
2013 stands out globally with multiple regions hitting peak profitability, indicating a strong business year.*</i>
