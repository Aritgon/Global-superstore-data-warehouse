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

1. **How YoY growth of profit and sales looks like for each market? Which year has achieved most profit surpassing total sales that year? Which market has gained more profit?**

> SQL Query:
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


**Insight** : <i>*Several regions like **Canada (2012)**, **EMEA (2013–2014)**, and **Africa (2013–2014)** experienced exceptionally high profit margins, marking them as standout years.
Most regions maintained positive margins across all years, reflecting stable performance.
**2013** stands out globally with multiple regions hitting peak profitability, indicating a strong business year.*</i>


2. **Which product categories and sub-categories are the top performers in terms of both sales and profit margin? Conversely, which are the least performing?"**

> SQL query:
```
select
	b.category,
	b.sub_category,
	sum(a.sales) as total_sales,
	sum(a.profit) as total_profit,
	rank() over (partition by b.category order by sum(a.sales) desc) as sales_rank,
	rank() over (partition by b.category order by sum(a.profit) desc) as profit_rank
from fact_superstore as a
join dim_product as b on b.product_key = a.product_key
group by 1,2
order by sales_rank asc, profit_rank asc;
```


**Insight** : <i>**Phones**, **Storage**, and **Chairs** are the **top-performing** sub-categories by sales and profit, consistently ranking **1st** or **2nd** in both.
**Copiers**, **Bookcases** and **Appliances** rank **2nd** in sales but **1st** in profit, making them highly profitable relative to revenue.
**Tables** are a red flag with high sales but negative profit, indicating potential pricing or cost issues.</i>


3. **Which countries contributed the most to overall sales and profit?**

> SQL query:
```
with cte as (select
	extract(year from a.order_date) as order_year,
	b.country,
	sum(a.sales) as total_sales,
	sum(a.profit) as total_profit
from fact_superstore as a
join dim_location as b on b.location_key = a.location_key
group by 1,2),

rank_cte as (select
	order_year,
	country,
	round((total_sales * 100 / (select sum(sales) from fact_superstore))::numeric ,2) as sales_contribution_pct,
	round((total_profit * 100 / (select sum(profit) from fact_superstore))::numeric ,2) as profit_contribution_pct
from cte),


final_cte as (select
	order_year,
	country,
	sales_contribution_pct,
	profit_contribution_pct,
	dense_rank() over (partition by order_year order by sales_contribution_pct desc) as sales_contribution_pct_rnk,
	dense_rank() over (partition by order_year order by profit_contribution_pct desc) as profit_contribution_pct_rnk
from rank_cte)

select
	order_year,
	country,
	sales_contribution_pct,
	profit_contribution_pct,
	sales_contribution_pct_rnk,
	profit_contribution_pct_rnk
from final_cte
where sales_contribution_pct_rnk <= 5 and profit_contribution_pct_rnk <= 5;
```


**Insight** : <i>**United States** consistently leads *both* in *sales* and profit across all years, securing Rank **1** throughout.
**China** shows strong profit growth, especially in *2013–2014*, despite *lower sales* ranks.
**Australia** and **France** occasionally rank high in sales but don’t maintain top profitability, suggesting narrower margins.</i>


4. **What is the average discount applied across different product categories, and how does this discount correlate with the resulting profit margin?"**

> SQL query:
```
with cte as (select
	extract(year from a.order_date) as order_year,
	b.category,
	round(avg(profit)::int, 2) as avg_profit,
	lag(round(avg(profit), 2)) over (partition by b.category order by extract(year from a.order_date)) as prev_year_avg_profit,
	round(avg(a.discount)::decimal, 3) as avg_discount_applied,
	lag(round(avg(a.discount)::decimal, 3)) over (partition by b.category order by extract(year from a.order_date)) as prev_year_avg_discount
from fact_superstore as a
join dim_product as b on b.product_key = a.product_key
group by 1, 2)

select
	order_year,
	category,
	avg_profit,
	avg_discount_applied,
	case
		when avg_profit > prev_year_avg_profit and avg_discount_applied > prev_year_avg_discount
		then 'both increased'
		when avg_profit > prev_year_avg_profit and avg_discount_applied < prev_year_avg_discount
		then 'profit incresed while discount dropped'
		when avg_profit < prev_year_avg_profit and avg_discount_applied > prev_year_avg_discount
		then 'profit dropped but discount increased'
		when avg_profit < prev_year_avg_profit and avg_discount_applied < prev_year_avg_discount
		then 'both dropped'
	else 'unknown' -- handing edge cases of where previous year avg profit and discount is null because of lag().
	end as yearly_profit_discount_trend
from cte
order by 2, 1;
```


**Insight** : <i>**Technology** consistently outperforms in profit across all years, showing a stable inverse relationship between *profit* and *discount*.
**Furniture** exhibits volatile trends with both profit and discount rising and falling together, hinting at sensitivity to pricing strategies.
**Office** Supplies show mixed behavior, with *2013 standing out for higher profit despite lower discount*, suggesting better efficiency or product mix that year.</i>


5. **Customer Segmentation (RFM Analysis)**

> SQL query:
```
with cte1 as (select
	b.customer_id,
	(select max(order_date) from fact_superstore) - max(a.order_date) as latest_order_date_diff,
	count(a.fact_key) as number_of_orders,
	sum(a.sales) as total_sales
from fact_superstore as a
join dim_customer as b on b.customer_key = a.customer_key
group by 1),

rfm_cte as (
	select
		customer_id,
		latest_order_date_diff,
		number_of_orders,
		total_sales,
		ntile(5) over (order by latest_order_date_diff desc) as recency_score,
		ntile(5) over (order by number_of_orders desc) as frequency_score,
		ntile(5) over(order by total_sales desc) as monetary_score
	from cte1
)

select
	CASE
            WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Champions'
            WHEN recency_score >= 3 AND frequency_score >= 4 AND monetary_score >= 3 THEN 'Loyal Customers'
            WHEN recency_score >= 4 AND frequency_score BETWEEN 2 AND 3 AND monetary_score >= 3 THEN 'Potential Loyalists'
            WHEN recency_score >= 4 AND frequency_score <= 2 THEN 'New Customers'
            WHEN recency_score >= 4 AND frequency_score <= 3 AND monetary_score <= 3 THEN 'Promising'
            WHEN recency_score <= 2 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Cannot Lose Them'
            WHEN recency_score <= 2 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'At Risk'
            WHEN monetary_score >= 4 AND frequency_score <= 2 THEN 'Big Spenders'
            WHEN recency_score = 3 AND frequency_score = 3 AND monetary_score = 3 THEN 'Need Attention'
            WHEN recency_score <= 3 AND frequency_score <= 2 AND monetary_score <= 3 THEN 'About to Sleep'
            WHEN recency_score <= 2 AND frequency_score BETWEEN 1 AND 2 AND monetary_score <= 3 THEN 'Hibernating'
            WHEN recency_score <= 2 AND frequency_score <= 2 AND monetary_score <= 2 THEN 'Lost'
            ELSE 'Others'
        END AS rfm_segment,
		count(distinct customer_id) as customer_count,

		-- numerics.
		round(avg(latest_order_date_diff)::numeric, 2) as avg_delivery_timing,
		round(avg(number_of_orders)::numeric, 2) as avg_order_count,
		round(avg(total_sales)::numeric, 2) as avg_sales_amount
from rfm_cte
group by 1;
```


**Insight** : **"New Customers"** and **"Promising"** segments show **high average order value**, making them prime for conversion into loyal buyers.
**"Cannot Lose Them"** has the *highest total profit*, but *very low frequency*, signaling a need for re-engagement strategies.
**"Champions"** and **"Loyal Customers"** offer steady, reliable profits — perfect for upselling and retention campaigns.

---

### Further Analysis

Only key insights are included in this document.
You can explore the complete set of SQL ad-hoc analyses by checking the `SQL_analysis.sql` file from the `/SQL` folder in this repository.
Feel free to clone the repo or download the file directly to run and explore the queries yourself.

### Tools Used

- `pandas` - To explore the dataset and upload it to `postgresql` database.
- `PostgreSQL` - used it to design the data warehouse and do EDA using SQL.
- `VS Code` - for writing the documentation.
- `Git & GitHub` - for uploading the project.

### A note
This project is a work in progress and may continue to evolve as I explore deeper patterns or add more advanced queries.

Stay tuned for updates — contributions and feedback are always welcome!


# Signing off 🙋‍♂️