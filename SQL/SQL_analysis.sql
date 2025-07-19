-- "How have total sales and profit trended year-over-year across all market?"

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

-- "Which product categories and sub-categories are the top performers in terms of both sales 
-- and profit margin? Conversely, which are the least performing?"

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

-- "Which countries and regions contribute the most to overall sales and profit, 
-- and are there any emerging or declining markets?"

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

-- "What is the average discount applied across different product categories,
-- and how does this discount correlate with the resulting profit margin?"

-- binning profit and discount trend.

-- profit increment (current year than last year) & discount increment (current year than last year), 
--'both discount and profit increased',
-- profit increment (current year than last year) & discount decrement (current year than last year),
-- 'profit incresed while discount dropped'
-- profit decrement (current year than last year) & discount increment (current year than last year), 
-- 'profit dropped but discount increased'
-- profit decrement (current year than last year) & discount decrement (current year than last year),
-- 'both dropped'
-- 'uncategorized' for edge cases.


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

-- "What is the average shipping time (from order date to ship date) for each shipping mode 
-- (e.g., Standard Class, First Class), and how does this vary by customer segment or region?"

select
	ship_mode,
	round(avg(ship_date - order_date), 2) as avg_delivery_time
from fact_superstore
group by 1;

-- "Within our top 3 most profitable countries, which specific product sub-categories are driving the most profit?"

with c_cte as (select
	b.country
from fact_superstore as a
join dim_location as b on b.location_key = a.location_key
group by 1
order by sum(a.profit) desc limit 3)

select
	c.country,
	b.sub_category,
	sum(a.profit) as total_profit,
	rank() over (partition by c.country order by sum(a.profit) desc) as profit_product_rnk
from fact_superstore as a
join dim_product as b on b.product_key = a.product_key
join dim_location as c on c.location_key = a.location_key
where c.country in (select country from c_cte)
group by 1,2
order by 1, 3 desc;


-- Customer Segmentation (RFM Analysis).
-- Ntile() over (order by ...) -> this will return max of higher frequency as 5 and the least will be 1. 
-- most latest_order_date, frequency and monetary will be as 5.


with cte1 as (select
	b.customer_id,
	b.customer_name,
	(select max(order_date) from fact_superstore) - max(a.order_date) as latest_order_date,
	count(a.fact_key) as number_of_orders,
	sum(a.sales) as total_sales
from fact_superstore as a
join dim_customer as b on b.customer_key = a.customer_key
group by 1,2),

rfm_cte as (
	select
		customer_id,
		customer_name,
		ntile(5) over (order by latest_order_date desc) as recency,
		ntile(5) over (order by number_of_orders desc) as frequency,
		ntile(5) over(order by total_sales desc) as monetary
	from cte1
)

select
	case
	    when recency = 5 and frequency = 5 and monetary = 5 then 'Champions'
	    when recency >= 4 and frequency >= 4 and monetary between 3 and 4 then 'Loyal Customers'
	    when recency >= 4 and frequency between 3 and 4 and monetary >= 4 then 'Potential Loyalist'
	    when recency between 3 and 4 and frequency between 3 and 4 and monetary >= 4 then 'High Value Customers'
	    when recency between 3 and 5 and frequency between 1 and 3 and monetary >= 4 then 'Big Spenders'
	    when recency between 3 and 5 and frequency between 1 and 2 and monetary <= 2 then 'Promising'
	    when recency between 2 and 3 and frequency <= 2 and monetary <= 2 then 'About to Sleep'
	    when recency = 1 and frequency >= 3 and monetary >= 4 then 'Cannot Lose Them'
	    when recency = 1 and frequency between 1 and 2 and monetary >= 3 then 'Hibernating'
	    when recency = 1 and frequency = 1 and monetary <= 2 then 'Lost'
	    when frequency = 1 and monetary <= 2 and recency between 2 and 4 then 'New Customers'
	    else 'Others'
	end as rfm_category,
	count(distinct customer_id) as customer_count
from rfm_cte
group by 1 order by 1 desc;

-- seasonal analysis of profit and sales.

select
	c.region,
	b.category,
	CASE
	    WHEN d.month IN (12, 1, 2) THEN 'Winter'
	    WHEN d.month IN (3, 4, 5) THEN 'Spring'
	    WHEN d.month IN (6, 7, 8) THEN 'Summer'
	    WHEN d.month IN (9, 10, 11) THEN 'Fall'
	END AS season,
	count(a.fact_key) as total_order_count,
	sum(a.sales) as total_sales,
	sum(a.profit) as total_profit
from fact_superstore as a
join dim_product as b on b.product_key = a.product_key
join dim_location as c on c.location_key = a.location_key
join dim_date as d on d.date_key = a.order_date_key
group by 1,2,3
order by 1,3;


-- "Identify all customers who have not made a purchase in the last 12 (or 24) months. 
-- What was their last recorded total sales and profit contribution, and how many such 'inactive' customers do we have?"

with last_12_cte as (select
	b.customer_key,
	b.customer_id,
	b.customer_name,
	max(a.order_date) as latest_order_date_12
from fact_superstore as a
join dim_customer as b on b.customer_key = a.customer_key
where a.order_date >= (select max(order_date) - interval '12 month' from fact_superstore)
group by 1,2,3),

-- create another cte to get all users.
all_cte as (select
	b.customer_key,
	b.customer_id,
	b.customer_name,
	max(a.order_date) as latest_order_date_all
from fact_superstore as a
join dim_customer as b on b.customer_key = a.customer_key
group by 1,2,3)

-- join using left join to get customers who aren't active in the last 12 months.
select
	'Not active for last 12 months' as churn_customer_count, count(*) 
from all_cte as a
left join last_12_cte as b on b.customer_key = a.customer_key
where b.customer_key is null
union all
select
	'active before the last 12 months', count(*)
from all_cte as a
left join last_12_cte as b on b.customer_key = a.customer_key
where b.customer_key is not null;


-- monthly churn rate.
-- get customer's signup or first activity year, first and last activity date.

with joined_cte as (
	select
		market,
		extract(month from order_date) as month_joined,
		customer_id,
		min(extract(month from order_date)) as first_activity_month,
		max(extract(month from order_date)) as last_activity_month
	from fact_superstore as a
	join dim_location as b on b.location_key = a.location_key
	join dim_customer as c on c.customer_key = a.customer_key
	group by 1,2,3
),

-- temporary dim_date cte.
temp_month_cte as (
	select
		max(month) as latest_month
	from dim_date
),

-- churned month.
churned_cte as (
	select
		a.market,
		a.month_joined,
		count(*) as churned_count
	from joined_cte as a
	cross join temp_month_cte as b
	where a.month_joined < (b.latest_month - 3)
	group by 1,2
),

-- month wise total user count.
total_cte as (
	select
		market, 
		month_joined,
		count(*) as total_user_count
	from joined_cte
	group by 1,2
)

-- left join total_cte with churned_cte to find out which customers are being null.
select
	a.market,
	a.month_joined,
	a.total_user_count,
	coalesce(b.churned_count, 0) as churned_user_count,
	round(coalesce(b.churned_count, 0) * 100 / a.total_user_count, 2) as monthly_churn_rate
from total_cte as a
left join churned_cte as b on b.market = a.market and b.month_joined = a.month_joined;

-- Yearly top 10 products by profit by each market.

with cte as (select
	b.market,
	extract(year from a.order_date) as order_year,
	c.product_name,
	sum(a.sales) as total_sales,
	sum(a.profit) as total_profit,
	rank() over (partition by b.market,extract(year from a.order_date) order by sum(a.profit) desc) as profit_rnk
from fact_superstore as a
join dim_location as b on b.location_key = a.location_key
join dim_product as c on c.product_key = a.product_key
group by 1,2,3)

select
	*
from cte
where profit_rnk <= 10;