-- lets get each column and its datatype.
select column_name, data_type
from information_schema.columns
where table_name = 'superstore';

-- changing order_date and ship_date datatype.
alter table superstore
alter column order_date type date
using order_date::date;

alter table superstore
alter column ship_date type date
using ship_date::date;

-- deleting weeknum column.
alter table superstore
drop column weeknum;

select 
	max(order_date) as maximum_order_date,
	min(order_date) as minimum_order_date, -- minimum activity date of the whole dataset.
	max(ship_date) as maximum_ship_date, -- maximum activity date of the whole dataset.
	min(ship_date) as minimum_ship_date
from superstore;


-- **************************** DWH begins *************************************
-- create dim_location(location_key(pk), market, market2, country, state, region, city)

drop table if exists dim_location;
create table dim_location (
	location_key serial primary key,
	market text not null,
	market2 text,
	country text not null,
	state text not null,
	region text,
	city text not null,

	unique (market, country, state, city) -- composite unique key to force unique references.
);

with src as (
	select distinct
		market,
		market2,
		country,
		state,
		region,
		city
	from superstore
	where market is not null
	and country is not null
	and state is not null
	and city is not null
)

insert into dim_location (market, market2, country, state, region, city)
select * from src 
on conflict (market, country, state, city)
do update
set market2 = excluded.market2,
	region = excluded.region;

select * from dim_location;

-- create dim_product(product_key(PK,serial), product_id(business_id), product_name, sub_category, category)

-- data sanity check.
-- select count(product_id) from superstore
-- union all
-- select count(distinct product_id) from superstore;

drop table if exists dim_product;
create table dim_product (
	product_key serial primary key,
	product_id text unique not null,
	product_name text not null,
	sub_category text,
	category text,
	segment text
);

with src as (
	select distinct on (product_id)
	product_id,
	product_name,
	sub_category,
	category
	from superstore
	where product_id is not null
	order by product_id, product_name
)

insert into dim_product (product_id, product_name, sub_category, category)
select	* from src
on conflict (product_id)
do update
set     product_name = EXCLUDED.product_name,
        sub_category = EXCLUDED.sub_category,
        category     = EXCLUDED.category;


-- create dim_customer (customer_key, customer_id, customer_name)
drop table if exists dim_customer;
create table dim_customer (
	customer_key serial primary key,
	customer_id text unique not null,
	customer_name text
);

with src as (
	select
		distinct on (customer_id)
		customer_id,
		customer_name
	from superstore
	where customer_id is not null
	order by customer_id, customer_name -- deterministic pick.
)

insert into dim_customer (customer_id, customer_name)
select * from src
on conflict (customer_id)
do update
set	customer_name = excluded.customer_name;

-- dim_date (date_key, year, month, quarter, month_name, day_name, day_of_week, is_weekend)
drop table if exists dim_date;
create table dim_date (
	date_key BIGINT primary key,
	full_date date,
	year INT,
	month INT,
	quarter INT,
	month_name text,
	day_name text,
	day_of_week INT,
	is_weekend bool
);

insert into dim_date (date_key, full_date, year, month, quarter, month_name, day_name, day_of_week, is_weekend)
select
	to_char(rn, 'YYYYMMDD')::INT as date_key,
	rn::date as full_date,
	extract(year from rn)::INT as year,
	extract(month from rn)::INT as month,
	extract(quarter from rn)::INT as quarter,
	to_char(rn, 'month')::text as month_name,
	to_char(rn, 'day')::text as day_name,
	extract(DOW from rn)::INT as day_of_week, -- 1 as monday.
	(extract(DOW from rn)) in (0,6) as is_weekend -- 6 - saturday, 0 - sunday
from generate_series(
	(select min(order_date) from superstore),
	(select max(ship_date) from superstore),
	interval '1 day'
) as rn
on conflict (date_key)
do nothing;

-- altering dim_date table.
alter table dim_date
add constraint uq_dim_full_date unique (full_date);

-- create fact_superstore (fact_key(PK, serial), order_id, location_key(FK), product_key(FK), customer_key(FK), order_date_key(FK), 
-- ship_order_key(FK), order_date, quantity, sales, discount, profit, order_priority, ship_date, shipping_cost, ship_mode, delivery_time)

drop table if exists fact_superstore;
create table fact_superstore (
	fact_key serial primary key, -- surrogate key.
	order_id text not null, -- business key.
	location_key INT not null,
	product_key INT not null,
	customer_key INT not null,

	order_date_key bigint not null, -- Fk to dim_date(date_key)
	ship_date_key bigint not null, -- Fk to dim_date(date_key)

	order_date date,
	quantity INT,
	sales float,
	discount float,
	order_priority text,
	ship_date date,
	shipping_cost float,
	ship_mode text,
	delivery_time int,

	-- Fks.
	foreign key (location_key) references dim_location(location_key),
	foreign key (product_key) references dim_product(product_key),
	foreign key (customer_key) references dim_customer(customer_key),
	foreign key (order_date_key) references dim_date(date_key),
	foreign key (ship_date_key) references dim_date(date_key) -- for both key SQL joins and further analysis.
);

-- changing unique constraints.
truncate table fact_superstore;

-- adding new column line_num for better data incremental by order_id.
alter table fact_superstore
add column line_num int;

-- adding new column profit.
alter table fact_superstore
add column profit decimal(10,2);

-- creating unique index on order_id and product_key.
create unique index if not exists idx_uni_fact_order_key on fact_superstore(order_id, line_num);

-- creating a CTE to give every same second entry a line_num.
with staged as (
	select
		*,
		row_number() over(partition by order_id order by product_id, quantity, profit, sales) as line_num
	from superstore
)

insert into fact_superstore (order_id, location_key, product_key, customer_key, order_date_key, ship_date_key,
order_date, quantity, sales, discount, profit, order_priority, ship_date, shipping_cost, ship_mode, delivery_time, line_num)
select
	main.order_id,
	loc.location_key,
	prod.product_key,
	cust.customer_key,
	
	date1.date_key as order_date_key,
	date2.date_key as ship_date_key,

	main.order_date,
	main.quantity,
	main.sales,
	main.discount,
	main.profit,
	
	main.order_priority,
	main.ship_date,
	main.shipping_cost,
	main.ship_mode,
	main.delivery_time,
	main.line_num
	
from staged as main
join dim_location as loc on loc.market = main.market
	and loc.market2 = main.market2
	and loc.country = main.country
	and loc.state = main.state
	and loc.region = main.region
	and loc.city = main.city -- dim_location + superstore ends.
	
join dim_product as prod on prod.product_id = main.product_id -- dim_product + superstore ends.
join dim_customer as cust on cust.customer_id = main.customer_id -- dim_customer + superstore ends.

join dim_date as date1 on date1.full_date = main.order_date
join dim_date as date2 on date2.full_date = main.ship_date -- joining both order_date and ship_date.

on conflict (order_id, line_num)
do nothing;

-- data sanity check.
select 'main_dataset', count(*) from superstore
union all
select 'fact_dataset', count(*) from superstore;

-- null FK check.
select
	*
from fact_superstore as a
join dim_product as b on b.product_key = a.product_key
where b.product_key is null;

select fact_key from fact_superstore;
-- creating indexes for better performances while joining and multiple common calculations.
create index if not exists idx_fact_fact_key on fact_superstore(fact_key);
create index if not exists idx_dim_product_key on dim_product(product_key);
create index if not exists idx_dim_customer_key on dim_customer(customer_key);
create index if not exists idx_dim_location_key on dim_location(location_key);
create index if not exists idx_dim_date_key on dim_date(date_key);

-- indexes on fact date keys.
create index if not exists idx_fact_order_date_key on fact_superstore(order_date_key);
create index if not exists idx_fact_ship_date_key on fact_superstore(ship_date_key);


