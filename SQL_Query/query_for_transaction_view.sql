create or refresh materialized view ecommerce.gold.transactions_2view as (

select i.*,
c.year,
c.month_name,
c.day_name,
c.is_weekend,
c.quarter,
c.week,
p.sku,
p.category_code,
p.category_name,
p.brand_code,
p.brand_name,
p.color,
p.size,
p.rating_count,
extract(hour from transaction_ts) as hour_of_day
from ecommerce.gold.gld_fact_order_items i join ecommerce.gold.gold_date c on i.date_id = c.date_id
join ecommerce.gold.gold_products p on i.product_id = p.product_id

);
