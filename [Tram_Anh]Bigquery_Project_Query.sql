-- Big project for SQL
-- Link instruction: https://docs.google.com/spreadsheets/d/1WnBJsZXj_4FDi2DyfLH1jkWtfTridO2icWbWCh7PLs8/edit#gid=0


-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL
select
    left(date,6) as month,
    sum(totals.visits) as visits,
    sum(totals.pageviews) as pageviews,
    sum(totals.transactions) as transactions,
    safe_divide(sum(totals.totalTransactionRevenue),pow(10,6)) as revenue
from `bigquery-public-data.google_analytics_sample.ga_sessions_*`
where _table_suffix between '20170101' and '20170331'
group by month
order by month;

-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL
select
    trafficSource.source as source,
    sum(totals.visits) as total_visits,
    sum(totals.bounces) as total_no_of_bounces,
    sum(totals.bounces)/sum(totals.visits) * 100 as bounce_rate
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
group by trafficSource.source
order by total_visits desc;

-- Query 3: Revenue by traffic source by week, by month in June 2017
#standardSQL
with week_revenue as(
select
    'Week' as time_type,
    format_date('%Y%W', parse_date('%Y%m%d',date)) as time,
    trafficSource.source as source,
    safe_divide(sum(totals.totalTransactionRevenue),pow(10,6)) as revenue
from `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
group by source, time
order by revenue desc),

month_revenue as (
select
    'Month' as time_type,
    format_date('%Y%m', parse_date('%Y%m%d',date)) as time,
    trafficSource.source as source,
    safe_divide(sum(totals.totalTransactionRevenue),pow(10,6)) as revenue
from `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
group by source,time
order by revenue desc)

select
    *
from week_revenue
union all
select
    *
from month_revenue
order by revenue desc;

--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL
with purchase as (
select
    format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
    safe_divide(sum(totals.pageviews), count(distinct fullVisitorId)) as avg_pageviews_purchase
from `bigquery-public-data.google_analytics_sample.ga_sessions_*`
where _table_suffix between '20170601' and '20170731'
and totals.transactions >= 1 
group by month),

non_purchase as (
select
    format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
    safe_divide(sum(totals.pageviews),count(distinct fullVisitorId)) as avg_pageviews_non_purchase
from `bigquery-public-data.google_analytics_sample.ga_sessions_*`
where _table_suffix between '20170601' and '20170731'
and totals.transactions is null 
group by month)

select
    purchase.month,
    avg_pageviews_purchase,
    avg_pageviews_non_purchase
from purchase
join non_purchase using(month);

-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL
select
    format_date('%Y%m', parse_date('%Y%m%d',date)) as Month,
    safe_divide(sum(totals.transactions),count(distinct fullVisitorId)) as Avg_total_transactions_per_user
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
where totals.transactions >= 1
group by Month;

-- Query 06: Average amount of money spent per session
#standardSQL
select
    format_date('%Y%m', parse_date('%Y%m%d',date)) as Month,
    safe_divide(sum(totals.totalTransactionRevenue), count(distinct visitId)) as avg_revenue_by_user_per_visit
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
where totals.transactions >= 1
group by Month;

-- Query 07: Products purchased by customers who purchased product A (Classic Ecommerce)
#standardSQL
with customer_list as (
select
    distinct fullVisitorId as customer
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
unnest (hits) as hits,
unnest (hits.product) as product
where product.v2ProductName = "YouTube Men's Vintage Henley"
and hits.ecommerceaction.action_type = '6')

select
    distinct product.v2ProductName as other_purchased_products,
    sum(product.productQuantity) as quantity
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
unnest (hits) as hits,
unnest (hits.product) as product
where fullVisitorId in 
    (select
        customer
    from customer_list)
and hits.ecommerceaction.action_type = '6'
and product.v2ProductName != "YouTube Men's Vintage Henley"
group by other_purchased_products
order by quantity desc

--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL
with product_view as (
select
    format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
    count(visitId) as num_product_view
from `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
unnest (hits) as hits
where _table_suffix between '20170101' and '20170331'
and hits.ecommerceaction.action_type = '2'
group by month),

addtocart as (
select
    format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
    count(visitId) as num_addtocart
from `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
unnest (hits) as hits
where _table_suffix between '20170101' and '20170331'
and hits.ecommerceaction.action_type = '3'
group by month),

purchase as (
select
    format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
    count(product.v2ProductName) as num_purchase
from `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
unnest (hits) as hits,
unnest (hits.product) as product
where _table_suffix between '20170101' and '20170331'
and hits.ecommerceaction.action_type = '6'
group by month)

select
    month,
    num_product_view,
    num_addtocart,
    num_purchase,
    round(num_addtocart/num_product_view * 100, 2) as add_to_cart_rate,
    round(num_purchase/num_product_view * 100, 2) as purchase_rate
from product_view
join addtocart using (month)
join purchase using (month)
order by month
