{{
  config(
    materialized='table'
  )
}}

with products as (
    select
        *
    from
        {{ ref('stg_postgres__products') }}
),
events as (
    select
        *
    from
        {{ ref('stg_postgres__events') }}
),
order_items as (
    select
        *
    from
        {{ ref('stg_postgres__order_items') }}
    
)
select 
    products.name
    , products.product_id
    , sum(case when events.event_type = 'page_view' then 1 else 0 end) as product_page_views
    , sum(case when events.event_type = 'add_to_cart' then 1 else 0 end) as product_add_to_cart
    , count(distinct order_items.order_id) as product_checkout
from products
left join events on events.product_id = products.product_id
left join order_items on order_items.product_id = products.product_id
group by 1, 2
order by 1 asc