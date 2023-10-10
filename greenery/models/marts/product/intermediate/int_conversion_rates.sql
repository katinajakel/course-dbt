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
    product_traffic.name
    , product_traffic.product_id
    , product_traffic.product_checkout
    , product_orders.inventory
    , product_orders.price
    , count(distinct session_id) as num_unique_sessions
    , (product_checkout)/(num_unique_sessions) as overall_conversion_rate
from
    {{ ref('stg_postgres__events') }} as events
    left join {{ ref('fact_product_events') }} as product_traffic on product_traffic.product_id = events.product_id
    left join {{ ref('fact_orders') }} as product_orders on product_orders.name = product_traffic.name
where product_traffic.product_id is not null
group by 1, 2, 3, 4, 5
order by 1 asc