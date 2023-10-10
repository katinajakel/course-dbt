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
order_items as (
    select
        *
    from
        {{ ref('stg_postgres__order_items') }}
)
select 
    products.name
    , products.price
    , products.inventory
    , sum(order_items.quantity) as quantity_sold
    , count(distinct order_items.order_id) as count_of_orders
    from products
    left join order_items on order_items.product_id = products.product_id
    group by 1, 2, 3