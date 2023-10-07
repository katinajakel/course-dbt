{{
  config(
    materialized='table'
  )
}}

select     
    order_id
    , user_id
    , address_id
    , created_at
    , order_cost
    , shipping_cost
    , order_total
    , tracking_id
    , estimated_delivery_at
    , delivered_at
    , status
from {{ ref('stg_postgres__orders') }}
where
    status in ('delivered')
