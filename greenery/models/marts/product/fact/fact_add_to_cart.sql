{{
  config(
    materialized='table'
  )
}}

select
    event_id
    , session_id
    , user_id
    , event_type
    , page_url
    , created_at
    , order_id
    , product_id

from {{ ref('stg_postgres__events') }}
where event_type in ('add_to_cart')