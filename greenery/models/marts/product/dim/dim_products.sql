{{
  config(
    materialized='table'
  )
}}
select
    product.product_id
    , name
    , price
    , inventory
    , count(case when event_type = 'page_view' then event_id end) as total_views
    , count(case when event_type = 'add_to_cart' then event_id end) as count_added_to_cart

from {{ ref('stg_postgres__products') }} as product
    left join {{ ref('stg_postgres__events') }} as events 
    on product.product_id = events.product_id
group by 1,2,3,4
order by 5 desc