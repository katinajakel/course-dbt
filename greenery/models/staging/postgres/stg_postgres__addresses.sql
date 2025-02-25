{{
  config(
    materialized='view'
  )
}}

select 
    address_id
    , address
    , zipcode
    , state
    , country
from {{ source('postgres', 'addresses') }} 