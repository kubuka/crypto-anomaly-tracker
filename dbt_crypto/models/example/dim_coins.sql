{{ config(materialized = 'table') }} 

with source_data as (
    select id as coin_id,
        symbol,
        name
    from {{ source('silver_layer', 'crypto_prices') }}
)
select distinct coin_id,
    UPPER(symbol) as symbol,
    name
from source_data