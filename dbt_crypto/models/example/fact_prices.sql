{{ config(materialized = 'incremental', unique_key = 'fact_id') }} 

with source_data as (
    select id as coin_id,
        current_price,
        market_cap,
        total_volume,
        last_updated as price_timestamp
    from {{source('silver_layer', 'crypto_prices') }}
)

select 
    md5(concat(coin_id, cast(price_timestamp as varchar))) as fact_id,
    coin_id,
    current_price,
    market_cap,
    total_volume,
    price_timestamp
from source_data

{% if is_incremental() %}
    WHERE year = CAST(EXTRACT(YEAR FROM CURRENT_DATE) AS VARCHAR)
      AND month = CAST(EXTRACT(MONTH FROM CURRENT_DATE) AS VARCHAR)
      AND price_timestamp > (SELECT MAX(price_timestamp) FROM {{ this }})
{% endif %}