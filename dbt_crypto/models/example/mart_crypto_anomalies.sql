{{ config(
    materialized='table'
) }}

with hisorical_metrics as (
    select
        f.fact_id,
        f.coin_id,
        d.name as coin_name,
        d.symbol as coin_symbol,
        f.current_price,
        f.price_timestamp,
        --obliczenie sredniej kroczacej
        avg(f.current_price) over 
        (partition by f.coin_id order by f.price_timestamp rows between 10 preceding and current row)
        as rolling_avg,
        --obliczenie odchylenia
        coalesce(stddev(f.current_price) over(partition by f.coin_id order by f.price_timestamp rows between 10 preceding and current row),0)
        as rolling_stddev
    from {{ ref('fact_prices') }} f
    join {{ ref('dim_coins') }} d on f.coin_id = d.coin_id
),

anomaly_detection as (
    select
        *,
        abs(current_price - rolling_avg) as price_delta,
        -- anomalia: delta * 3 stddev
        case
            when rolling_stddev > 0 and abs(current_price - rolling_avg) > (3 * rolling_stddev) then true
            else false
        end as is_anomaly
    from hisorical_metrics 
)

select
    fact_id,
    coin_id,
    coin_name,
    coin_symbol,
    current_price,
    price_timestamp,
    rolling_avg,
    rolling_stddev as rolling_stddev,
    price_delta,
    is_anomaly
from anomaly_detection