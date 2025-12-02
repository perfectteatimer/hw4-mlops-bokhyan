with base as (
    select
        day_name,
        transaction_hour,
        amount,
        is_fraud
    from {{ ref('stg_transactions') }}
),

bucketed as (
    select
        day_name,
        extract(hour from transaction_hour) as hour_of_day,
        amount,
        is_fraud
    from base
)

select
    day_name,
    hour_of_day,
    count(*) as transaction_count,
    sum(is_fraud) as fraud_count,
    case when count(*) = 0 then 0 else sum(is_fraud) * 100.0 / count(*) end as fraud_rate,
    avg(amount) as avg_amount,
    percentile_cont(0.95) within group (order by amount) as p95_amount
from bucketed
group by day_name, hour_of_day
order by fraud_rate desc
