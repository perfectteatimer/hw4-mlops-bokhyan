with base as (
    select
        us_state,
        amount,
        is_fraud,
        customer_id,
        merchant_id
    from {{ ref('stg_transactions') }}
),
agg as (
    select
        us_state,
        count(*) as transaction_count,
        count(distinct customer_id) as unique_customers,
        count(distinct merchant_id) as unique_merchants,
        sum(is_fraud) as fraud_count,
        sum(amount) as total_amount,
        sum(case when is_fraud = 1 then amount else 0 end) as fraud_amount,
        avg(amount) as avg_amount
    from base
    group by us_state
)
select
    us_state,
    transaction_count,
    unique_customers,
    unique_merchants,
    total_amount,
    fraud_amount,
    avg_amount,
    fraud_count,
    case when transaction_count = 0 then 0 else fraud_count * 100.0 / transaction_count end as fraud_rate
from agg
order by fraud_rate desc
