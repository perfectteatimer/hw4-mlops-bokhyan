with base as (
    select
        transaction_date,
        us_state,
        amount,
        amount_bucket,
        is_fraud
    from {{ ref('stg_transactions') }}
),
agg as (
    select
        transaction_date,
        us_state,
        count(*) as transaction_count,
        sum(amount) as total_amount,
        avg(amount) as avg_amount,
        percentile_cont(0.95) within group (order by amount) as p95_amount,
        sum(case when amount_bucket in ('large', 'extra_large') then 1 else 0 end) as large_txn_count,
        sum(is_fraud) as fraud_count,
        sum(case when is_fraud = 1 then amount else 0 end) as fraud_amount
    from base
    group by transaction_date, us_state
)
select
    transaction_date,
    us_state,
    transaction_count,
    total_amount,
    avg_amount,
    p95_amount,
    large_txn_count,
    case when transaction_count = 0 then 0 else large_txn_count * 1.0 / transaction_count end as large_txn_share,
    fraud_count,
    fraud_amount,
    case when transaction_count = 0 then 0 else fraud_count * 1.0 / transaction_count end as fraud_rate
from agg
