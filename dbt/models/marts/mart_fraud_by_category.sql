with base as (
    select
        cat_id,
        amount,
        is_fraud
    from {{ ref('stg_transactions') }}
),
agg as (
    select
        cat_id,
        count(*) as transaction_count,
        sum(is_fraud) as fraud_count,
        sum(amount) as total_amount,
        sum(case when is_fraud = 1 then amount else 0 end) as fraud_amount,
        avg(amount) as avg_amount
    from base
    group by cat_id
)
select
    cat_id,
    transaction_count,
    fraud_count,
    case when transaction_count = 0 then 0 else fraud_count * 100.0 / transaction_count end as fraud_rate,
    total_amount,
    fraud_amount,
    avg_amount
from agg
order by fraud_rate desc
