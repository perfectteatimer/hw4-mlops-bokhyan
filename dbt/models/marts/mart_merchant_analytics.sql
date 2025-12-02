with base as (
    select
        merchant_id,
        merchant_name,
        us_state,
        amount,
        amount_bucket,
        is_fraud,
        customer_id
    from {{ ref('stg_transactions') }}
),
state_pref as (
    select
        merchant_id,
        us_state,
        count(*) as txn_count,
        row_number() over (partition by merchant_id order by count(*) desc) as rn
    from base
    group by merchant_id, us_state
),
primary_state as (
    select merchant_id, us_state as primary_state from state_pref where rn = 1
),
agg as (
    select
        merchant_id,
        max(merchant_name) as merchant_name,
        count(*) as transaction_count,
        sum(is_fraud) as fraud_count,
        sum(amount) as total_amount,
        avg(amount) as avg_amount,
        sum(case when amount_bucket in ('large', 'extra_large') then 1 else 0 end) as large_txn_count,
        count(distinct customer_id) as unique_customers
    from base
    group by merchant_id
),
scored as (
    select
        a.merchant_id,
        a.merchant_name,
        coalesce(p.primary_state, 'NA') as primary_state,
        a.transaction_count,
        a.total_amount,
        a.avg_amount,
        a.fraud_count,
        a.large_txn_count,
        a.unique_customers,
        case when a.transaction_count = 0 then 0 else a.fraud_count * 100.0 / a.transaction_count end as fraud_rate,
        case when a.transaction_count = 0 then 0 else a.large_txn_count * 1.0 / a.transaction_count end as large_txn_share
    from agg a
    left join primary_state p on a.merchant_id = p.merchant_id
)
select
    *,
    case when fraud_rate >= 5 or large_txn_share >= 0.4 then 1 else 0 end as suspicious_flag
from scored
order by fraud_rate desc
