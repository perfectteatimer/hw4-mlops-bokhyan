with base as (
    select
        customer_id,
        customer_full_name,
        us_state,
        amount,
        amount_bucket,
        is_fraud,
        merchant_id,
        transaction_date
    from {{ ref('stg_transactions') }}
),

agg as (
    select
        customer_id,
        max(customer_full_name) as customer_full_name,
        max(us_state) as home_state,
        count(*) as transaction_count,
        sum(is_fraud) as fraud_count,
        case when count(*) = 0 then 0 else sum(is_fraud) * 100.0 / count(*) end as fraud_rate,
        sum(amount) as total_amount,
        avg(amount) as avg_amount,
        max(transaction_date) as last_transaction_date,
        sum(case when amount_bucket in ('large', 'extra_large') then 1 else 0 end) as large_txn_count,
        count(distinct merchant_id) as merchant_diversity
    from base
    group by customer_id
),

scored as (
    select
        *,
        case when transaction_count = 0 then 0 else large_txn_count * 1.0 / transaction_count end as large_txn_share
    from agg
),

final as (
    select
        customer_id,
        customer_full_name,
        home_state,
        transaction_count,
        fraud_count,
        fraud_rate,
        total_amount,
        avg_amount,
        last_transaction_date,
        large_txn_share,
        merchant_diversity,
        case
            when fraud_rate >= 5 or large_txn_share >= 0.4 then 'HIGH'
            when fraud_rate >= 2 or large_txn_share >= 0.2 then 'MEDIUM'
            else 'LOW'
        end as risk_level
    from scored
)

select * from final
