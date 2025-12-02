with source as (
    select * from {{ source('transactions_db', 'transactions') }}
),
renamed as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'transaction_time',
            'merch',
            'amount',
            'cat_id',
            'post_code'
        ]) }} as transaction_id,
        cast(transaction_time as timestamp) as transaction_ts,
        cast(date(transaction_time) as date) as transaction_date,
        date_trunc('hour', cast(transaction_time as timestamp)) as transaction_hour,
        {{ dbt_date.day_name('cast(transaction_time as timestamp)') }} as day_name,
        merch as merchant_name,
        {{ dbt_utils.generate_surrogate_key(['merch']) }} as merchant_id,
        cat_id,
        cast(amount as double) as amount,
        {{ amount_bucket('cast(amount as double)') }} as amount_bucket,
        case
            when upper(gender) in ('M', 'MALE') then 'M'
            when upper(gender) in ('F', 'FEMALE') then 'F'
            else 'U'
        end as gender,
        {{ dbt_utils.generate_surrogate_key(['name_1', 'name_2', 'street', 'post_code']) }} as customer_id,
        concat_ws(' ', nullif(name_1, ''), nullif(name_2, '')) as customer_full_name,
        street,
        one_city as city,
        upper(us_state) as us_state,
        post_code,
        cast(lat as double) as customer_lat,
        cast(lon as double) as customer_lon,
        cast(merchant_lat as double) as merchant_lat,
        cast(merchant_lon as double) as merchant_lon,
        nullif(jobs, '') as job_title,
        cast(population_city as bigint) as population_city,
        cast(target as integer) as is_fraud,
        case when {{ amount_bucket('cast(amount as double)') }} in ('large', 'extra_large') then 1 else 0 end as is_large_amount
    from source
),
with_states as (
    select
        r.*,
        s.state_name
    from renamed r
    left join {{ ref('states') }} s
        on r.us_state = s.state_code
)
select * from with_states
