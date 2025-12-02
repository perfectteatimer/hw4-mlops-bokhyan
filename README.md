# HW 4 bokhyan mlops

## Project highlights
- DuckDB profile stored in `dbt/profiles.yml` using local file `dbt/data/transactions.duckdb`.
- Seeds: `transactions.csv` (raw data) and `states.csv` (51 states including DC).
- Macro: `amount_bucket` to segment amounts (small/medium/large/extra_large/unknown).
- Staging: `stg_transactions` normalizes types, genders, ids, dates, day names, large-amount flag, joins state names.
- Marts (tables):
  - `mart_daily_state_metrics` (daily totals, fraud_rate, large_txn_share, p95)
  - `mart_fraud_by_category` (fraud by category)
  - `mart_fraud_by_state` (fraud geography, unique actors)
  - `mart_customer_risk_profile` (risk buckets per customer)
  - `mart_hourly_fraud_pattern` (weekday/hour patterns)
  - `mart_merchant_analytics` (merchant performance and suspicious flag)
- Tests: schema tests, dbt_expectations checks, singular SQL tests, and a unit test for the staging model.
- Linting: sqlfluff configured for dbt + DuckDB, pre-commit hooks ready.
- Docs: `dbt docs generate` output in `dbt/target`; quick DAG sketch stored at `docs/dag.png`.

## How to run
0) Delete any possible trash
```bash
rm -rf dbt/dbt_packages
```

1) Install deps
```bash
pip install -r requirements.txt
```

2) Pull dbt packages
```bash
cd dbt
DBT_PROFILES_DIR=. dbt deps
```

3) Build everything (seed → run → test)
```bash
DBT_PROFILES_DIR=. dbt seed
DBT_PROFILES_DIR=. dbt run
DBT_PROFILES_DIR=. dbt test
```
For the new dbt 1.8 unit test: `DBT_PROFILES_DIR=. dbt test --select test_type:unit`.

4) Generate docs
```bash
cd dbt
DBT_PROFILES_DIR=. dbt docs generate
DBT_PROFILES_DIR=. dbt docs serve --port 8080  # optional live UI
```

5) Lint SQL
```bash
make lint       # sqlfluff lint
make fmt        # sqlfluff fix
```

6) Pre-commit
```bash
pre-commit install
pre-commit run --all-files
```

