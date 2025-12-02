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
DBT_PROFILES_DIR=. dbt docs serve --port 8080  
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

## Results
```bash
(base) roman@romans-MacBook-Pro-2 hw4-mlops-bokhyan % pip install -r requirements.txt
Requirement already satisfied: dbt-core==1.8.2 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from -r requirements.txt (line 1)) (1.8.2)
Requirement already satisfied: dbt-duckdb==1.8.2 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from -r requirements.txt (line 2)) (1.8.2)
Requirement already satisfied: sqlfluff==3.0.7 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from -r requirements.txt (line 3)) (3.0.7)
Requirement already satisfied: sqlfluff-templater-dbt==3.0.7 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from -r requirements.txt (line 4)) (3.0.7)
Requirement already satisfied: pre-commit==3.7.1 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from -r requirements.txt (line 5)) (3.7.1)
Requirement already satisfied: agate<1.10,>=1.7.0 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from dbt-core==1.8.2->-r requirements.txt (line 1)) (1.9.1)
Requirement already satisfied: Jinja2<4,>=3.1.3 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from dbt-core==1.8.2->-r requirements.txt (line 1)) (3.1.6)
Requirement already satisfied: mashumaro<4.0,>=3.9 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from mashumaro[msgpack]<4.0,>=3.9->dbt-core==1.8.2->-r requirements.txt (line 1)) (3.17)
Requirement already satisfied: logbook<1.6,>=1.5 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from dbt-core==1.8.2->-r requirements.txt (line 1)) (1.5.3)
Requirement already satisfied: click<9.0,>=8.0.2 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from dbt-core==1.8.2->-r requirements.txt (line 1)) (8.2.1)
Requirement already satisfied: networkx<4.0,>=2.3 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from dbt-core==1.8.2->-r requirements.txt (line 1)) (3.4.2)
Requirement already satisfied: protobuf<5,>=4.0.0 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from dbt-core==1.8.2->-r requirements.txt (line 1)) (4.25.8)
Requirement already satisfied: requests<3.0.0 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from dbt-core==1.8.2->-r requirements.txt (line 1)) (2.32.4)
Requirement already satisfied: pathspec<0.13,>=0.9 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from dbt-core==1.8.2->-r requirements.txt (line 1)) (0.12.1)
Requirement already satisfied: sqlparse<0.6.0,>=0.5.0 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from dbt-core==1.8.2->-r requirements.txt (line 1)) (0.5.4)
Requirement already satisfied: dbt-extractor<=0.6,>=0.5.0 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from dbt-core==1.8.2->-r requirements.txt (line 1)) (0.6.0)
Requirement already satisfied: minimal-snowplow-tracker<0.1,>=0.0.2 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from dbt-core==1.8.2->-r requirements.txt (line 1)) (0.0.2)
Requirement already satisfied: dbt-semantic-interfaces<0.6,>=0.5.1 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from dbt-core==1.8.2->-r requirements.txt (line 1)) (0.5.1)
Requirement already satisfied: dbt-common<2.0,>=1.0.4 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from dbt-core==1.8.2->-r requirements.txt (line 1)) (1.12.0)
Requirement already satisfied: dbt-adapters<2.0,>=1.1.1 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from dbt-core==1.8.2->-r requirements.txt (line 1)) (1.9.0)
Requirement already satisfied: packaging>20.9 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from dbt-core==1.8.2->-r requirements.txt (line 1)) (23.2)
Requirement already satisfied: pytz>=2015.7 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from dbt-core==1.8.2->-r requirements.txt (line 1)) (2024.2)
Requirement already satisfied: pyyaml>=6.0 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from dbt-core==1.8.2->-r requirements.txt (line 1)) (6.0.1)
Requirement already satisfied: daff>=1.3.46 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from dbt-core==1.8.2->-r requirements.txt (line 1)) (1.4.2)
Requirement already satisfied: typing-extensions>=4.4 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from dbt-core==1.8.2->-r requirements.txt (line 1)) (4.15.0)
Requirement already satisfied: duckdb>=1.0.0 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from dbt-duckdb==1.8.2->-r requirements.txt (line 2)) (1.4.2)
Requirement already satisfied: appdirs in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from sqlfluff==3.0.7->-r requirements.txt (line 3)) (1.4.4)
Requirement already satisfied: chardet in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from sqlfluff==3.0.7->-r requirements.txt (line 3)) (3.0.4)
Requirement already satisfied: colorama>=0.3 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from sqlfluff==3.0.7->-r requirements.txt (line 3)) (0.4.6)
Requirement already satisfied: diff-cover>=2.5.0 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from sqlfluff==3.0.7->-r requirements.txt (line 3)) (9.7.2)
Requirement already satisfied: pytest in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from sqlfluff==3.0.7->-r requirements.txt (line 3)) (9.0.1)
Requirement already satisfied: regex in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from sqlfluff==3.0.7->-r requirements.txt (line 3)) (2025.11.3)
Requirement already satisfied: tblib in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from sqlfluff==3.0.7->-r requirements.txt (line 3)) (3.2.2)
Requirement already satisfied: tqdm in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from sqlfluff==3.0.7->-r requirements.txt (line 3)) (4.67.0)
Requirement already satisfied: jinja2-simple-tags>=0.3.1 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from sqlfluff-templater-dbt==3.0.7->-r requirements.txt (line 4)) (0.6.1)
Requirement already satisfied: markupsafe in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from sqlfluff-templater-dbt==3.0.7->-r requirements.txt (line 4)) (2.1.3)
Requirement already satisfied: pydantic in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from sqlfluff-templater-dbt==3.0.7->-r requirements.txt (line 4)) (2.11.7)
Requirement already satisfied: rich in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from sqlfluff-templater-dbt==3.0.7->-r requirements.txt (line 4)) (14.2.0)
Requirement already satisfied: ruamel.yaml in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from sqlfluff-templater-dbt==3.0.7->-r requirements.txt (line 4)) (0.18.14)
Requirement already satisfied: cfgv>=2.0.0 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from pre-commit==3.7.1->-r requirements.txt (line 5)) (3.5.0)
Requirement already satisfied: identify>=1.0.0 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from pre-commit==3.7.1->-r requirements.txt (line 5)) (2.6.15)
Requirement already satisfied: nodeenv>=0.11.1 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from pre-commit==3.7.1->-r requirements.txt (line 5)) (1.9.1)
Requirement already satisfied: virtualenv>=20.10.0 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from pre-commit==3.7.1->-r requirements.txt (line 5)) (20.24.3)
Requirement already satisfied: Babel>=2.0 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from agate<1.10,>=1.7.0->dbt-core==1.8.2->-r requirements.txt (line 1)) (2.14.0)
Requirement already satisfied: isodate>=0.5.4 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from agate<1.10,>=1.7.0->dbt-core==1.8.2->-r requirements.txt (line 1)) (0.6.1)
Requirement already satisfied: leather>=0.3.2 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from agate<1.10,>=1.7.0->dbt-core==1.8.2->-r requirements.txt (line 1)) (0.4.0)
Requirement already satisfied: parsedatetime!=2.5,>=2.1 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from agate<1.10,>=1.7.0->dbt-core==1.8.2->-r requirements.txt (line 1)) (2.6)
Requirement already satisfied: python-slugify>=1.2.1 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from agate<1.10,>=1.7.0->dbt-core==1.8.2->-r requirements.txt (line 1)) (8.0.4)
Requirement already satisfied: pytimeparse>=1.1.5 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from agate<1.10,>=1.7.0->dbt-core==1.8.2->-r requirements.txt (line 1)) (1.1.8)
Requirement already satisfied: deepdiff<8.0,>=7.0 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from dbt-common<2.0,>=1.0.4->dbt-core==1.8.2->-r requirements.txt (line 1)) (7.0.1)
Requirement already satisfied: jsonschema<5.0,>=4.0 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from dbt-common<2.0,>=1.0.4->dbt-core==1.8.2->-r requirements.txt (line 1)) (4.20.0)
Requirement already satisfied: python-dateutil<3.0,>=2.0 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from dbt-common<2.0,>=1.0.4->dbt-core==1.8.2->-r requirements.txt (line 1)) (2.8.2)
Requirement already satisfied: importlib-metadata<7,>=6.0 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from dbt-semantic-interfaces<0.6,>=0.5.1->dbt-core==1.8.2->-r requirements.txt (line 1)) (6.11.0)
Requirement already satisfied: more-itertools<11.0,>=8.0 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from dbt-semantic-interfaces<0.6,>=0.5.1->dbt-core==1.8.2->-r requirements.txt (line 1)) (10.8.0)
Requirement already satisfied: Pygments<3.0.0,>=2.19.1 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from diff-cover>=2.5.0->sqlfluff==3.0.7->-r requirements.txt (line 3)) (2.19.2)
Requirement already satisfied: pluggy<2,>=0.13.1 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from diff-cover>=2.5.0->sqlfluff==3.0.7->-r requirements.txt (line 3)) (1.6.0)
Requirement already satisfied: msgpack>=0.5.6 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from mashumaro[msgpack]<4.0,>=3.9->dbt-core==1.8.2->-r requirements.txt (line 1)) (1.1.1)
Requirement already satisfied: six<2.0,>=1.9.0 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from minimal-snowplow-tracker<0.1,>=0.0.2->dbt-core==1.8.2->-r requirements.txt (line 1)) (1.16.0)
Requirement already satisfied: annotated-types>=0.6.0 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from pydantic->sqlfluff-templater-dbt==3.0.7->-r requirements.txt (line 4)) (0.7.0)
Requirement already satisfied: pydantic-core==2.33.2 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from pydantic->sqlfluff-templater-dbt==3.0.7->-r requirements.txt (line 4)) (2.33.2)
Requirement already satisfied: typing-inspection>=0.4.0 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from pydantic->sqlfluff-templater-dbt==3.0.7->-r requirements.txt (line 4)) (0.4.1)
Requirement already satisfied: charset_normalizer<4,>=2 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from requests<3.0.0->dbt-core==1.8.2->-r requirements.txt (line 1)) (3.3.2)
Requirement already satisfied: idna<4,>=2.5 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from requests<3.0.0->dbt-core==1.8.2->-r requirements.txt (line 1)) (2.10)
Requirement already satisfied: urllib3<3,>=1.21.1 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from requests<3.0.0->dbt-core==1.8.2->-r requirements.txt (line 1)) (2.1.0)
Requirement already satisfied: certifi>=2017.4.17 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from requests<3.0.0->dbt-core==1.8.2->-r requirements.txt (line 1)) (2023.7.22)
Requirement already satisfied: distlib<1,>=0.3.7 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from virtualenv>=20.10.0->pre-commit==3.7.1->-r requirements.txt (line 5)) (0.3.7)
Requirement already satisfied: filelock<4,>=3.12.2 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from virtualenv>=20.10.0->pre-commit==3.7.1->-r requirements.txt (line 5)) (3.12.2)
Requirement already satisfied: platformdirs<4,>=3.9.1 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from virtualenv>=20.10.0->pre-commit==3.7.1->-r requirements.txt (line 5)) (3.10.0)
Requirement already satisfied: iniconfig>=1.0.1 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from pytest->sqlfluff==3.0.7->-r requirements.txt (line 3)) (2.3.0)
Requirement already satisfied: markdown-it-py>=2.2.0 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from rich->sqlfluff-templater-dbt==3.0.7->-r requirements.txt (line 4)) (4.0.0)
Requirement already satisfied: ruamel.yaml.clib>=0.2.7 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from ruamel.yaml->sqlfluff-templater-dbt==3.0.7->-r requirements.txt (line 4)) (0.2.12)
Requirement already satisfied: ordered-set<4.2.0,>=4.1.0 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from deepdiff<8.0,>=7.0->dbt-common<2.0,>=1.0.4->dbt-core==1.8.2->-r requirements.txt (line 1)) (4.1.0)
Requirement already satisfied: zipp>=0.5 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from importlib-metadata<7,>=6.0->dbt-semantic-interfaces<0.6,>=0.5.1->dbt-core==1.8.2->-r requirements.txt (line 1)) (3.23.0)
Requirement already satisfied: attrs>=22.2.0 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from jsonschema<5.0,>=4.0->dbt-common<2.0,>=1.0.4->dbt-core==1.8.2->-r requirements.txt (line 1)) (23.2.0)
Requirement already satisfied: jsonschema-specifications>=2023.03.6 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from jsonschema<5.0,>=4.0->dbt-common<2.0,>=1.0.4->dbt-core==1.8.2->-r requirements.txt (line 1)) (2023.12.1)
Requirement already satisfied: referencing>=0.28.4 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from jsonschema<5.0,>=4.0->dbt-common<2.0,>=1.0.4->dbt-core==1.8.2->-r requirements.txt (line 1)) (0.32.1)
Requirement already satisfied: rpds-py>=0.7.1 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from jsonschema<5.0,>=4.0->dbt-common<2.0,>=1.0.4->dbt-core==1.8.2->-r requirements.txt (line 1)) (0.16.2)
Requirement already satisfied: mdurl~=0.1 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from markdown-it-py>=2.2.0->rich->sqlfluff-templater-dbt==3.0.7->-r requirements.txt (line 4)) (0.1.2)
Requirement already satisfied: text-unidecode>=1.3 in /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages (from python-slugify>=1.2.1->agate<1.10,>=1.7.0->dbt-core==1.8.2->-r requirements.txt (line 1)) (1.3)

[notice] A new release of pip is available: 25.0.1 -> 25.3
[notice] To update, run: pip install --upgrade pip
(base) roman@romans-MacBook-Pro-2 hw4-mlops-bokhyan % cd dbt
DBT_PROFILES_DIR=. dbt deps
12:01:15  Running with dbt=1.8.2
12:01:16  [WARNING]: Deprecated functionality
The `calogica/dbt_date` package is deprecated in favor of
`godatadriven/dbt_date`. Please update your `packages.yml` configuration to use
`godatadriven/dbt_date` instead.
12:01:17  Installing dbt-labs/dbt_utils
12:01:17  Installed from version 1.3.2
12:01:17  Up to date!
12:01:17  Installing calogica/dbt_date
12:01:18  Installed from version 0.10.1
12:01:18  Up to date!
12:01:18  Installing calogica/dbt_expectations
12:01:19  Installed from version 0.10.4
12:01:19  Up to date!
(base) roman@romans-MacBook-Pro-2 dbt % DBT_PROFILES_DIR=. dbt seed
DBT_PROFILES_DIR=. dbt run
DBT_PROFILES_DIR=. dbt test
12:01:26  Running with dbt=1.8.2
12:01:26  Registered adapter: duckdb=1.8.2
12:01:26  Found 7 models, 56 data tests, 2 seeds, 1 source, 805 macros, 1 unit test
12:01:26  
12:01:26  Concurrency: 4 threads (target='dev')
12:01:26  
12:01:26  1 of 2 START seed file main_transactions_db.states ............................. [RUN]
12:01:26  2 of 2 START seed file main_transactions_db.transactions ....................... [RUN]
12:01:27  1 of 2 OK loaded seed file main_transactions_db.states ......................... [INSERT 51 in 0.45s]
12:01:53  2 of 2 OK loaded seed file main_transactions_db.transactions ................... [INSERT 786431 in 26.43s]
12:01:53  
12:01:53  Finished running 2 seeds in 0 hours 0 minutes and 26.58 seconds (26.58s).
12:01:53  
12:01:53  Completed successfully
12:01:53  
12:01:53  Done. PASS=2 WARN=0 ERROR=0 SKIP=0 TOTAL=2
12:01:55  Running with dbt=1.8.2
12:01:55  Registered adapter: duckdb=1.8.2
12:01:55  Found 7 models, 56 data tests, 2 seeds, 1 source, 805 macros, 1 unit test
12:01:55  
12:01:55  Concurrency: 4 threads (target='dev')
12:01:55  
12:01:55  1 of 7 START sql view model main_staging.stg_transactions ...................... [RUN]
12:01:55  1 of 7 OK created sql view model main_staging.stg_transactions ................. [OK in 0.07s]
12:01:55  2 of 7 START sql table model main_marts.mart_customer_risk_profile ............. [RUN]
12:01:55  3 of 7 START sql table model main_marts.mart_daily_state_metrics ............... [RUN]
12:01:55  4 of 7 START sql table model main_marts.mart_fraud_by_category ................. [RUN]
12:01:55  5 of 7 START sql table model main_marts.mart_fraud_by_state .................... [RUN]
12:01:56  4 of 7 OK created sql table model main_marts.mart_fraud_by_category ............ [OK in 0.10s]
12:01:56  6 of 7 START sql table model main_marts.mart_hourly_fraud_pattern .............. [RUN]
12:01:56  6 of 7 OK created sql table model main_marts.mart_hourly_fraud_pattern ......... [OK in 0.13s]
12:01:56  7 of 7 START sql table model main_marts.mart_merchant_analytics ................ [RUN]
12:01:56  5 of 7 OK created sql table model main_marts.mart_fraud_by_state ............... [OK in 0.29s]
12:01:56  3 of 7 OK created sql table model main_marts.mart_daily_state_metrics .......... [OK in 0.31s]
12:01:56  2 of 7 OK created sql table model main_marts.mart_customer_risk_profile ........ [OK in 0.38s]
12:01:56  7 of 7 OK created sql table model main_marts.mart_merchant_analytics ........... [OK in 0.26s]
12:01:56  
12:01:56  Finished running 1 view model, 6 table models in 0 hours 0 minutes and 0.70 seconds (0.70s).
12:01:56  
12:01:56  Completed successfully
12:01:56  
12:01:56  Done. PASS=7 WARN=0 ERROR=0 SKIP=0 TOTAL=7
12:01:58  Running with dbt=1.8.2
12:01:58  Registered adapter: duckdb=1.8.2
12:01:58  Found 7 models, 56 data tests, 2 seeds, 1 source, 805 macros, 1 unit test
12:01:58  
12:01:58  Concurrency: 4 threads (target='dev')
12:01:58  
12:01:58  1 of 57 START test accepted_values_mart_customer_risk_profile_risk_level__HIGH__MEDIUM__LOW  [RUN]
12:01:58  2 of 57 START test accepted_values_mart_merchant_analytics_suspicious_flag__0__1  [RUN]
12:01:58  3 of 57 START test accepted_values_stg_transactions_amount_bucket__small__medium__large__extra_large__unknown  [RUN]
12:01:58  4 of 57 START test accepted_values_stg_transactions_gender__M__F__U ............ [RUN]
12:01:58  1 of 57 PASS accepted_values_mart_customer_risk_profile_risk_level__HIGH__MEDIUM__LOW  [PASS in 0.05s]
12:01:58  5 of 57 START test accepted_values_stg_transactions_is_fraud__0__1 ............. [RUN]
12:01:58  2 of 57 PASS accepted_values_mart_merchant_analytics_suspicious_flag__0__1 ..... [PASS in 0.06s]
12:01:58  3 of 57 PASS accepted_values_stg_transactions_amount_bucket__small__medium__large__extra_large__unknown  [PASS in 0.06s]
12:01:58  6 of 57 START test assert_fraud_rate_bounds .................................... [RUN]
12:01:58  4 of 57 PASS accepted_values_stg_transactions_gender__M__F__U .................. [PASS in 0.06s]
12:01:58  7 of 57 START test assert_no_negative_amounts .................................. [RUN]
12:01:58  8 of 57 START test dbt_expectations_expect_column_values_to_be_between_mart_customer_risk_profile_fraud_rate__100__0  [RUN]
12:01:58  5 of 57 PASS accepted_values_stg_transactions_is_fraud__0__1 ................... [PASS in 0.05s]
12:01:58  6 of 57 PASS assert_fraud_rate_bounds .......................................... [PASS in 0.03s]
12:01:58  9 of 57 START test dbt_expectations_expect_column_values_to_be_between_mart_daily_state_metrics_fraud_rate__1__0  [RUN]
12:01:58  8 of 57 PASS dbt_expectations_expect_column_values_to_be_between_mart_customer_risk_profile_fraud_rate__100__0  [PASS in 0.03s]
12:01:58  10 of 57 START test dbt_expectations_expect_column_values_to_be_between_mart_daily_state_metrics_large_txn_share__1__0  [RUN]
12:01:58  7 of 57 PASS assert_no_negative_amounts ........................................ [PASS in 0.04s]
12:01:58  11 of 57 START test dbt_expectations_expect_column_values_to_be_between_mart_fraud_by_category_fraud_rate__100__0  [RUN]
12:01:58  12 of 57 START test dbt_expectations_expect_column_values_to_be_between_mart_fraud_by_state_fraud_rate__100__0  [RUN]
12:01:58  11 of 57 PASS dbt_expectations_expect_column_values_to_be_between_mart_fraud_by_category_fraud_rate__100__0  [PASS in 0.03s]
12:01:58  10 of 57 PASS dbt_expectations_expect_column_values_to_be_between_mart_daily_state_metrics_large_txn_share__1__0  [PASS in 0.04s]
12:01:58  9 of 57 PASS dbt_expectations_expect_column_values_to_be_between_mart_daily_state_metrics_fraud_rate__1__0  [PASS in 0.04s]
12:01:58  12 of 57 PASS dbt_expectations_expect_column_values_to_be_between_mart_fraud_by_state_fraud_rate__100__0  [PASS in 0.03s]
12:01:58  13 of 57 START test dbt_expectations_expect_column_values_to_be_between_mart_hourly_fraud_pattern_fraud_rate__100__0  [RUN]
12:01:58  14 of 57 START test dbt_expectations_expect_column_values_to_be_between_mart_merchant_analytics_fraud_rate__100__0  [RUN]
12:01:58  15 of 57 START test dbt_expectations_expect_column_values_to_be_between_stg_transactions_amount__0__True  [RUN]
12:01:58  16 of 57 START test dbt_utils_unique_combination_of_columns_mart_daily_state_metrics_transaction_date__us_state  [RUN]
12:01:58  14 of 57 PASS dbt_expectations_expect_column_values_to_be_between_mart_merchant_analytics_fraud_rate__100__0  [PASS in 0.04s]
12:01:58  13 of 57 PASS dbt_expectations_expect_column_values_to_be_between_mart_hourly_fraud_pattern_fraud_rate__100__0  [PASS in 0.04s]
12:01:58  17 of 57 START test dbt_utils_unique_combination_of_columns_mart_hourly_fraud_pattern_day_name__hour_of_day  [RUN]
12:01:58  15 of 57 PASS dbt_expectations_expect_column_values_to_be_between_stg_transactions_amount__0__True  [PASS in 0.04s]
12:01:58  18 of 57 START test dbt_utils_unique_combination_of_columns_stg_transactions_transaction_id  [RUN]
12:01:58  16 of 57 PASS dbt_utils_unique_combination_of_columns_mart_daily_state_metrics_transaction_date__us_state  [PASS in 0.04s]
12:01:58  19 of 57 START test not_null_mart_customer_risk_profile_customer_id ............ [RUN]
12:01:58  20 of 57 START test not_null_mart_daily_state_metrics_transaction_count ........ [RUN]
12:01:58  20 of 57 PASS not_null_mart_daily_state_metrics_transaction_count .............. [PASS in 0.04s]
12:01:58  21 of 57 START test not_null_mart_daily_state_metrics_transaction_date ......... [RUN]
12:01:58  19 of 57 PASS not_null_mart_customer_risk_profile_customer_id .................. [PASS in 0.07s]
12:01:58  17 of 57 PASS dbt_utils_unique_combination_of_columns_mart_hourly_fraud_pattern_day_name__hour_of_day  [PASS in 0.08s]
12:01:59  22 of 57 START test not_null_mart_daily_state_metrics_us_state ................. [RUN]
12:01:59  23 of 57 START test not_null_mart_fraud_by_category_cat_id ..................... [RUN]
12:01:59  21 of 57 PASS not_null_mart_daily_state_metrics_transaction_date ............... [PASS in 0.16s]
12:01:59  18 of 57 PASS dbt_utils_unique_combination_of_columns_stg_transactions_transaction_id  [PASS in 0.22s]
12:01:59  24 of 57 START test not_null_mart_fraud_by_state_unique_customers .............. [RUN]
12:01:59  23 of 57 PASS not_null_mart_fraud_by_category_cat_id ........................... [PASS in 0.06s]
12:01:59  25 of 57 START test not_null_mart_fraud_by_state_us_state ...................... [RUN]
12:01:59  26 of 57 START test not_null_mart_hourly_fraud_pattern_day_name ................ [RUN]
12:01:59  22 of 57 PASS not_null_mart_daily_state_metrics_us_state ....................... [PASS in 0.14s]
12:01:59  27 of 57 START test not_null_mart_hourly_fraud_pattern_hour_of_day ............. [RUN]
12:01:59  24 of 57 PASS not_null_mart_fraud_by_state_unique_customers .................... [PASS in 0.04s]
12:01:59  26 of 57 PASS not_null_mart_hourly_fraud_pattern_day_name ...................... [PASS in 0.03s]
12:01:59  25 of 57 PASS not_null_mart_fraud_by_state_us_state ............................ [PASS in 0.04s]
12:01:59  27 of 57 PASS not_null_mart_hourly_fraud_pattern_hour_of_day ................... [PASS in 0.02s]
12:01:59  28 of 57 START test not_null_mart_merchant_analytics_merchant_id ............... [RUN]
12:01:59  29 of 57 START test not_null_states_state_code ................................. [RUN]
12:01:59  30 of 57 START test not_null_states_state_name ................................. [RUN]
12:01:59  31 of 57 START test not_null_stg_transactions_amount ........................... [RUN]
12:01:59  28 of 57 PASS not_null_mart_merchant_analytics_merchant_id ..................... [PASS in 0.07s]
12:01:59  29 of 57 PASS not_null_states_state_code ....................................... [PASS in 0.07s]
12:01:59  30 of 57 PASS not_null_states_state_name ....................................... [PASS in 0.07s]
12:01:59  32 of 57 START test not_null_stg_transactions_amount_bucket .................... [RUN]
12:01:59  33 of 57 START test not_null_stg_transactions_cat_id ........................... [RUN]
12:01:59  34 of 57 START test not_null_stg_transactions_customer_id ...................... [RUN]
12:01:59  31 of 57 PASS not_null_stg_transactions_amount ................................. [PASS in 0.06s]
12:01:59  35 of 57 START test not_null_stg_transactions_is_fraud ......................... [RUN]
12:01:59  33 of 57 PASS not_null_stg_transactions_cat_id ................................. [PASS in 0.04s]
12:01:59  35 of 57 PASS not_null_stg_transactions_is_fraud ............................... [PASS in 0.04s]
12:01:59  36 of 57 START test not_null_stg_transactions_merchant_id ...................... [RUN]
12:01:59  32 of 57 PASS not_null_stg_transactions_amount_bucket .......................... [PASS in 0.05s]
12:01:59  37 of 57 START test not_null_stg_transactions_transaction_date ................. [RUN]
12:01:59  38 of 57 START test not_null_stg_transactions_transaction_hour ................. [RUN]
12:01:59  34 of 57 PASS not_null_stg_transactions_customer_id ............................ [PASS in 0.09s]
12:01:59  39 of 57 START test not_null_stg_transactions_transaction_id ................... [RUN]
12:01:59  37 of 57 PASS not_null_stg_transactions_transaction_date ....................... [PASS in 0.05s]
12:01:59  36 of 57 PASS not_null_stg_transactions_merchant_id ............................ [PASS in 0.04s]
12:01:59  40 of 57 START test not_null_stg_transactions_transaction_ts ................... [RUN]
12:01:59  41 of 57 START test not_null_stg_transactions_us_state ......................... [RUN]
12:01:59  38 of 57 PASS not_null_stg_transactions_transaction_hour ....................... [PASS in 0.07s]
12:01:59  42 of 57 START test relationships_stg_transactions_us_state__state_code__ref_states_  [RUN]
12:01:59  40 of 57 PASS not_null_stg_transactions_transaction_ts ......................... [PASS in 0.12s]
12:01:59  43 of 57 START test source_accepted_values_transactions_db_transactions_target__0__1  [RUN]
12:01:59  39 of 57 PASS not_null_stg_transactions_transaction_id ......................... [PASS in 0.14s]
12:01:59  44 of 57 START test source_accepted_values_transactions_db_transactions_us_state__AL__AK__AZ__AR__CA__CO__CT__DC__DE__FL__GA__HI__ID__IL__IN__IA__KS__KY__LA__ME__MD__MA__MI__MN__MS__MO__MT__NE__NV__NH__NJ__NM__NY__NC__ND__OH__OK__OR__PA__RI__SC__SD__TN__TX__UT__VT__VA__WA__WV__WI__WY  [RUN]
12:01:59  41 of 57 PASS not_null_stg_transactions_us_state ............................... [PASS in 0.14s]
12:01:59  45 of 57 START test source_not_null_transactions_db_transactions_amount ........ [RUN]
12:01:59  42 of 57 PASS relationships_stg_transactions_us_state__state_code__ref_states_ . [PASS in 0.11s]
12:01:59  46 of 57 START test source_not_null_transactions_db_transactions_cat_id ........ [RUN]
12:01:59  45 of 57 PASS source_not_null_transactions_db_transactions_amount .............. [PASS in 0.03s]
12:01:59  43 of 57 PASS source_accepted_values_transactions_db_transactions_target__0__1 . [PASS in 0.06s]
12:01:59  46 of 57 PASS source_not_null_transactions_db_transactions_cat_id .............. [PASS in 0.02s]
12:01:59  47 of 57 START test source_not_null_transactions_db_transactions_merch ......... [RUN]
12:01:59  48 of 57 START test source_not_null_transactions_db_transactions_target ........ [RUN]
12:01:59  49 of 57 START test source_not_null_transactions_db_transactions_transaction_time  [RUN]
12:01:59  44 of 57 PASS source_accepted_values_transactions_db_transactions_us_state__AL__AK__AZ__AR__CA__CO__CT__DC__DE__FL__GA__HI__ID__IL__IN__IA__KS__KY__LA__ME__MD__MA__MI__MN__MS__MO__MT__NE__NV__NH__NJ__NM__NY__NC__ND__OH__OK__OR__PA__RI__SC__SD__TN__TX__UT__VT__VA__WA__WV__WI__WY  [PASS in 0.07s]
12:01:59  50 of 57 START test source_not_null_transactions_db_transactions_us_state ...... [RUN]
12:01:59  48 of 57 PASS source_not_null_transactions_db_transactions_target .............. [PASS in 0.04s]
12:01:59  49 of 57 PASS source_not_null_transactions_db_transactions_transaction_time .... [PASS in 0.04s]
12:01:59  47 of 57 PASS source_not_null_transactions_db_transactions_merch ............... [PASS in 0.04s]
12:01:59  50 of 57 PASS source_not_null_transactions_db_transactions_us_state ............ [PASS in 0.02s]
12:01:59  51 of 57 START test unique_mart_customer_risk_profile_customer_id .............. [RUN]
12:01:59  52 of 57 START test unique_mart_fraud_by_category_cat_id ....................... [RUN]
12:01:59  53 of 57 START test unique_mart_fraud_by_state_us_state ........................ [RUN]
12:01:59  54 of 57 START test unique_mart_merchant_analytics_merchant_id ................. [RUN]
12:01:59  51 of 57 PASS unique_mart_customer_risk_profile_customer_id .................... [PASS in 0.04s]
12:01:59  53 of 57 PASS unique_mart_fraud_by_state_us_state .............................. [PASS in 0.04s]
12:01:59  54 of 57 PASS unique_mart_merchant_analytics_merchant_id ....................... [PASS in 0.03s]
12:01:59  52 of 57 PASS unique_mart_fraud_by_category_cat_id ............................. [PASS in 0.04s]
12:01:59  55 of 57 START test unique_states_state_code ................................... [RUN]
12:01:59  56 of 57 START test unique_stg_transactions_transaction_id ..................... [RUN]
12:01:59  57 of 57 START unit_test stg_transactions::stg_transactions_unit ............... [RUN]
12:01:59  55 of 57 PASS unique_states_state_code ......................................... [PASS in 0.06s]
12:01:59  57 of 57 PASS stg_transactions::stg_transactions_unit .......................... [PASS in 0.16s]
12:01:59  56 of 57 PASS unique_stg_transactions_transaction_id ........................... [PASS in 0.22s]
12:01:59  
12:01:59  Finished running 56 data tests, 1 unit test in 0 hours 0 minutes and 1.24 seconds (1.24s).
12:01:59  
12:01:59  Completed successfully
12:01:59  
12:01:59  Done. PASS=57 WARN=0 ERROR=0 SKIP=0 TOTAL=57
```
