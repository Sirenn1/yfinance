# yfinance Lakehouse Roadmap: local pipeline to cloud lakehouse

Goal: turn the current local pandas pipeline into a Medallion lakehouse that backs every claim on the CV, while spending **less than $1 in total**.

Estimated effort: 30-35 hours, about 3-4 weekends.

## Target architecture

```
yfinance --> Airflow (local Docker) --> S3 bronze/ (Parquet, run_date=YYYY-MM-DD)
                                              |
                              Glue Data Catalog + Athena (partition projection)
                                              |
                      dbt-athena --> silver (Iceberg, MERGE) --> gold (Iceberg)

Terraform provisions: S3, Glue database, Athena workgroup, IAM policy, budget alert
```

## Where the repo stands today

| Area | Current state |
|---|---|
| Bronze | `bronze/ingestion.py` downloads ~780 `.JK` tickers in batches of 300 and saves yfinance's **wide multi-index** frame to `bronze/run_date=.../batch_N.parquet`. |
| Silver | `silver/silver.py` melts and pivots to one row per (date, ticker), writes `silver.csv`. Reads only today's `run_date`, so no history accumulates. |
| Gold | `gold/gold.py` computes `daily_return_pct`, `ma_20` (`min_periods=1`), rounds to 2 decimals, writes `daily_metrics.csv` and `latest_snapshot.csv` (`above_ma_20`). |
| Missing | Tests, Docker, Terraform, CI, retries, `.env.example`, cloud storage. `.gitignore` lacks `.env`, `*.tfstate`, `.terraform/`, `target/`, `logs/`. |
| History | Some vague commits ("Commit", "Initial Commit"). Do not rewrite history. Use good messages from now on. |

## Cost rules (read before touching AWS)

The budget is under $1. Follow these rules for the whole project.

1. **Check your free tier or credits first.** Billing console, Free Tier / Credits page. Free-tier rules for new accounts have changed, so confirm what applies to yours.
2. **A budget alert only emails you. It does not stop spending.** Set two alerts: $0.50 actual and $1 forecast (Step 4).
3. **Never create a Glue crawler.** Crawlers bill by DPU-hour and would break the budget. Use Athena partition projection instead (Step 5).
4. **Keep the 1 GB per-query scan cutoff** on the Athena workgroup. Better, lower it to 100 MB for this dataset.
5. **Always filter on the partition column** (`run_date`) in Athena queries. Never `SELECT *` on the whole table.
6. **Do not loop-test against AWS.** Develop and test with local files and mocks. Hit Athena only for real runs.
7. **Do the full historical backfill once**, not repeatedly.
8. **Run `terraform destroy` when you are done** or when you pause the project for a long time.
9. **Check Cost Explorer after the first real run, then weekly.** Costs show up with up to ~24 hours delay.
10. Athena pricing is $5 per TB scanned, with a 10 MB minimum per query. DDL is free. Estimate: ~1M rows is small, so a few dozen queries a day should stay far below $1, but measure it. Do not assume it.

Stop and investigate if Cost Explorer shows more than $0.30 in the first week.

---

## Step 1: Plan and set up (1 h)

- [ ] Create an AWS account. Create a **non-root** IAM user (or IAM Identity Center/SSO user). Enable MFA on root and on the user.
- [ ] Install the AWS CLI. Configure a named profile, e.g. `yfin`:
  ```powershell
  aws configure sso --profile yfin      # or: aws configure --profile yfin
  aws sts get-caller-identity --profile yfin
  ```
- [ ] Pick **one region** and stay in it. Suggested: `ap-southeast-1` (Singapore).
- [ ] Update `.gitignore` **before anything else**:
  ```
  .env
  *.tfstate
  *.tfstate.*
  .terraform/
  target/
  logs/
  dbt_packages/
  ```
- [ ] Create `.env.example` (no real values):
  ```
  AWS_PROFILE=yfin
  AWS_REGION=ap-southeast-1
  S3_BUCKET=
  ATHENA_WORKGROUP=
  GLUE_DATABASE=
  ```
- [ ] If the TMDB repo really leaked a key, **rotate it** in that service's console. Deleting the file does not remove it from git history.
- [ ] Tag the current state so history shows local to cloud:
  ```powershell
  git tag v0-local
  git push origin v0-local
  ```
- [ ] Work on branches (`feat/restructure`, `feat/bronze-long`, ...) and merge via PRs.

**Done when:** `aws sts get-caller-identity --profile yfin` works, `.gitignore` is committed, `v0-local` exists.

## Step 2: Restructure the repo (1 h)

```
extract/        <- moved from bronze/ingestion.py, plus config/ (ticker CSVs)
infra/          <- Terraform
dbt/            <- models, tests, macros
dags/           <- Airflow DAGs
tests/          <- pytest
docker/         <- Dockerfile, docker-compose.yml
reference/      <- old silver.py and gold.py (pandas), kept for reconciliation in Step 8
Makefile
README.md
```

- [ ] Use `git mv` so history follows the files.
- [ ] Keep the pandas Silver/Gold code in `reference/`. It becomes the ground truth you test dbt against.
- [ ] Generate and keep a reference CSV from the old pipeline for a fixed date range (see Step 8).

Commit example: `refactor: split repo into extract, dbt, dags, infra layout`

## Step 3: Make Bronze queryable (2-3 h)

Athena cannot query yfinance's wide multi-index frame. Reshape before writing.

- [ ] Target schema (long format): `ticker, trade_date, open, high, low, close, adj_close, volume`.
- [ ] Call `yf.download(..., auto_adjust=False)`. In recent yfinance versions the default is `auto_adjust=True`, which removes the `Adj Close` column. Check your installed version.
- [ ] Set an explicit `start=`. The default period is about 1 month. For the **initial backfill** use `period="max"` or an explicit start. For **daily runs** pull the last 7 days (lookback, see Step 6).
- [ ] Reshape with `stack` or `melt`. Drop rows where all prices are null.
- [ ] Keep the Hive layout: `bronze/prices/run_date=YYYY-MM-DD/part-N.parquet`.
- [ ] Add **retries with exponential backoff** and keep the batch size (300 or lower). Yahoo rate-limits aggressively.
- [ ] Fix the end-of-run message so it reports the tickers actually returned, not `len(codes)`.
- [ ] Check where `time.sleep(2)` sits. It should run between batches inside the loop.
- [ ] Return a result (rows written, tickers returned, failed tickers). Airflow uses it in Step 9.
- [ ] pytest with a mocked `yf.download`:
  - empty response
  - one bad ticker among valid ones
  - a market holiday (no rows)
  - schema and dtypes of the output
  - retry behavior on an exception

**Done when:** `pytest` passes locally with no network and no AWS.

## Step 4: Terraform (4 h)

Create in `infra/` (`main.tf`, `variables.tf`, `outputs.tf`, `providers.tf`). Use local state, and keep `*.tfstate` out of git.

- [ ] **S3 bucket**: public access block on all four settings, SSE-S3 encryption, lifecycle rule that expires `athena-results/` objects after 7 days.
- [ ] **Glue database**: `aws_glue_catalog_database`.
- [ ] **Athena workgroup**: engine `Athena engine version 3`, `enforce_workgroup_configuration = true`, `bytes_scanned_cutoff_per_query` (1 GB max, 100 MB recommended), results location `s3://<bucket>/athena-results/`, encrypted results.
- [ ] **IAM policy** scoped to this bucket, this Glue database and this workgroup only. No `*` resources except where AWS requires it.
- [ ] **Budget**: `aws_budgets_budget` of $1/month with notifications at $0.50 actual and 100% forecast, sent to your email.
- [ ] **Do not** create Silver/Gold tables in Terraform. dbt-athena can delete S3 files if a model shares a name or location with an existing table. Let dbt own those tables.

```powershell
cd infra
terraform fmt
terraform init
terraform validate
terraform plan -out tfplan      # read it fully
terraform apply tfplan          # ask yourself: have I read the plan?
terraform destroy               # test once to prove clean teardown
```

**Done when:** `apply` builds everything and `destroy` removes it cleanly. Re-apply after the destroy test.

## Step 5: Bronze to S3 without a crawler (3 h)

- [ ] Upload with `boto3` to `s3://<bucket>/bronze/prices/run_date=YYYY-MM-DD/`. Use the `yfin` profile through `AWS_PROFILE`. Never hard-code keys.
- [ ] Register the table once with **partition projection** on `run_date` (run through the Athena workgroup):
  ```sql
  CREATE EXTERNAL TABLE bronze_prices (
    ticker     string,
    trade_date date,
    open       double,
    high       double,
    low        double,
    close      double,
    adj_close  double,
    volume     bigint
  )
  PARTITIONED BY (run_date string)
  STORED AS PARQUET
  LOCATION 's3://<bucket>/bronze/prices/'
  TBLPROPERTIES (
    'projection.enabled' = 'true',
    'projection.run_date.type' = 'date',
    'projection.run_date.range' = '2026-01-01,NOW',
    'projection.run_date.format' = 'yyyy-MM-dd',
    'storage.location.template' = 's3://<bucket>/bronze/prices/run_date=${run_date}/'
  );
  ```
- [ ] Keep this DDL in the repo (`infra/sql/bronze_prices.sql`) and run it from a small script or Makefile target.
- [ ] Verify:
  ```sql
  SELECT count(*), count(DISTINCT ticker)
  FROM bronze_prices
  WHERE run_date = '2026-xx-xx';
  ```
- [ ] Put the real ticker count in the README. Do not assume 780.

**Done when:** the query returns the expected ticker count.

## Step 6: Silver in dbt on Iceberg (5 h)

- [ ] Install `dbt-athena` in a virtualenv (check the dbt docs for the current package name before installing).
- [ ] `dbt/profiles.yml` (no secrets, uses the AWS profile):
  ```yaml
  yfinance:
    target: dev
    outputs:
      dev:
        type: athena
        s3_staging_dir: s3://<bucket>/athena-results/
        s3_data_dir: s3://<bucket>/dbt/
        region_name: ap-southeast-1
        database: awsdatacatalog
        schema: <glue_database>
        work_group: <workgroup>
        aws_profile_name: yfin
  ```
- [ ] Declare the Bronze table as a dbt `source` with a `freshness` check.
- [ ] `models/silver/silver_prices.sql`:
  ```sql
  {{ config(
      materialized='incremental',
      table_type='iceberg',
      incremental_strategy='merge',
      unique_key=['ticker','trade_date'],
      format='parquet',
      partitioned_by=['month(trade_date)']
  ) }}

  with src as (
    select
      ticker, trade_date, open, high, low, close, adj_close, volume, run_date,
      row_number() over (
        partition by ticker, trade_date
        order by run_date desc
      ) as rn
    from {{ source('bronze', 'prices') }}
    {% if is_incremental() %}
    where run_date >= cast(date_add('day', -7, current_date) as varchar)
    {% endif %}
  )
  select ticker, trade_date, open, high, low, close, adj_close, volume
  from src
  where rn = 1
  ```
- [ ] Design points:
  - **Deduplicate** with `row_number()`. A MERGE fails if several source rows hit one target row.
  - **7-day lookback.** Yahoo restates history after splits and dividends, so a new-days-only load goes stale.
  - **Filter on `run_date`** (the partition) to keep scans small. `run_date` is a string, so compare it as a string in `YYYY-MM-DD`.
  - MERGE needs Athena engine v3.
- [ ] First run: backfill with `dbt run --full-refresh` once. Later runs are incremental.

**Done when:** `dbt run -s silver_prices` works twice in a row with identical results.

## Step 7: Gold models (3 h)

Port the pandas logic to SQL window functions. Materialize as Iceberg tables (full rebuilds are cheaper than complexity at this size).

- [ ] `gold_daily_metrics`:
  ```sql
  select
    trade_date, ticker, close, volume,
    round((close / lag(close) over w - 1) * 100, 2) as daily_return_pct,
    round(avg(close) over (
      partition by ticker order by trade_date
      rows between 19 preceding and current row
    ), 2) as ma_20
  from {{ ref('silver_prices') }}
  where close is not null
  window w as (partition by ticker order by trade_date)
  ```
  Match the pandas behavior: rows with null `close` are dropped first, `ma_20` uses a partial window at the start (`min_periods=1`), values rounded to 2 decimals. If the `window` clause is not supported in your engine version, inline the window.
- [ ] `gold_latest_snapshot`: last row per ticker, `close > ma_20 as above_ma_20`, sorted by `daily_return_pct` descending.

## Step 8: Tests and idempotency proof (4 h)

- [ ] dbt tests (install `dbt_utils`):
  - `not_null` on `ticker`, `trade_date`, `close`
  - `dbt_utils.unique_combination_of_columns` on (`ticker`, `trade_date`)
  - `high >= low` and `close between low and high` (custom or `dbt_utils.expression_is_true`)
  - `volume >= 0`
  - source freshness on Bronze
- [ ] **Idempotency test** (`tests/test_idempotency.py`): run the pipeline twice for the same date, then compare before/after: `count(*)`, `count(distinct (ticker, trade_date))`, `sum(close)`, `sum(volume)`. Assert they are identical. Keep this in the repo. It is the evidence for the "idempotent MERGE" claim.
- [ ] **Reconciliation**: compare dbt Gold with the pandas reference output for the same date range. Join on (`ticker`, `date`), compare `daily_return_pct` and `ma_20` within 0.01. Report the match rate in the README.
  - Expect small differences if yfinance restated prices between the two pulls. Use the same Bronze data for both when you test.

## Step 9: Airflow in Docker (5 h)

- [ ] `docker/docker-compose.yml`: `apache/airflow` with `LocalExecutor` and a Postgres container.
- [ ] Mount `~/.aws` read-only and set `AWS_PROFILE`. Do not copy keys into the image.
- [ ] Dockerfile: install dbt **in its own virtualenv** (e.g. `/opt/dbt-venv`) and call it from `BashOperator`. Airflow and dbt have conflicting dependencies, so do not install both in one environment.
- [ ] DAG `dags/yfinance_lakehouse.py`:
  ```
  extract_to_s3 -> validate_bronze -> dbt_build (silver, tests, gold, tests)
  ```
- [ ] Schedule weekdays after the IDX close: `30 17 * * 1-5` in `Asia/Jakarta`, `catchup=False`.
- [ ] `retries=2` with a retry delay. Add a **no-data path** for holidays: if the extract returns zero rows, skip the downstream tasks (`ShortCircuitOperator`) instead of failing.
- [ ] `validate_bronze`: check row count and ticker count are above a minimum before dbt runs.
- [ ] Keep the machine awake at 17:30 on run days, or trigger manually.

**Done when:** two scheduled (or manually triggered) runs succeed. Take a DAG screenshot.

## Step 10: Maintenance, CI and cost (4 h)

- [ ] Weekly Iceberg maintenance, because daily MERGEs create small files:
  ```sql
  OPTIMIZE <schema>.silver_prices REWRITE DATA USING BIN_PACK;
  VACUUM <schema>.silver_prices;
  ```
  Add it as a separate weekly DAG. Check the Athena docs for exact syntax on your engine version.
- [ ] GitHub Actions (`.github/workflows/ci.yml`): `terraform fmt -check` and `terraform validate`, `ruff check`, `pytest`, `dbt parse`. CI must not need AWS credentials. Use `terraform init -backend=false`.
- [ ] Run the pipeline for 2-4 weeks, then read the real cost in Cost Explorer. Take a screenshot.

## Step 11: Document (3 h)

README contents:

- [ ] Architecture diagram
- [ ] `make` quick-start (`make setup`, `make test`, `make up`, `make dbt-build`, `make destroy`)
- [ ] dbt lineage screenshot (`dbt docs generate`)
- [ ] Airflow DAG screenshot
- [ ] Cost Explorer screenshot and the measured figure
- [ ] Reconciliation match rate and idempotency result
- [ ] "Design decisions": 7-day lookback window, partition projection, why no crawler, why dbt owns Silver/Gold tables
- [ ] GitHub repo description and topics (`data-engineering`, `dbt`, `airflow`, `athena`, `iceberg`, `terraform`, `yfinance`)
- [ ] Meaningful commit messages (e.g. `feat(bronze): reshape yfinance output to long format`)

## Evidence for each CV claim

| CV claim | Evidence in the repo |
|---|---|
| Hybrid Medallion Lakehouse, ~780 tickers | Bronze Parquet on S3, Silver/Gold Iceberg, measured ticker count in README |
| Local Docker Airflow | `docker-compose.yml`, DAG file, run screenshot |
| S3, Glue Data Catalog, Athena | `infra/` Terraform |
| dbt on Iceberg with idempotent MERGE | dbt models and the run-twice test |
| Automated schema tests | `dbt test` in CI and in the DAG |
| Terraform | `infra/` |
| Cost figure | Cost Explorer screenshot. Write the measured number, e.g. "under $1/month" |

## Order and checkpoints

1. Steps 1-3 (free, local). Merge, then review.
2. Step 4. Read the `terraform plan` before the first `apply`.
3. Steps 5-8. First real AWS spend happens here. Check Cost Explorer the next day.
4. Steps 9-11. Run 2-4 weeks, then measure and document.

## Assumptions to verify

- yfinance `auto_adjust` default and the `Adj Close` column depend on your installed version.
- Athena `OPTIMIZE`/`VACUUM` syntax, the `dbt-athena` package name, and free-tier rules were not checked against current docs. Verify them when you reach those steps.
- Whether the TMDB key leak is real, and your AWS account's free-tier status, are unknown.
