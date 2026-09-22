# dbt training – AlpSport (Databricks)

Hands-on introduction to dbt with a synthetic Swiss sports retailer ("AlpSport": 11 stores + online shop).
Data flows **bronze → silver → gold**, one Unity Catalog catalog per layer.

| Layer | Catalog | Built by | Content |
|---|---|---|---|
| Bronze | `bronze.sports_shop` | notebook [generate_bronze_data.py](src/notebooks/generate_bronze_data.py) | raw source-system tables, deliberately a bit dirty |
| Silver | `silver.sports_shop` | dbt | cleaned, typed, deduplicated staging models (`stg_*`), the `countries` seed, the snapshots |
| Gold | `gold.sports_shop` | dbt | star schema (`dim_*`, `fct_sales`) and KPI marts (`mart_*`) |

Setting up dbt on your machine: see [SETUP.md](SETUP.md).
The hands-on notebooks for students are in [training/](training/README.md): a starter project to build up module by module,
plus a checkpoint with the finished files for each module. The complete reference solution is the project in `src/`.
The bronze data is shared by all students and loaded by the trainer, see [TRAINER.md](TRAINER.md); students build silver and gold in a personal schema.

## dbt project layout

```
dbt_project.yml, packages.yml        project config, dbt_utils
src/
  seeds/countries.csv                31 countries (continent, region, currency, EU) -> silver
  snapshots/snapshots.yml            snap_products, snap_sales_persons (SCD2, timestamp strategy) -> silver
  models/silver/                     _sources.yml (bronze + tests, severity warn), stg_* models, _silver.yml
  models/gold/                       dim_date, dim_product, dim_store, dim_sales_person, dim_customer,
                                     dim_product_history, dim_sales_person_history,
                                     fct_sales (incremental merge),
                                     mart_revenue_by_category_month, mart_store_performance,
                                     mart_sales_person_ranking, mart_revenue_by_country
  models/docs.md                     overview page and doc blocks
  macros/                            generate_schema_name (catalog/schema layout), net_amount
  tests/                             singular tests
  notebooks/generate_bronze_data.py  bronze generator
```

Order to run after a new bronze batch: `dbt snapshot` then `dbt build` (also what the Databricks job does).

## Bronze data

Tables: `products`, `stores`, `sales_persons`, `customers`, `sales_orders`, `sales_order_lines`
(every row has `_batch_id` and `_ingested_at`).

The notebook has two modes:

* `init` – wipes and rebuilds everything. Batch 0 = 2 years of history (2024-01-01 → 2025-12-31), ~60k orders, ~110k lines.
* `append` – adds **one more batch** (the next 7 days, ~600 orders). Each batch also:
  * changes ~5% of product prices and moves ~5% of sales persons to another store (→ snapshots),
  * adds 40 customers and upgrades ~3% of loyalty tiers,
  * turns ~2% of the previous batch's orders into `returned` (→ incremental merge on `updated_at`).

Widgets: `catalog`, `schema`, `mode`, `n_orders_init`, `n_orders_batch`, `days_per_batch`, `dirty_rate`, `seed`.

Dirty rows (`dirty_rate`, default 1%) for the tests module: duplicate orders/lines, upper-case statuses
(`COMPLETED`), orphan `customer_id` (`C99999`) and `product_id` (`P9999`), negative quantities.
Legitimate NULLs: anonymous in-store customers (~15%) and online orders without a sales person.

Story in the data: ski peaks Nov–Feb, swimming Jun–Aug (July 2025 heatwave), Saturdays and Black Friday spike,
a handful of sales persons and best-selling products make most of the revenue.

## Course outline (≈ 1 day, 9 modules)

| # | Module | dbt concepts | AlpSport exercise |
|---|---|---|---|
| 1 | Setup and concepts | ELT vs ETL, `dbt-databricks`, `profiles.yml`, project layout, medallion architecture, `dbt debug` | Connect to the SQL warehouse (`dbt debug`), explore the project, run `dbt build` |
| 2 | Sources and first models | `source()`, `ref()`, materializations (view / table), `dbt run`, `--select`, DAG | Staging models `stg_products`, `stg_customers`, `stg_sales_orders` in **silver** (rename, cast, trim, lower-case status) |
| 3 | Seeds | `dbt seed`, seed properties, column types, joining a seed | `countries.csv`: enrich `dim_customer` with country name, continent, EU flag; `mart_revenue_by_country` |
| 4 | Data tests | generic tests (`unique`, `not_null`, `accepted_values`, `relationships`), singular tests, severity, `store_failures`, `dbt test` | Find the dirty rows; fix duplicates and orphans in silver (dedup with `row_number()`), keep `warn` for the rest |
| 5 | Gold layer and Jinja | dimensional modelling, `dim_*` / `fct_sales`, Jinja, macros, packages (`dbt_utils`) | `dim_*`, `dim_date` (`dbt_utils.date_spine`), `fct_sales` (line grain), macro `net_amount(qty, price, discount)` |
| 6 | Incremental models | `is_incremental()`, `unique_key`, `merge`, `incremental_strategy`, `--full-refresh` | `fct_sales` is incremental (merge on `order_line_id`, watermark `order_updated_at`); run notebook `append`, rerun dbt, watch returned orders update |
| 7 | Snapshots | SCD type 2, `timestamp` vs `check` strategy | `snap_products` (price history) and `snap_sales_persons` (store moves) feed `dim_*_history`; run `append` twice and query the history |
| 8 | Documentation | `schema.yml` descriptions, `doc` blocks, `dbt docs generate/serve`, lineage graph, exposures | Document the gold layer; explore the lineage from bronze to KPI mart |
| 9 | Wrap-up | tags and selectors, `dbt build`, running in a Databricks Job (bundle in this repo), CI, best practices | the four `mart_*` models; the Databricks Job in `resources/` runs `dbt deps, seed, snapshot, build` |

Suggested flow of the day: modules 1–3 morning, 4–6 midday, 7–9 afternoon. Between modules 6 and 7
participants run the notebook in `append` mode so that they see new data flowing through the whole pipeline.

## Running the notebook

```bash
# upload (already done once) and run as a serverless job
databricks workspace import /Users/<you>/dbt-training/generate_bronze_data \
  --file src/notebooks/generate_bronze_data.py --format SOURCE --language PYTHON --overwrite
```

Or import the file in the workspace UI and run it with the widgets. `mode=append` on an empty schema falls back to `init`.
