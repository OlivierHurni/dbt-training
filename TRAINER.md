# Trainer guide

The bronze data is generated **once by the trainer** and **shared read-only by all students**. Every student builds silver and gold
in a **personal schema** (`silver.<name>`, `gold.<name>`), so nobody overwrites anybody else. This page is what you prepare before the course
and what you do during it.

> Note: the students connect to *your* workspace with their own account and token. If your workspace is a Databricks Free Edition workspace,
> check beforehand that you can give other people access to it; otherwise use a workspace that allows multiple users.

## Before the course

### 1. Catalogs and bronze data

1. Create the catalogs `bronze`, `silver` and `gold` (*Catalog* > *+* > *Create a catalog*).
2. Import the generator notebook `src/notebooks/generate_bronze_data.py` (workspace > *Import*), attach **Serverless** compute,
   set the widgets `catalog = bronze`, `schema = sports_shop`, **`mode = init`** and run it (about 2 minutes).
   You get batch 0: about 60 000 orders over 2024-2025, with about 1% deliberately dirty rows.

Same thing from the command line (single line, works in bash and PowerShell):

```bash
databricks workspace import /Users/<you>/dbt-training/generate_bronze_data --file src/notebooks/generate_bronze_data.py --format SOURCE --language PYTHON --overwrite
```

Run `mode = init` again at any time to reset bronze to batch 0 (it wipes the tables).

### 2. Permissions

Create a group (here `students`) and grant, in a SQL editor:

```sql
-- bronze: read only
GRANT USE CATALOG ON CATALOG bronze TO `students`;
GRANT USE SCHEMA, SELECT ON SCHEMA bronze.sports_shop TO `students`;

-- silver and gold: students create their own schema and tables
GRANT USE CATALOG, CREATE SCHEMA ON CATALOG silver TO `students`;
GRANT USE CATALOG, CREATE SCHEMA ON CATALOG gold TO `students`;
```

A student who creates a schema owns it, so they can create tables in it without further grants.
Also give the group **CAN USE** on the SQL warehouse.

### 3. Warehouse size

`dbt build` runs several queries in parallel (4 threads in the default profile). For a class use a larger warehouse
(or a serverless one that scales) so that queries do not queue, and check that it auto-starts.

### 4. Test the whole path yourself

Follow [SETUP.md](SETUP.md) and notebooks 01-05 with your own personal schema on a Windows and a macOS machine if you can.

## Schedule of the bronze batches

The notebooks 06, 07 and 09 need **new data in bronze**. Since bronze is shared, *you* load the batch and announce it;
the students' notebooks wait (a cell to re-run tells them whether the new batch is there).

| When | Notebook | Trainer action | Students must have done |
|---|---|---|---|
| Start | 01 | `mode = init` (batch 0), see above | nothing |
| End of section 3 of notebook 06 | 06 | run the generator with **`mode = append`** (batch 1) | converted `fct_sales` to incremental and run it once with `--full-refresh` |
| End of section 3 of notebook 07 | 07 | run **`mode = append`** (batch 2) | taken the **baseline snapshot** (`dbt snapshot`); the history starts there |
| Section 2 of notebook 09 | 09 | run **`mode = append`** (batch 3) | notebooks 01-08 |

A batch adds one week of orders (about 600), 40 customers, price changes (~5% of products), store moves (~5% of sales persons) and
turns ~2% of the previous batch's orders into `returned`. Each `append` takes about a minute.

**Do not load the batch before everybody is ready**: a student who has not taken the baseline snapshot yet would see no history
(they would have to wait for the next batch). If someone is late, load the next batch anyway and let them catch up with
`restore_checkpoint(N)` in their notebook.

## During the course

* Students who are stuck can run `restore_checkpoint(N)` (last cell of each notebook) to get the finished files up to module N.
* The complete reference solution is the project in `src/`. To rebuild it (fixed schema `sports_shop`): from the repository root,
  with a profile pointing at your workspace, run `dbt deps`, `dbt seed`, `dbt snapshot`, `dbt build`.
* Typical problems are listed at the end of [SETUP.md](SETUP.md) step 5 (token, virtual environment, permissions).

## After the course

Drop the student schemas (they are created in `silver` and `gold` under the student names):

```sql
DROP SCHEMA IF EXISTS silver.olivier_hurni CASCADE;
DROP SCHEMA IF EXISTS gold.olivier_hurni CASCADE;
-- failing-test tables of the store_failures exercise
DROP SCHEMA IF EXISTS silver.olivier_hurni_dbt_test__audit CASCADE;
```

To list them: `SHOW SCHEMAS IN silver;` and `SHOW SCHEMAS IN gold;`. Revoke the tokens of the students in the workspace admin settings.
