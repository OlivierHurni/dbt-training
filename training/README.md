# Training notebooks

Follow the notebooks in [notebooks/](notebooks/) in order (open them in VS Code with the `.venv` kernel, on Windows or macOS).
Each one explains a dbt concept, then you build a piece of the project in [project/](project/).
Setup instructions: [../SETUP.md](../SETUP.md).

**Shared data, personal results.** The bronze data is loaded once by the trainer and shared by the whole class (read-only).
Everything you build goes into *your* schema, `silver.<schema>` and `gold.<schema>`, where `<schema>` is the `schema` of your dbt profile
(your name, e.g. `olivier_hurni`). When a notebook needs new data, the trainer loads a new batch for everybody and announces it.

| # | Notebook | You build |
|---|---|---|
| 01 | Setup | virtual environment, dbt installed, profile, working connection, personal schemas |
| 02 | Sources and first models | `_sources.yml`, silver `stg_*` models |
| 03 | Seeds | `countries` seed, `dim_customer` |
| 04 | Data tests | generic and singular tests, cleaned orders and lines |
| 05 | Gold layer, Jinja and macros | dimensions, `fct_sales`, marts, `net_amount` macro |
| 06 | Incremental models | `fct_sales` loaded incrementally (needs a new batch from the trainer) |
| 07 | Snapshots | product and sales person history (needs a new batch from the trainer) |
| 08 | Documentation | descriptions, doc blocks, lineage |
| 09 | Wrap-up | selectors, tags, Databricks job (needs a new batch from the trainer) |

**dbt commands.** Every `dbt("...")` cell prints the equivalent terminal command first. You can run any of them in a terminal instead:
`cd training/project`, activate the virtual environment, then type the command shown.

**Stuck?** Every module has a checkpoint in [checkpoints/](checkpoints/) with the finished files.
The last cell of each notebook restores it: `restore_checkpoint(4)` gives you the project as it is at the end of notebook 04.
The complete reference project is in [../src/](../src/). Trainers: see [../TRAINER.md](../TRAINER.md).
