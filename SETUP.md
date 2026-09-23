# Setting up dbt on your machine

This guide gets you from zero to a working `dbt build` against Databricks. It works on **Windows** and **macOS**
(Linux is the same as macOS). Allow about 20 minutes.

Wherever the commands differ, you will see a **macOS / Linux** and a **Windows** variant. On Windows we use **PowerShell**
(search "PowerShell" in the Start menu); the commands are the same in the terminal of VS Code.

## What you need

* **Python 3.10 - 3.13** (3.14 is not supported by all dbt dependencies yet)
* **Git**
* **VS Code** (recommended) with the *Python* and *Jupyter* extensions
* From your trainer: the **Databricks workspace URL (host)**, the **SQL warehouse HTTP path**, and access to the catalogs
  `bronze`, `silver` and `gold`. The bronze data is loaded once by the trainer and **shared by everybody**.

### Installing the prerequisites

| | macOS | Windows |
|---|---|---|
| Python | `brew install python@3.13` or the installer from [python.org](https://www.python.org/downloads/) | Installer from [python.org](https://www.python.org/downloads/): **tick "Add python.exe to PATH"**. Or `winget install Python.Python.3.13` |
| Git | `brew install git` (or Xcode command line tools) | `winget install Git.Git` or [git-scm.com](https://git-scm.com/download/win) |
| VS Code | [code.visualstudio.com](https://code.visualstudio.com/) | `winget install Microsoft.VisualStudioCode` |

Check the versions (open a **new** terminal after installing):

| macOS / Linux | Windows |
|---|---|
| `python3 --version` | `py --version` (or `python --version`) |
| `git --version` | `git --version` |

## 1. Get the code

```bash
git clone https://github.com/OlivierHurni/dbt-training.git
cd dbt-training
git checkout student-start
```

`student-start` is the branch you work on: it starts empty and you fill it in notebook by notebook. `main` holds the
trainer's finished reference project (see step 5) — don't build your own work there, your personal schema is only
honored on `student-start`.

## 2. Create a virtual environment and install dbt

A virtual environment keeps dbt and its dependencies separate from the rest of your system.
Run these commands from the `dbt-training` folder.

**macOS / Linux**

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements-dev.txt
```

**Windows (PowerShell)**

```powershell
py -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements-dev.txt
```

> **Windows: "running scripts is disabled on this system"?** Run this once, then activate again:
> `Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned`.
> Or use the classic command prompt (`cmd`) and `.venv\Scripts\activate.bat`. In Git Bash: `source .venv/Scripts/activate`.

Your prompt now starts with `(.venv)`. Check it:

```bash
dbt --version
```

`requirements-dev.txt` pins `dbt-databricks>=1.12.0` (this project is developed and tested with dbt-core 1.12 and dbt-databricks 1.12).
`dbt --version` should list `dbt-core` 1.12 or later and the `databricks` plugin at 1.12 or later.

* Every time you open a new terminal, **activate the environment again** (`source .venv/bin/activate` on macOS,
  `.venv\Scripts\Activate.ps1` on Windows). Always `source` the script on macOS: `.venv/bin/activate` alone gives "permission denied".
* `deactivate` leaves the environment.
* Prefer [uv](https://docs.astral.sh/uv/)? `uv venv --python 3.13` then `uv pip install -r requirements-dev.txt`.

## 3. Collect the connection details from Databricks

You need four values.

| Value | Where to find it |
|---|---|
| **Host** | The workspace URL without `https://`, e.g. `dbc-xxxxxxxx-xxxx.cloud.databricks.com` (from your trainer) |
| **HTTP path** | *SQL Warehouses* > the warehouse > *Connection details* > **HTTP path**, e.g. `/sql/1.0/warehouses/c2a5ecbfd978e48c` (from your trainer) |
| **Access token (PAT)** | See below |
| **Your personal schema** | Your first and last name in lower case, separated by an underscore, e.g. `olivier_hurni` (letters, digits and `_` only) |

**Create a personal access token (PAT)**

1. In the workspace, click your user icon (top right) > **Settings**.
2. Open **Developer** > **Access tokens** > **Manage** > **Generate new token**.
3. Give it a comment (`dbt training`) and a lifetime (e.g. 30 days), then **Generate**.
4. **Copy the token now** (it starts with `dapi...`); it is shown only once.

Treat the token like a password: never commit it, never paste it in a chat or a screenshot.

> **Why a personal schema?** The bronze data is shared, but everything *you* build (silver and gold tables) goes into
> `silver.<your_schema>` and `gold.<your_schema>`, so nobody overwrites anybody else's work.

## 4. Initialize dbt (create your profile)

dbt reads the connection settings from a *profile* stored **outside** the repository:

| macOS / Linux | Windows |
|---|---|
| `~/.dbt/profiles.yml` | `%USERPROFILE%\.dbt\profiles.yml` (i.e. `C:\Users\<you>\.dbt\profiles.yml`) |

The project asks for a profile called `dbt-training` (see `profile:` in `dbt_project.yml`).

### Option A: `dbt init` (interactive)

Run it from the project folder. It uses [profile_template.yml](profile_template.yml) to ask only what is needed.

```bash
dbt init
```

Answer the prompts:

| Prompt | Answer |
|---|---|
| host | your host (default is pre-filled) |
| token | paste your PAT (input is hidden) |
| http_path | your warehouse HTTP path |
| catalog | `bronze` |
| schema | **your personal schema**, e.g. `olivier_hurni` |
| threads | `4` |

If asked whether to overwrite an existing `dbt-training` profile, answer `y`.

### Option B: write the profile by hand

Create or edit the file (create the `.dbt` folder if it does not exist) and add:

```yaml
dbt-training:
  target: dev
  outputs:
    dev:
      type: databricks
      schema: olivier_hurni            # YOUR personal schema
      host: dbc-cc21e849-ca2a.cloud.databricks.com
      http_path: /sql/1.0/warehouses/c2a5ecbfd978e48c
      token: <your personal access token, starts with dapi>
      threads: 16
      catalog: bronze
```

Open the file in an editor:

| macOS / Linux | Windows (PowerShell) |
|---|---|
| `mkdir -p ~/.dbt && code ~/.dbt/profiles.yml` | `mkdir $env:USERPROFILE\.dbt -Force; code $env:USERPROFILE\.dbt\profiles.yml` |

Replace `host` and `http_path` with the values of your trainer if they differ, put your own `schema`, and paste your token in place of the placeholder.
Never commit or share this file, and never paste the token in a chat or a ticket.

Optional: keep the token out of the file by writing `token: "{{ env_var('DBT_TOKEN') }}"` and setting the variable in each terminal:

| macOS / Linux | Windows (PowerShell) |
|---|---|
| `export DBT_TOKEN=dapi...` | `$env:DBT_TOKEN = "dapi..."` |

> **About the schema.** On the `student-start` branch every model, seed and snapshot you build is written to the catalog of its layer
> (`silver` or `gold`) and to the `schema` of this profile. On `main` (the trainer's reference project) the schema is fixed to
> `sports_shop` regardless of your profile, so don't run `main` with your own profile — stay on `student-start`.

## 5. Check the connection

Run this from the repository root (no `cd` needed, `dbt_project.yml` lives there):

```bash
dbt deps
dbt debug
```

Expect `Connection test: [OK connection ok]` and `All checks passed!`.
If the warehouse was stopped, the first call can take up to a minute while it starts.

| Symptom | Likely cause |
|---|---|
| `dbt: command not found` / `'dbt' is not recognized` | the virtual environment is not activated (step 2) |
| `Could not find profile named 'dbt-training'` | profile name mismatch or the file is not in the `.dbt` folder of your home directory |
| `401` / `Invalid access token` | token expired, or copied with a trailing space |
| `Warehouse ... is not running` / timeout | wait and retry, or ask your trainer to start the warehouse |
| `Catalog 'bronze' does not exist` / permission denied | wrong catalog name or you were not granted access: ask your trainer |
| `env_var 'DBT_TOKEN' not provided` | (only with the `env_var` option) the variable was not set in this terminal |
| `UnicodeDecodeError` / strange characters (Windows) | update to the latest version of this repository; if it persists set `PYTHONUTF8=1` |
| Very long file paths fail (Windows) | clone the repository close to the drive root, e.g. `C:\dev\dbt-training` |

## 6. Follow the training notebooks

The hands-on course is in [training/](training/README.md): nine Jupyter notebooks that you follow in order,
starting with `training/notebooks/01_setup.ipynb`.

1. Open the `dbt-training` folder in VS Code (`code .`).
2. Open `training/notebooks/01_setup.ipynb`.
3. Top right, click **Select Kernel** > **Python Environments** > **`.venv`**. (No `.venv` in the list? Command palette > *Python: Select Interpreter* > the one in `.venv`, then reload the window.)
4. Run the cells one by one (`Shift+Enter`).

You build your own dbt project in `src/`, on the `student-start` branch. The finished reference project is `src/` on `main`.

## Useful dbt commands

Run them from the repository root with the environment activated.

```bash
dbt run --select stg_sales_orders          # one model
dbt run --select +fct_sales                # a model and everything it depends on
dbt build --select tag:gold                # everything tagged gold
dbt test --select source:bronze            # only the tests on the bronze sources
dbt run --select fct_sales --full-refresh  # rebuild an incremental model from scratch
dbt docs generate                          # then:
dbt docs serve                             # documentation site on http://localhost:8080
```

The folders `target/`, `dbt_packages/`, `logs/` and `.venv/` are generated and ignored by git.
