"""Small helpers used by the training notebooks so that you never have to write connection code.

    from helpers import q, dbt, check, show_file, restore_checkpoint

Works on macOS, Linux and Windows.
"""
import os
import re
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

import pandas as pd
import yaml

HERE = Path(__file__).resolve().parent
TRAINING = HERE.parent
ROOT = TRAINING.parent  # repo root: where dbt_project.yml lives, and where dbt commands run
SRC = ROOT / "src"  # the files a student is actually building
CHECKPOINTS = TRAINING / "checkpoints"
PROFILE_NAME = "dbt-training"

pd.set_option("display.max_columns", 50)
pd.set_option("display.width", 200)


def load_profile(target=None):
    """Read the `dbt-training` profile from ~/.dbt/profiles.yml (or $DBT_PROFILES_DIR)."""
    path = Path(os.environ.get("DBT_PROFILES_DIR", Path.home() / ".dbt")) / "profiles.yml"
    if not path.exists():
        raise FileNotFoundError(f"{path} not found. Follow step 4 of SETUP.md to create your profile.")
    profiles = yaml.safe_load(path.read_text(encoding="utf-8"))
    if PROFILE_NAME not in profiles:
        raise KeyError(f"No profile called '{PROFILE_NAME}' in {path}. Follow step 4 of SETUP.md.")
    profile = profiles[PROFILE_NAME]
    output = profile["outputs"][target or profile["target"]]
    # support token: "{{ env_var('DBT_TOKEN') }}"
    resolved = {}
    for key, value in output.items():
        if isinstance(value, str):
            value = re.sub(r"\{\{\s*env_var\('([^']+)'\)\s*\}\}", lambda m: os.environ.get(m.group(1), ""), value)
        resolved[key] = value
    return resolved


def my_schema():
    """Your personal schema: the `schema` of your dbt profile."""
    try:
        return load_profile().get("schema")
    except Exception:
        return None


SCHEMA = my_schema()


def profile_summary():
    """Profile with the token masked, safe to display."""
    p = dict(load_profile())
    if p.get("token"):
        p["token"] = p["token"][:4] + "…" + "*" * 6
    return p


_connection = None


def _connect():
    global _connection
    from databricks import sql

    if _connection is None:
        p = load_profile()
        _connection = sql.connect(
            server_hostname=p["host"], http_path=p["http_path"], access_token=p["token"]
        )
    return _connection


def q(query):
    """Run a SQL query on your warehouse and return a pandas DataFrame."""
    try:
        with _connect().cursor() as cursor:
            cursor.execute(query)
            if cursor.description is None:
                return pd.DataFrame()
            columns = [c[0] for c in cursor.description]
            return pd.DataFrame([tuple(r) for r in cursor.fetchall()], columns=columns)
    except Exception:
        global _connection
        _connection = None  # reconnect next time (token/warehouse hiccup)
        raise


def scalar(query):
    """First value of the first row."""
    return q(query).iloc[0, 0]


def _dbt_executable():
    """dbt of the environment running this notebook (dbt.exe on Windows)."""
    found = shutil.which("dbt", path=str(Path(sys.executable).parent))
    return found or shutil.which("dbt") or "dbt"


def dbt(args, project=True):
    """Run a dbt command in the training project and stream its output, e.g. dbt("run --select stg_products").

    It first prints the same command as you would type it in a terminal.
    """
    cmd = [_dbt_executable(), "--no-use-colors"] + shlex.split(args)
    where = "from the repository root" if project else "from any folder"
    print(f"Terminal equivalent ({where}, virtual environment activated):\n    dbt {args}\n")
    env = os.environ.copy()
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    proc = subprocess.Popen(cmd, cwd=ROOT if project else None, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace", env=env)
    for line in proc.stdout:
        # drop the timestamp prefix to keep the output readable
        print(re.sub(r"^\d\d:\d\d:\d\d\s+", "", line.rstrip()))
    proc.wait()
    print(f"\n-> exit code {proc.returncode}")


def check(label, condition, hint=""):
    """Print a green tick or a red cross."""
    print(("✅ " if condition else "❌ ") + label + ("" if condition else f"  -> {hint}" if hint else ""))
    return bool(condition)


def show_file(path):
    """Print a file of the project (path relative to the repository root)."""
    print(f"--- {path}")
    print((ROOT / path).read_text(encoding="utf-8"))


def project_tree():
    """List the files of the project you are building."""
    for p in sorted(SRC.rglob("*")):
        rel = p.relative_to(SRC)
        if p.is_file() and not any(part in ("target", "dbt_packages", "logs") for part in rel.parts) \
                and p.name != ".gitkeep":
            print(rel.as_posix())


def restore_checkpoint(n):
    """Copy the checkpoints 02..n over src/ (catch up if you are stuck; overwrites your files)."""
    n = int(n)
    for cp in sorted(CHECKPOINTS.iterdir()):
        if cp.is_dir() and 2 <= int(cp.name[:2]) <= n:
            for f in cp.rglob("*"):
                if f.is_file():
                    dest = SRC / f.relative_to(cp)
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy(f, dest)
    print(f"Project restored to the end of module {n:02d}.")


def bronze_status():
    """Latest batch loaded in bronze."""
    return q("""
        SELECT MAX(_batch_id) AS last_batch, MAX(order_date) AS last_order_date, COUNT(*) AS order_rows
        FROM bronze.sports_shop.sales_orders""")
