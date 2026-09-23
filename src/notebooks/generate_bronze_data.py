# Databricks notebook source
# MAGIC %md
# MAGIC # AlpSport – bronze data generator
# MAGIC
# MAGIC Generates the synthetic *source system* data for the dbt training: a Swiss sports retailer
# MAGIC (11 physical stores + 1 online shop).
# MAGIC
# MAGIC | Table | Type | Behaviour in later batches |
# MAGIC |---|---|---|
# MAGIC | `products` | dimension | ~5% of products change price (`updated_at` bumps) → **snapshots** |
# MAGIC | `stores` | dimension | static |
# MAGIC | `sales_persons` | dimension | ~5% change store → **snapshots** |
# MAGIC | `customers` | dimension | new customers (88% CH, some DE/FR/IT/AT) + tier upgrades |
# MAGIC | `sales_orders` | fact (header) | new orders; ~2% of last batch's orders become `returned` → **incremental merge** |
# MAGIC | `sales_order_lines` | fact (detail) | new lines |
# MAGIC
# MAGIC **Modes**
# MAGIC * `init` – wipes and rebuilds everything: batch 0 = 2 years of history (2024-01-01 → 2025-12-31).
# MAGIC * `append` – adds **one more batch** = the next `days_per_batch` days (default 7), plus dimension changes.
# MAGIC   Run it as often as you like during the course.
# MAGIC
# MAGIC **Story in the data:** ski peaks in winter, swimming peaks in summer (with a heatwave in July 2025),
# MAGIC Saturdays and Black Friday spike, a few sales persons and best-sellers dominate (80/20).
# MAGIC
# MAGIC **Deliberately dirty rows** (`dirty_rate`, default 1%): duplicate orders/lines, upper-cased statuses,
# MAGIC orphan customer/product ids, negative quantities. They are what the dbt tests should catch.

# COMMAND ----------

dbutils.widgets.text("catalog", "bronze", "Catalog")
dbutils.widgets.text("schema", "sports_shop", "Schema")
dbutils.widgets.dropdown("mode", "init", ["init", "append"], "Mode (init wipes everything)")
dbutils.widgets.text("n_orders_init", "600000", "Orders in init batch (2 years)")
dbutils.widgets.text("n_orders_batch", "600", "Orders per appended batch")
dbutils.widgets.text("days_per_batch", "7", "Days covered per appended batch")
dbutils.widgets.text("dirty_rate", "0.001", "Share of dirty rows (0 = clean)")
dbutils.widgets.text("seed", "42", "Random seed")

# COMMAND ----------

import datetime as dt
from pyspark.sql import functions as F, Window

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
MODE = dbutils.widgets.get("mode")
N_INIT = int(dbutils.widgets.get("n_orders_init"))
N_BATCH = int(dbutils.widgets.get("n_orders_batch"))
DAYS_PER_BATCH = int(dbutils.widgets.get("days_per_batch"))
DIRTY = float(dbutils.widgets.get("dirty_rate"))
SEED = int(dbutils.widgets.get("seed"))

FQ = f"{CATALOG}.{SCHEMA}"
tbl = lambda name: f"{FQ}.{name}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")

if MODE == "append" and not spark.catalog.tableExists(tbl("sales_orders")):
    print("No existing data found -> switching to init mode")
    MODE = "init"
INIT = MODE == "init"

HISTORY_START = dt.date(2024, 1, 1)
HISTORY_END = dt.date(2026, 1, 1)  # exclusive

if INIT:
    BATCH_ID, WIN_START, WIN_END, N_ORDERS = 0, HISTORY_START, HISTORY_END, N_INIT
else:
    last = spark.table(tbl("sales_orders")).agg(
        F.max("_batch_id").alias("b"), F.max(F.to_date("order_date")).alias("d")
    ).first()
    BATCH_ID = last.b + 1
    WIN_START = last.d + dt.timedelta(days=1)
    WIN_END = WIN_START + dt.timedelta(days=DAYS_PER_BATCH)
    N_ORDERS = N_BATCH

# "Simulated now": all updated_at values written by this batch use it, so it increases
# monotonically from batch to batch (handy for incremental models and snapshots).
SIM_NOW = dt.datetime.combine(WIN_END, dt.time.min)
S = SEED + BATCH_ID * 1000  # per-batch seed base

print(f"{MODE=} {BATCH_ID=} window={WIN_START} -> {WIN_END} (excl.) orders={N_ORDERS} sim_now={SIM_NOW}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers and reference lists

# COMMAND ----------

FIRST_NAMES = ["Lukas", "Noah", "Elias", "Leon", "Matteo", "Nico", "Luca", "Jan", "Fabian", "David",
               "Sophie", "Lena", "Mia", "Anna", "Laura", "Emma", "Julia", "Nina", "Sara", "Elena",
               "Marco", "Pascal", "Céline", "Aline", "Thomas", "Andrea", "Michael", "Sandra", "Claudia", "Daniel"]
LAST_NAMES = ["Müller", "Meier", "Schmid", "Keller", "Weber", "Huber", "Schneider", "Meyer", "Steiner", "Fischer",
              "Gerber", "Brunner", "Baumann", "Frei", "Zimmermann", "Moser", "Widmer", "Wyss", "Graf", "Roth",
              "Favre", "Rochat", "Bianchi", "Rossi", "Martin", "Dubois", "Bernasconi", "Ferrari", "Jaggi", "Lüthi"]
CITIES = ["Zürich", "Genève", "Basel", "Bern", "Lausanne", "Luzern", "St. Gallen", "Winterthur",
          "Lugano", "Biel/Bienne", "Thun", "Chur", "Zug", "Fribourg", "Sion"]
# customers: 88% CH, 4% DE, 3% FR, 3% IT, 2% AT (city drawn from these lists)
FOREIGN_CITIES = {"DE": ["Konstanz", "Freiburg", "München", "Stuttgart", "Berlin"],
                  "FR": ["Annecy", "Mulhouse", "Lyon", "Paris"],
                  "IT": ["Como", "Milano", "Varese", "Torino"],
                  "AT": ["Feldkirch", "Innsbruck", "Wien"]}

# store_id, name, city, canton, type, opened, traffic weight
STORES = [
    ("S01", "AlpSport Zürich Bahnhofstrasse", "Zürich", "ZH", "city", dt.date(2015, 3, 1), 0.18),
    ("S02", "AlpSport Genève Rive", "Genève", "GE", "city", dt.date(2016, 9, 1), 0.11),
    ("S03", "AlpSport Basel Centre", "Basel", "BS", "city", dt.date(2016, 5, 1), 0.09),
    ("S04", "AlpSport Bern Marktgasse", "Bern", "BE", "city", dt.date(2017, 4, 1), 0.08),
    ("S05", "AlpSport Lausanne Flon", "Lausanne", "VD", "city", dt.date(2018, 10, 1), 0.08),
    ("S06", "AlpSport Luzern Altstadt", "Luzern", "LU", "city", dt.date(2018, 3, 1), 0.07),
    ("S07", "AlpSport St. Gallen", "St. Gallen", "SG", "city", dt.date(2019, 6, 1), 0.05),
    ("S08", "AlpSport Zermatt", "Zermatt", "VS", "mountain", dt.date(2017, 11, 1), 0.06),
    ("S09", "AlpSport Davos", "Davos", "GR", "mountain", dt.date(2019, 12, 1), 0.05),
    ("S10", "AlpSport Interlaken", "Interlaken", "BE", "mountain", dt.date(2020, 5, 1), 0.04),
    ("S11", "AlpSport Outlet Lugano", "Lugano", "TI", "outlet", dt.date(2021, 9, 1), 0.03),
    ("S12", "AlpSport Online Shop", "Zürich", "ZH", "online", dt.date(2015, 1, 1), 0.16),
]
# sales persons per physical store (S12 = online has none)
PERSONS_PER_STORE = [("S01", 9), ("S02", 7), ("S03", 6), ("S04", 6), ("S05", 5), ("S06", 5),
                     ("S07", 4), ("S08", 5), ("S09", 4), ("S10", 5), ("S11", 4)]

# category -> [(product type, base price CHF)]
CATALOG_TYPES = {
    "Ski & Snowboard": [("Ski jacket", 349), ("Ski pants", 229), ("Alpine skis", 599), ("Snowboard", 449),
                        ("Ski boots", 379), ("Ski helmet", 149), ("Ski goggles", 119), ("Ski gloves", 79)],
    "Running": [("Running shoes", 149), ("Trail running shoes", 169), ("Running shorts", 45),
                ("Running jacket", 129), ("Running watch", 249), ("Running tights", 69)],
    "Cycling": [("Road bike", 1899), ("Mountain bike", 1499), ("Cycling helmet", 99), ("Cycling jersey", 89),
                ("Bike lights", 39), ("Cycling shoes", 179), ("Bike pump", 29)],
    "Hiking & Outdoor": [("Hiking boots", 199), ("Backpack", 129), ("Trekking poles", 79), ("Tent", 349),
                         ("Sleeping bag", 179), ("Rain jacket", 199), ("Headlamp", 49)],
    "Swimming": [("Swimsuit", 59), ("Swim goggles", 25), ("Wetsuit", 249), ("Swim cap", 15),
                 ("Beach towel", 29), ("Fins", 45)],
    "Fitness": [("Yoga mat", 39), ("Dumbbell set", 89), ("Kettlebell", 49), ("Resistance bands", 25),
                ("Treadmill", 1299), ("Sports bra", 49), ("Gym bag", 59)],
    "Team Sports": [("Football", 35), ("Football boots", 129), ("Basketball", 45), ("Tennis racket", 189),
                    ("Volleyball", 39), ("Shin guards", 25), ("Hockey stick", 119)],
    "Apparel": [("T-shirt", 35), ("Hoodie", 89), ("Sports socks", 15), ("Fleece jacket", 119),
                ("Leggings", 59), ("Cap", 25), ("Base layer", 79)],
}
BRANDS = [("Alpenglow", 1.3), ("Matterhorn Gear", 1.5), ("Nordwand", 1.2), ("SwissTrail", 1.0),
          ("Rhône Sport", 0.9), ("Vitalis", 0.8), ("Peak & Co", 1.1), ("Basic Line", 0.7)]

# base share of each category in the basket, and monthly seasonality multipliers (Jan..Dec)
CAT_BASE = {"Apparel": 0.20, "Running": 0.16, "Hiking & Outdoor": 0.14, "Fitness": 0.12,
            "Cycling": 0.12, "Ski & Snowboard": 0.12, "Team Sports": 0.08, "Swimming": 0.06}
CAT_SEASON = {
    "Ski & Snowboard": [3, 3, 1.5, .3, .1, .1, .1, .1, .1, .8, 2, 3.5],
    "Swimming": [.3, .3, .3, .4, 1.2, 2, 3, 3, .8, .3, .3, .3],
    "Cycling": [.5, .5, 1, 2, 2, 2, 2, 2, 2, 1, .5, .5],
    "Hiking & Outdoor": [.6, .6, .7, 1, 1.8, 1.8, 1.8, 1.8, 1.8, 1.5, .6, .6],
    "Running": [1, 1, 1.3, 1.3, 1.3, 1, 1, 1, 1.2, 1.2, 1, 1],
    "Fitness": [1.8, 1.3, .8, .8, .8, .8, .8, .8, .8, 1.2, 1.2, 1.2],
    "Team Sports": [1] * 12,
    "Apparel": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1.5],
}


def pick_from(values, rand_col, skew=1.0):
    """Random element of a python list; skew > 1 favours the first elements."""
    arr = F.array(*[F.lit(v) for v in values])
    idx = (F.floor(F.pow(rand_col, skew) * len(values)) + 1).cast("int")
    return F.element_at(arr, idx)


def weighted_pick(left, r_col, weights, keep, group=(), order_by=None):
    """Attach `keep` columns of `weights` to `left`, chosen with probability proportional to `weights.w`.

    `left[r_col]` must be uniform in [0, 1). Cumulative weights are built per `group`.
    """
    order_by = order_by or keep[0]
    part = Window.partitionBy(*group) if group else Window.partitionBy(F.lit(1))
    run = part.orderBy(order_by).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    w = (weights
         .withColumn("_tot", F.sum("w").over(part))
         .withColumn("_run", F.sum("w").over(run))
         .withColumn("_n", F.count(F.lit(1)).over(part))
         .withColumn("_rn", F.row_number().over(part.orderBy(order_by)))
         .withColumn("_lo", (F.col("_run") - F.col("w")) / F.col("_tot"))
         .withColumn("_hi", F.when(F.col("_rn") == F.col("_n"), F.lit(2.0)).otherwise(F.col("_run") / F.col("_tot"))))
    for g in group:
        w = w.withColumnRenamed(g, f"_k_{g}")
    w = w.select(*[f"_k_{g}" for g in group], *keep, "_lo", "_hi")
    cond = [F.col(g) == F.col(f"_k_{g}") for g in group] + [F.col(r_col) >= F.col("_lo"), F.col(r_col) < F.col("_hi")]
    return left.join(w, cond, "inner").drop(*[f"_k_{g}" for g in group], "_lo", "_hi")


def write(df, name):
    """Init: overwrite the table. Append: add the rows."""
    writer = df.write.format("delta")
    if INIT:
        writer.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tbl(name))
    else:
        writer.mode("append").saveAsTable(tbl(name))


def meta(df, ts=None):
    """Technical bronze columns."""
    return df.withColumn("_batch_id", F.lit(BATCH_ID)).withColumn("_ingested_at", F.current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensions

# COMMAND ----------

def make_customers(first_idx, n, created, seed):
    """`n` customers whose ids start at C{first_idx+1}."""
    df = spark.range(first_idx, first_idx + n, numPartitions=4).select(
        F.concat(F.lit("C"), F.lpad((F.col("id") + 1).cast("string"), 5, "0")).alias("customer_id"),
        pick_from(FIRST_NAMES, F.rand(seed + 1)).alias("first_name"),
        pick_from(LAST_NAMES, F.rand(seed + 2)).alias("last_name"),
        F.rand(seed + 6).alias("r_country"),
        F.rand(seed + 3).alias("r_city"),
        F.rand(seed + 4).alias("r_tier"),
        F.rand(seed + 5).alias("r_created"),
        F.col("id"),
    )
    country = (F.when(F.col("r_country") < 0.88, "CH").when(F.col("r_country") < 0.92, "DE")
                .when(F.col("r_country") < 0.95, "FR").when(F.col("r_country") < 0.98, "IT").otherwise("AT"))
    city = F.when(country == "CH", pick_from(CITIES, F.col("r_city"), skew=1.6))
    for code, cities in FOREIGN_CITIES.items():
        city = city.when(country == code, pick_from(cities, F.col("r_city")))
    tier = (F.when(F.col("r_tier") < 0.60, "basic").when(F.col("r_tier") < 0.85, "silver")
             .when(F.col("r_tier") < 0.97, "gold").otherwise("platinum"))
    span = (created[1] - created[0]).total_seconds()
    created_ts = (F.lit(created[0]).cast("timestamp").cast("long") + (F.col("r_created") * span).cast("long")).cast("timestamp")
    email = F.concat(
        F.translate(F.lower("first_name"), "üöäéèç", "uoaeec"), F.lit("."),
        F.translate(F.lower("last_name"), "üöäéèç", "uoaeec"), (F.col("id") + 1).cast("string"),
        F.lit("@example.com"))
    return (df.select("customer_id", "first_name", "last_name", email.alias("email"), city.alias("city"),
                      country.alias("country_code"),
                      tier.alias("loyalty_tier"), created_ts.alias("created_at"))
              .withColumn("updated_at", F.col("created_at")))


if INIT:
    # --- stores (static) ---
    stores = spark.createDataFrame(
        [s[:6] for s in STORES],
        "store_id string, store_name string, city string, canton string, store_type string, opened_date date")
    write(meta(stores), "stores")

    # --- products: product types x brands, ~70% of the combinations -> ~300 products ---
    types = spark.createDataFrame(
        [(cat, t, float(p)) for cat, lst in CATALOG_TYPES.items() for t, p in lst],
        "category string, product_type string, base_price double")
    brands = spark.createDataFrame(BRANDS, "brand string, price_factor double")
    combos = (types.crossJoin(brands)
              .where(F.abs(F.hash("product_type", "brand")) % 10 < 7)
              .withColumn("product_name", F.concat_ws(" ", "brand", "product_type"))
              .withColumn("price", (F.round(F.col("base_price") * F.col("price_factor") / 5) * 5 - 0.10)))
    products = (combos
                .withColumn("product_id", F.concat(F.lit("P"), F.lpad(
                    F.row_number().over(Window.orderBy("category", "product_type", "brand")).cast("string"), 4, "0")))
                .select("product_id", "product_name", "category", "brand",
                        F.col("price").cast("decimal(10,2)").alias("list_price"),
                        (F.col("price") * (0.45 + (F.abs(F.hash("product_name")) % 16) / 100)).cast("decimal(10,2)").alias("unit_cost"),
                        F.lit(True).alias("is_active"),
                        F.lit(dt.datetime(2023, 6, 1)).alias("created_at"),
                        F.lit(dt.datetime(2023, 6, 1)).alias("updated_at")))
    write(meta(products), "products")

    # --- sales persons ---
    persons = (spark.createDataFrame(PERSONS_PER_STORE, "store_id string, n int")
               .select("store_id", F.explode(F.sequence(F.lit(1), F.col("n"))).alias("seq"))
               .withColumn("sales_person_id", F.concat(F.lit("SP"), F.lpad(
                   F.row_number().over(Window.orderBy("store_id", "seq")).cast("string"), 3, "0")))
               .withColumn("first_name", pick_from(FIRST_NAMES, F.rand(S + 11)))
               .withColumn("last_name", pick_from(LAST_NAMES, F.rand(S + 12)))
               .withColumn("hire_date", F.date_add(F.lit(dt.date(2014, 1, 1)), (F.rand(S + 13) * 3650).cast("int")))
               .select("sales_person_id", "first_name", "last_name",
                       F.concat(F.translate(F.lower("first_name"), "üöäéèç", "uoaeec"), F.lit("."),
                                F.translate(F.lower("last_name"), "üöäéèç", "uoaeec"), F.lit("@alpsport.example")).alias("email"),
                       "store_id", "hire_date",
                       F.col("hire_date").cast("timestamp").alias("created_at"),
                       F.col("hire_date").cast("timestamp").alias("updated_at")))
    write(meta(persons), "sales_persons")

    # --- customers ---
    write(meta(make_customers(0, 5000, (dt.datetime(2022, 1, 1), dt.datetime(2024, 12, 31)), S + 100)), "customers")

else:
    ts = f"TIMESTAMP'{SIM_NOW:%Y-%m-%d %H:%M:%S}'"

    # products: ~5% get a price change (+5% or -10% promo) -> product snapshots
    spark.sql(f"""
        UPDATE {tbl('products')}
        SET list_price = CAST(ROUND(list_price * CASE WHEN abs(hash(product_id, 'p', {BATCH_ID})) % 2 = 0 THEN 1.05 ELSE 0.90 END, 1) AS DECIMAL(10,2)),
            updated_at = {ts}, _batch_id = {BATCH_ID}, _ingested_at = current_timestamp()
        WHERE is_active AND abs(hash(product_id, {BATCH_ID})) % 100 < 5""")

    # sales persons: ~5% move to another physical store -> sales person snapshots
    new_store = f"concat('S', lpad(cast(1 + abs(hash(sales_person_id, 'mv', {BATCH_ID})) % 11 as string), 2, '0'))"
    spark.sql(f"""
        UPDATE {tbl('sales_persons')}
        SET store_id = {new_store}, updated_at = {ts}, _batch_id = {BATCH_ID}, _ingested_at = current_timestamp()
        WHERE abs(hash(sales_person_id, {BATCH_ID})) % 100 < 5 AND {new_store} <> store_id""")

    # customers: ~3% climb one loyalty tier, then add new customers
    spark.sql(f"""
        UPDATE {tbl('customers')}
        SET loyalty_tier = CASE loyalty_tier WHEN 'basic' THEN 'silver' WHEN 'silver' THEN 'gold' ELSE 'platinum' END,
            updated_at = {ts}, _batch_id = {BATCH_ID}, _ingested_at = current_timestamp()
        WHERE loyalty_tier <> 'platinum' AND abs(hash(customer_id, {BATCH_ID})) % 100 < 3""")
    N_NEW_CUSTOMERS = 40
    n_existing = spark.table(tbl("customers")).count()
    write(meta(make_customers(n_existing, N_NEW_CUSTOMERS,
                              (dt.datetime.combine(WIN_START, dt.time.min), SIM_NOW), S + 100)), "customers")

    # orders from the previous batch: ~2% get returned -> late updates for incremental merge
    spark.sql(f"""
        UPDATE {tbl('sales_orders')}
        SET status = 'returned', updated_at = {ts}
        WHERE status = 'completed' AND _batch_id = {BATCH_ID - 1} AND abs(hash(order_id)) % 100 < 2""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orders (header)
# MAGIC
# MAGIC Dates, stores, categories are drawn with weights (weekday, month, growth, Black Friday, store traffic),
# MAGIC sales persons and customers with a skew so that a few of them do most of the sales.

# COMMAND ----------

N_NEW_CUSTOMERS = 0 if INIT else N_NEW_CUSTOMERS
n_customers = spark.table(tbl("customers")).count()

# --- calendar weights ---
DOW_W = [0.5, 0.8, 0.8, 0.9, 1.0, 1.3, 1.8]  # Sunday..Saturday (dayofweek 1..7)
MONTH_W = [1.2, 1.1, 0.9, 0.9, 1.0, 1.0, 1.0, 1.0, 0.9, 1.0, 1.3, 1.6]
cal = (spark.range((WIN_END - WIN_START).days)
       .select(F.date_add(F.lit(WIN_START), F.col("id").cast("int")).alias("d"))
       .withColumn("dow", F.dayofweek("d")).withColumn("m", F.month("d")).withColumn("dom", F.dayofmonth("d"))
       .withColumn("w", F.element_at(F.array(*map(F.lit, DOW_W)), F.col("dow"))
                   * F.element_at(F.array(*map(F.lit, MONTH_W)), F.col("m"))
                   * (1 + 0.15 * F.datediff("d", F.lit(HISTORY_START)) / 365)
                   * F.when((F.col("m") == 11) & (F.col("dow") == 6) & F.col("dom").between(23, 29), 3.0).otherwise(1.0))
       .select("d", "w"))

store_w = spark.createDataFrame([(s[0], s[6]) for s in STORES], "store_id string, w double")

persons_rank = (spark.table(tbl("sales_persons"))
                .select("store_id", "sales_person_id",
                        (F.row_number().over(Window.partitionBy("store_id").orderBy("sales_person_id")) - 1).alias("s_rn"),
                        F.count("*").over(Window.partitionBy("store_id")).alias("s_cnt")))
store_cnt = persons_rank.select("store_id", "s_cnt").distinct()

if N_NEW_CUSTOMERS:
    # 10% of orders come from the freshly created customers
    cust_idx = F.when(F.col("r_new") < 0.10, n_customers - 1 - F.floor(F.col("r_cust") * N_NEW_CUSTOMERS)) \
                .otherwise(F.floor(F.pow(F.col("r_cust"), 1.8) * (n_customers - N_NEW_CUSTOMERS)))
else:
    cust_idx = F.floor(F.pow(F.col("r_cust"), 1.8) * n_customers)

base = spark.range(N_ORDERS, numPartitions=8).select(
    "id", *[F.rand(S + 1 + i).alias(n) for i, n in enumerate(
        ["r_date", "r_store", "r_person", "r_cust", "r_anon", "r_pay", "r_status", "r_time", "r_new"])])

o = weighted_pick(base, "r_date", cal, keep=["d"])
o = weighted_pick(o, "r_store", store_w, keep=["store_id"])
o = o.join(store_cnt, "store_id", "left")
o = o.withColumn("s_rn", F.floor(F.pow(F.col("r_person"), 2) * F.col("s_cnt")).cast("int"))  # 80/20 skew
o = o.join(persons_rank.select("store_id", "s_rn", "sales_person_id"), ["store_id", "s_rn"], "left")

is_online = F.col("store_id") == "S12"
payment = F.when(is_online,
                 F.when(F.col("r_pay") < 0.55, "card").when(F.col("r_pay") < 0.85, "twint").otherwise("invoice")) \
           .otherwise(F.when(F.col("r_pay") < 0.55, "card").when(F.col("r_pay") < 0.88, "twint").otherwise("cash"))
order_ts = (F.col("d").cast("timestamp").cast("long") + (9 * 3600 + F.col("r_time") * 11 * 3600).cast("long")).cast("timestamp")

orders = o.select(
    F.concat(F.lit("ORD-"), F.lpad(F.lit(BATCH_ID).cast("string"), 3, "0"), F.lit("-"),
             F.lpad(F.col("id").cast("string"), 7, "0")).alias("order_id"),
    order_ts.alias("order_date"),
    F.when(~is_online & (F.col("r_anon") < 0.15), F.lit(None))  # walk-in customers without loyalty card
     .otherwise(F.concat(F.lit("C"), F.lpad((cust_idx + 1).cast("string"), 5, "0"))).alias("customer_id"),
    "store_id", "sales_person_id",
    F.when(is_online, "online").otherwise("in_store").alias("channel"),
    payment.alias("payment_method"),
    F.when(F.col("r_status") < 0.04, "cancelled").otherwise("completed").alias("status"),
    order_ts.alias("created_at"), order_ts.alias("updated_at"))

# dirty data: upper-cased statuses and orphan customer ids
if DIRTY > 0:
    orders = (orders
              .withColumn("status", F.when(F.rand(S + 50) < DIRTY, F.upper("status")).otherwise(F.col("status")))
              .withColumn("customer_id", F.when(F.rand(S + 51) < DIRTY, F.lit("C99999")).otherwise(F.col("customer_id"))))
write(meta(orders), "sales_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Order lines
# MAGIC
# MAGIC Built from the orders just written (read back from Delta), so header and lines always match.

# COMMAND ----------

# category share per year-month (small lookup table, built on the driver)
ym_list = sorted({(WIN_START + dt.timedelta(days=i)).year * 100 + (WIN_START + dt.timedelta(days=i)).month
                  for i in range((WIN_END - WIN_START).days)})
cat_rows = []
for ym in ym_list:
    for cat, base_w in CAT_BASE.items():
        w = base_w * CAT_SEASON[cat][ym % 100 - 1]
        if ym == 202507 and cat == "Swimming":
            w *= 2.5  # heatwave
        if ym == 202507 and cat == "Hiking & Outdoor":
            w *= 0.7
        cat_rows.append((ym, cat, float(w)))
cat_w = spark.createDataFrame(cat_rows, "ym int, category string, w double")

prod = spark.table(tbl("products")).where("is_active")
prod_rank = prod.select("product_id", "category", "list_price",
                        (F.row_number().over(Window.partitionBy("category").orderBy("product_id")) - 1).alias("p_rn"),
                        F.count("*").over(Window.partitionBy("category")).alias("p_cnt"))
cat_cnt = prod_rank.select("category", "p_cnt").distinct()

ob = spark.table(tbl("sales_orders")).where(F.col("_batch_id") == BATCH_ID)
l = (ob.select("order_id", (F.year("order_date") * 100 + F.month("order_date")).alias("ym"),
               F.month("order_date").alias("m"))
     .withColumn("r_nl", F.rand(S + 60))
     .withColumn("n_lines", F.when(F.col("r_nl") < .50, 1).when(F.col("r_nl") < .78, 2).when(F.col("r_nl") < .92, 3)
                            .when(F.col("r_nl") < .98, 4).otherwise(5))
     .withColumn("line_no", F.explode(F.sequence(F.lit(1), F.col("n_lines"))))
     .select("order_id", "ym", "m", "line_no",
             *[F.rand(S + 61 + i).alias(n) for i, n in enumerate(["r_cat", "r_prod", "r_qty", "r_disc", "r_disc2"])]))

l = weighted_pick(l, "r_cat", cat_w, keep=["category"], group=["ym"])
l = l.join(cat_cnt, "category")
l = l.withColumn("p_rn", F.floor(F.pow(F.col("r_prod"), 1.5) * F.col("p_cnt")).cast("int"))  # best-sellers
l = l.join(prod_rank.select("category", "p_rn", "product_id", "list_price"), ["category", "p_rn"])

in_sale = F.col("m").isin(1, 7, 11)  # winter, summer and Black Friday sales
discount = (F.when(in_sale & (F.col("r_disc") < 0.30), 10 + 10 * F.floor(F.col("r_disc2") * 3))
             .when(~in_sale & (F.col("r_disc") < 0.06), 5).otherwise(0)).cast("int")
quantity = F.when(F.col("r_qty") < .70, 1).when(F.col("r_qty") < .90, 2).when(F.col("r_qty") < .97, 3).otherwise(4)

lines = l.select(
    F.concat("order_id", F.lit("-L"), F.col("line_no").cast("string")).alias("order_line_id"),
    "order_id", "product_id", quantity.alias("quantity"),
    F.col("list_price").alias("unit_price"), discount.alias("discount_pct"))

# dirty data: negative quantities and orphan product ids
if DIRTY > 0:
    lines = (lines
             .withColumn("quantity", F.when(F.rand(S + 70) < DIRTY, -F.col("quantity")).otherwise(F.col("quantity")))
             .withColumn("product_id", F.when(F.rand(S + 71) < DIRTY, F.lit("P9999")).otherwise(F.col("product_id"))))
write(meta(lines), "sales_order_lines")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dirty data: exact duplicates
# MAGIC Re-inserted from the stored rows so that the copies are identical (tests: `unique`).

# COMMAND ----------

if DIRTY > 0:
    for name in ["sales_orders", "sales_order_lines"]:
        dups = spark.table(tbl(name)).where(F.col("_batch_id") == BATCH_ID).sample(False, DIRTY, S + 80)
        dups.write.format("delta").mode("append").saveAsTable(tbl(name))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(f"Batch {BATCH_ID} written to {FQ} (window {WIN_START} -> {WIN_END})")
spark.sql(f"""
    SELECT 'products' AS tbl, COUNT(*) AS n_rows, NULL AS last_batch FROM {tbl('products')}
    UNION ALL SELECT 'stores', COUNT(*), NULL FROM {tbl('stores')}
    UNION ALL SELECT 'sales_persons', COUNT(*), NULL FROM {tbl('sales_persons')}
    UNION ALL SELECT 'customers', COUNT(*), NULL FROM {tbl('customers')}
    UNION ALL SELECT 'sales_orders', COUNT(*), MAX(_batch_id) FROM {tbl('sales_orders')}
    UNION ALL SELECT 'sales_order_lines', COUNT(*), MAX(_batch_id) FROM {tbl('sales_order_lines')}
""").display()

# COMMAND ----------

# rows per batch and monthly revenue by category: the seasonality should be visible
spark.sql(f"""
    SELECT date_trunc('month', o.order_date) AS month, p.category,
           ROUND(SUM(l.quantity * l.unit_price * (1 - l.discount_pct / 100)), 0) AS revenue_chf
    FROM {tbl('sales_order_lines')} l
    JOIN {tbl('sales_orders')} o USING (order_id)
    JOIN {tbl('products')} p USING (product_id)
    WHERE l.quantity > 0
    GROUP BY 1, 2 ORDER BY 1, 2
""").display()

# COMMAND ----------

# integrity report: the dirty rows the dbt tests are supposed to find
orders_t, lines_t = spark.table(tbl("sales_orders")), spark.table(tbl("sales_order_lines"))
checks = {
    "duplicate_orders": orders_t.count() - orders_t.select("order_id").distinct().count(),
    "bad_status": orders_t.where(~F.col("status").isin("completed", "cancelled", "returned")).count(),
    "orphan_customers": orders_t.where(F.col("customer_id").isNotNull())
                                .join(spark.table(tbl("customers")).select("customer_id"), "customer_id", "left_anti").count(),
    "non_positive_quantity": lines_t.where("quantity <= 0").count(),
    "orphan_products": lines_t.join(spark.table(tbl("products")).select("product_id"), "product_id", "left_anti").count(),
}
spark.createDataFrame(list(checks.items()), "check string, n_rows long").display()
