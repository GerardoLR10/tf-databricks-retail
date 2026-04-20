# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Capa Silver
# MAGIC
# MAGIC Lee bronze, aplica reglas de calidad de datos y escribe en `tf_retail.silver`.
# MAGIC
# MAGIC **Reglas aplicadas:**
# MAGIC - Normalización de nombres de columnas (minúsculas consistentes)
# MAGIC - Casteo de tipos (date, int, double)
# MAGIC - Drop de filas con nulos en campos clave
# MAGIC - Filtrado de outliers obvios (price <= 0, units_sold <= 0)
# MAGIC - Deduplicación
# MAGIC - Para Instacart: **join de las 6 tablas** en un único fact enriquecido con dimensiones

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, StringType, DateType

CATALOG_NAME  = "tf_retail"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"

print(f"Catalog: {CATALOG_NAME}")
print(f"Source:  {SCHEMA_BRONZE}")
print(f"Target:  {SCHEMA_SILVER}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: E-commerce
# MAGIC
# MAGIC - Casts a tipos correctos
# MAGIC - Drop de nulos en campos clave (date, price, units_sold, product_category)
# MAGIC - Filtros de calidad (price > 0, units_sold > 0)
# MAGIC - Deduplicación
# MAGIC - Columnas derivadas: `total_revenue`, `year`, `month`

# COMMAND ----------

src = f"{CATALOG_NAME}.{SCHEMA_BRONZE}.ecommerce_sales"
tgt = f"{CATALOG_NAME}.{SCHEMA_SILVER}.ecommerce_sales"

print(f"\n→ {src} → {tgt}")

df = spark.table(src)
print(f"  Bronze rows: {df.count():,}")

# 1. Normalizar nombres a minúsculas
for c in df.columns:
    df = df.withColumnRenamed(c, c.lower())

# 2. Castear tipos
df = (
    df
    .withColumn("date",            F.to_date("date"))
    .withColumn("price",           F.col("price").cast(DoubleType()))
    .withColumn("discount",        F.col("discount").cast(DoubleType()))
    .withColumn("marketing_spend", F.col("marketing_spend").cast(DoubleType()))
    .withColumn("units_sold",      F.col("units_sold").cast(IntegerType()))
)

# 3. Drop nulos en campos clave
df = df.dropna(subset=["date", "price", "units_sold", "product_category"])

# 4. Filtros de calidad
df = df.filter((F.col("price") > 0) & (F.col("units_sold") > 0))

# 5. Deduplicar
df = df.dropDuplicates()

# 6. Columnas derivadas para analytics
df = (
    df
    .withColumn("total_revenue",
                F.round(F.col("price") * F.col("units_sold") * (1 - F.col("discount")/100), 2))
    .withColumn("year",  F.year("date"))
    .withColumn("month", F.month("date"))
)

# 7. Reordenar columnas (negocio primero, metadatos al final)
business_cols = [c for c in df.columns if not c.startswith("_")]
meta_cols     = [c for c in df.columns if c.startswith("_")]
df = df.select(*business_cols, *meta_cols)

silver_count = df.count()
print(f"  Silver rows: {silver_count:,}")

(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(tgt)
)
print(f"  ✅ Escrito {tgt}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: Instacart unificado
# MAGIC
# MAGIC Une las 6 tablas en un fact enriquecido (1 fila por order_id + product_id con todo el contexto).

# COMMAND ----------

orders        = spark.table(f"{CATALOG_NAME}.{SCHEMA_BRONZE}.instacart_orders")
products      = spark.table(f"{CATALOG_NAME}.{SCHEMA_BRONZE}.instacart_products")
aisles        = spark.table(f"{CATALOG_NAME}.{SCHEMA_BRONZE}.instacart_aisles")
departments   = spark.table(f"{CATALOG_NAME}.{SCHEMA_BRONZE}.instacart_departments")
op_prior      = spark.table(f"{CATALOG_NAME}.{SCHEMA_BRONZE}.instacart_order_products_prior")
op_train      = spark.table(f"{CATALOG_NAME}.{SCHEMA_BRONZE}.instacart_order_products_train")

print(f"  orders:       {orders.count():,}")
print(f"  products:     {products.count():,}")
print(f"  aisles:       {aisles.count():,}")
print(f"  departments:  {departments.count():,}")
print(f"  op_prior:     {op_prior.count():,}")
print(f"  op_train:     {op_train.count():,}")

# 1. Unir prior + train (mismo esquema, son los datos transaccionales)
op_all = op_prior.unionByName(op_train).dropDuplicates(["order_id", "product_id"])
print(f"  op_all (deduplicado): {op_all.count():,}")

# 2. Limpiar metadatos antes del join
clean_cols = lambda d: d.drop("_ingest_ts", "_source_file")

orders      = clean_cols(orders)
products    = clean_cols(products)
aisles      = clean_cols(aisles)
departments = clean_cols(departments)
op_all      = clean_cols(op_all)

# 3. Construir el fact enriquecido vía joins
fact = (
    op_all
    .join(orders,      on="order_id",      how="inner")
    .join(products,    on="product_id",    how="inner")
    .join(aisles,      on="aisle_id",      how="left")
    .join(departments, on="department_id", how="left")
)

# 4. Drop nulos en claves de negocio
fact = fact.dropna(subset=["order_id", "product_id", "user_id"])

# 5. Casts de tipos
fact = (
    fact
    .withColumn("order_id",               F.col("order_id").cast(IntegerType()))
    .withColumn("product_id",             F.col("product_id").cast(IntegerType()))
    .withColumn("user_id",                F.col("user_id").cast(IntegerType()))
    .withColumn("add_to_cart_order",      F.col("add_to_cart_order").cast(IntegerType()))
    .withColumn("reordered",              F.col("reordered").cast(IntegerType()))
    .withColumn("order_number",           F.col("order_number").cast(IntegerType()))
    .withColumn("order_dow",              F.col("order_dow").cast(IntegerType()))
    .withColumn("order_hour_of_day",      F.col("order_hour_of_day").cast(IntegerType()))
    .withColumn("days_since_prior_order", F.col("days_since_prior_order").cast(DoubleType()))
)

# 6. Manejo de nulos en campos opcionales
fact = fact.fillna({"days_since_prior_order": 0.0})

# 7. Agregar metadato de procesamiento
fact = fact.withColumn("_silver_ts", F.current_timestamp())

# 8. Reordenar columnas
final_cols = [
    "order_id", "user_id", "product_id", "product_name",
    "department_id", "department", "aisle_id", "aisle",
    "add_to_cart_order", "reordered",
    "order_number", "order_dow", "order_hour_of_day", "days_since_prior_order",
    "eval_set",
    "_silver_ts"
]
existing_cols = [c for c in final_cols if c in fact.columns]
fact = fact.select(*existing_cols)

silver_count = fact.count()
print(f"\n  Fact rows final: {silver_count:,}")

tgt = f"{CATALOG_NAME}.{SCHEMA_SILVER}.instacart_orders_enriched"
(
    fact.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(tgt)
)
print(f"  ✅ Escrito {tgt}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificación

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {CATALOG_NAME}.{SCHEMA_SILVER}"))

# COMMAND ----------

print("=== Muestra ecommerce_sales ===")
display(spark.sql(f"SELECT * FROM {CATALOG_NAME}.{SCHEMA_SILVER}.ecommerce_sales LIMIT 5"))

# COMMAND ----------

print("=== Muestra instacart_orders_enriched ===")
display(spark.sql(f"SELECT * FROM {CATALOG_NAME}.{SCHEMA_SILVER}.instacart_orders_enriched LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality report: Bronze → Silver

# COMMAND ----------

def compare_layers(table_name, bronze_table, silver_table):
    b = spark.table(bronze_table).count()
    s = spark.table(silver_table).count()
    diff = b - s
    pct = (diff / b * 100) if b > 0 else 0
    print(f"\n{table_name}")
    print(f"  Bronze: {b:>10,}")
    print(f"  Silver: {s:>10,}")
    print(f"  Diff:   {diff:>10,}  ({pct:.2f}% removed)")

print("=" * 60)
print("Quality report: Bronze → Silver")
print("=" * 60)

compare_layers(
    "E-commerce",
    f"{CATALOG_NAME}.{SCHEMA_BRONZE}.ecommerce_sales",
    f"{CATALOG_NAME}.{SCHEMA_SILVER}.ecommerce_sales"
)

op_total = (
    spark.table(f"{CATALOG_NAME}.{SCHEMA_BRONZE}.instacart_order_products_prior").count() +
    spark.table(f"{CATALOG_NAME}.{SCHEMA_BRONZE}.instacart_order_products_train").count()
)
silver_count = spark.table(f"{CATALOG_NAME}.{SCHEMA_SILVER}.instacart_orders_enriched").count()
print(f"\nInstacart")
print(f"  op_prior + op_train: {op_total:>10,}")
print(f"  enriched (silver):   {silver_count:>10,}")
print(f"  Diff:                {op_total - silver_count:>10,}  (deduplicación + joins)")
