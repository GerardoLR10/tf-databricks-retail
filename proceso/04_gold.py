# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Capa Gold
# MAGIC
# MAGIC Genera 4 tablas analytics-ready para alimentar el dashboard Lakeview.
# MAGIC
# MAGIC | Tabla gold | Pregunta de negocio que responde |
# MAGIC |---|---|
# MAGIC | `ecom_monthly_by_category` | ¿Qué categorías generan más revenue cada mes? |
# MAGIC | `ecom_segment_by_category` | ¿Qué segmento de cliente gasta más por categoría? |
# MAGIC | `instacart_top_products` | ¿Cuáles son los productos estrella del super y su tasa de recompra? |
# MAGIC | `instacart_orders_by_dow_hour` | ¿Cuándo (día/hora) compran más los clientes? |

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG_NAME  = "tf_retail"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD   = "gold"

print(f"Catalog: {CATALOG_NAME}")
print(f"Source:  {SCHEMA_SILVER}")
print(f"Target:  {SCHEMA_GOLD}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper para escribir tablas gold

# COMMAND ----------

def write_gold(df, table_name: str, description: str):
    full_table = f"{CATALOG_NAME}.{SCHEMA_GOLD}.{table_name}"
    df = df.withColumn("_gold_ts", F.current_timestamp())
    rows = df.count()
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table)
    )
    print(f"  ✅ {full_table}")
    print(f"     {description}")
    print(f"     {rows:,} filas\n")
    return full_table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 1: Ventas mensuales por categoría (e-commerce)

# COMMAND ----------

ecom = spark.table(f"{CATALOG_NAME}.{SCHEMA_SILVER}.ecommerce_sales")

g1 = (
    ecom
    .groupBy("year", "month", "product_category")
    .agg(
        F.sum("total_revenue").alias("total_revenue"),
        F.sum("units_sold").alias("total_units"),
        F.avg("price").alias("avg_price"),
        F.avg("discount").alias("avg_discount_pct"),
        F.sum("marketing_spend").alias("total_marketing_spend"),
    )
    .withColumn("revenue_per_unit", F.round(F.col("total_revenue") / F.col("total_units"), 2))
    .withColumn("total_revenue",          F.round("total_revenue", 2))
    .withColumn("avg_price",              F.round("avg_price", 2))
    .withColumn("avg_discount_pct",       F.round("avg_discount_pct", 2))
    .withColumn("total_marketing_spend",  F.round("total_marketing_spend", 2))
    .orderBy("year", "month", F.desc("total_revenue"))
)

write_gold(
    g1,
    "ecom_monthly_by_category",
    "Revenue/units/marketing por mes y categoría (e-commerce)"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 2: Performance por segmento de cliente y categoría

# COMMAND ----------

g2 = (
    ecom
    .groupBy("customer_segment", "product_category")
    .agg(
        F.sum("total_revenue").alias("total_revenue"),
        F.sum("units_sold").alias("total_units"),
        F.count("*").alias("num_transactions"),
        F.avg("discount").alias("avg_discount_pct"),
    )
    .withColumn("avg_ticket", F.round(F.col("total_revenue") / F.col("num_transactions"), 2))
    .withColumn("total_revenue",     F.round("total_revenue", 2))
    .withColumn("avg_discount_pct",  F.round("avg_discount_pct", 2))
    .orderBy(F.desc("total_revenue"))
)

write_gold(
    g2,
    "ecom_segment_by_category",
    "Comportamiento de segmentos de clientes por categoría"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 3: Top 20 productos más comprados (Instacart)

# COMMAND ----------

inst = spark.table(f"{CATALOG_NAME}.{SCHEMA_SILVER}.instacart_orders_enriched")

g3 = (
    inst
    .groupBy("product_id", "product_name", "department", "aisle")
    .agg(
        F.count("*").alias("times_ordered"),
        F.sum("reordered").alias("times_reordered"),
        F.countDistinct("user_id").alias("unique_customers"),
    )
    .withColumn("reorder_rate_pct",
                F.round(F.col("times_reordered") / F.col("times_ordered") * 100, 2))
    .orderBy(F.desc("times_ordered"))
    .limit(20)
)

write_gold(
    g3,
    "instacart_top_products",
    "Top 20 productos más comprados con tasa de recompra"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 4: Patrones de compra por día/hora (Instacart)

# COMMAND ----------

day_names = F.create_map(*[
    F.lit(0), F.lit("Sunday"),
    F.lit(1), F.lit("Monday"),
    F.lit(2), F.lit("Tuesday"),
    F.lit(3), F.lit("Wednesday"),
    F.lit(4), F.lit("Thursday"),
    F.lit(5), F.lit("Friday"),
    F.lit(6), F.lit("Saturday"),
])

g4 = (
    inst
    .select("order_id", "order_dow", "order_hour_of_day", "department")
    .dropDuplicates(["order_id"])
    .groupBy("order_dow", "order_hour_of_day")
    .agg(
        F.count("*").alias("num_orders"),
        F.countDistinct("department").alias("unique_departments"),
    )
    .withColumn("day_name", day_names[F.col("order_dow")])
    .select("order_dow", "day_name", "order_hour_of_day", "num_orders", "unique_departments")
    .orderBy("order_dow", "order_hour_of_day")
)

write_gold(
    g4,
    "instacart_orders_by_dow_hour",
    "Distribución de órdenes por día de semana y hora del día"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificación final

# COMMAND ----------

print("=" * 60)
print("✅ Gold layer completada")
print("=" * 60)
print("\nTablas en gold:")
display(spark.sql(f"SHOW TABLES IN {CATALOG_NAME}.{SCHEMA_GOLD}"))

# COMMAND ----------

print("=== ecom_monthly_by_category (top 5 por revenue) ===")
display(spark.sql(f"""
  SELECT * FROM {CATALOG_NAME}.{SCHEMA_GOLD}.ecom_monthly_by_category
  ORDER BY total_revenue DESC LIMIT 5
"""))

# COMMAND ----------

print("=== instacart_top_products (top 5) ===")
display(spark.sql(f"SELECT * FROM {CATALOG_NAME}.{SCHEMA_GOLD}.instacart_top_products LIMIT 5"))
