# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Capa Bronze
# MAGIC
# MAGIC Lee CSVs desde `raw/` (vía Managed Identity) y los persiste como tablas Delta en `tf_retail.bronze`.
# MAGIC
# MAGIC **Características:**
# MAGIC - Detección automática de delimitador (`,` o `;`)
# MAGIC - Limpieza de nombres de columnas a snake_case (Delta no acepta caracteres especiales)
# MAGIC - Metadatos de auditoría: `_ingest_ts` y `_source_file` por fila
# MAGIC - Modo `overwrite` → idempotente

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, input_file_name
import re

# Parámetros
STORAGE_ACCOUNT = "sttfdatabricks1993"
CATALOG_NAME    = "tf_retail"
SCHEMA_BRONZE   = "bronze"

URL_RAW = f"abfss://raw@{STORAGE_ACCOUNT}.dfs.core.windows.net/"

print(f"Catalog:   {CATALOG_NAME}")
print(f"Schema:    {SCHEMA_BRONZE}")
print(f"Raw path:  {URL_RAW}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Función helper de ingesta

# COMMAND ----------

def clean_column_name(name: str) -> str:
    """
    Limpia un nombre de columna para que sea compatible con Delta:
    quita espacios, caracteres especiales, lo deja en snake_case minúscula.
    """
    name = name.strip().lower()
    name = re.sub(r"[^\w]+", "_", name)   # cualquier no-alfanumérico → _
    name = re.sub(r"_+", "_", name)       # múltiples _ consecutivos → 1
    name = name.strip("_")                # quitar _ al inicio/fin
    return name


def ingest_to_bronze(source_path: str, table_name: str) -> dict:
    """
    Lee un CSV de raw y lo escribe como Delta en bronze.
    Detecta delimitador automáticamente (, o ;) y limpia nombres de columnas.
    """
    full_path  = URL_RAW + source_path
    full_table = f"{CATALOG_NAME}.{SCHEMA_BRONZE}.{table_name}"

    print(f"\n→ Ingesting {source_path}")
    print(f"  Source: {full_path}")
    print(f"  Target: {full_table}")

    # Detectar delimitador: leer primera línea cruda
    first_line = spark.read.text(full_path).limit(1).collect()[0][0]
    delim = ";" if first_line.count(";") > first_line.count(",") else ","
    print(f"  Delimiter detected: '{delim}'")

    # Leer CSV con el delimitador detectado
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("sep", delim)
        .csv(full_path)
    )

    # Renombrar columnas a versión limpia
    for old_name in df.columns:
        new_name = clean_column_name(old_name)
        if new_name != old_name:
            df = df.withColumnRenamed(old_name, new_name)

    # Agregar metadatos de auditoría
    df = (
        df
        .withColumn("_ingest_ts",   current_timestamp())
        .withColumn("_source_file", input_file_name())
    )

    row_count = df.count()
    col_count = len(df.columns)

    # Escribir como Delta, sobrescribiendo si ya existe
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table)
    )

    print(f"  ✅ {row_count:,} rows, {col_count} cols → {full_table}")

    return {"table": full_table, "rows": row_count, "cols": col_count}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fuente 1: E-commerce Sales Prediction (Kaggle)

# COMMAND ----------

result_ecom = ingest_to_bronze(
    source_path = "ecommerce/Ecommerce_Sales_Prediction_Dataset.csv",
    table_name  = "ecommerce_sales"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fuente 2: Instacart (supermarket) - 6 archivos

# COMMAND ----------

instacart_files = {
    "aisles.csv":               "instacart_aisles",
    "departments.csv":          "instacart_departments",
    "products.csv":             "instacart_products",
    "orders.csv":               "instacart_orders",
    "order_products__prior.csv":"instacart_order_products_prior",
    "order_products__train.csv":"instacart_order_products_train",
}

results_instacart = []
for filename, tablename in instacart_files.items():
    r = ingest_to_bronze(
        source_path = f"supermarket/{filename}",
        table_name  = tablename
    )
    results_instacart.append(r)

print(f"\n{'='*60}")
print(f"✅ {len(results_instacart)} tablas Instacart creadas")
print(f"{'='*60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen y verificación

# COMMAND ----------

print("=" * 70)
print("✅ Bronze layer completada")
print("=" * 70)

all_results = [result_ecom] + results_instacart
total_rows = sum(r["rows"] for r in all_results)

print(f"\nTablas creadas en {CATALOG_NAME}.{SCHEMA_BRONZE}:")
for r in all_results:
    print(f"  - {r['table']:60s}  {r['rows']:>10,} filas")

print(f"\nTotal filas ingestadas: {total_rows:,}")

display(spark.sql(f"SHOW TABLES IN {CATALOG_NAME}.{SCHEMA_BRONZE}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity check - muestra de 2 tablas

# COMMAND ----------

print("=== ECOMMERCE_SALES (primeras 5 filas) ===")
display(spark.sql(f"SELECT * FROM {CATALOG_NAME}.{SCHEMA_BRONZE}.ecommerce_sales LIMIT 5"))

# COMMAND ----------

print("=== INSTACART_ORDERS (primeras 5 filas) ===")
display(spark.sql(f"SELECT * FROM {CATALOG_NAME}.{SCHEMA_BRONZE}.instacart_orders LIMIT 5"))
