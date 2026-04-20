# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Preparación del ambiente
# MAGIC
# MAGIC Crea catalog, schemas y valida la conexión a ADLS Gen2 vía Managed Identity.
# MAGIC
# MAGIC **Pre-requisitos manuales (hechos vía portal Azure + Databricks UI):**
# MAGIC - Storage Account ADLS Gen2 con containers `raw`, `bronze`, `silver`, `gold`
# MAGIC - Access Connector for Azure Databricks con rol Storage Blob Data Contributor sobre el storage
# MAGIC - Storage Credential (`cred-tf-databricks`) creada en Unity Catalog apuntando al Access Connector
# MAGIC - External Locations creadas para los 4 containers (extloc_raw, extloc_bronze, extloc_silver, extloc_gold)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parámetros del proyecto

# COMMAND ----------

# Parámetros (ajusta solo si cambiaste nombres en Azure)
STORAGE_ACCOUNT = "sttfdatabricks1993"
CATALOG_NAME    = "tf_retail"
SCHEMA_BRONZE   = "bronze"
SCHEMA_SILVER   = "silver"
SCHEMA_GOLD     = "gold"

URL_RAW    = f"abfss://raw@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
URL_BRONZE = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
URL_SILVER = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
URL_GOLD   = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/"

print(f"Storage Account: {STORAGE_ACCOUNT}")
print(f"Catalog:         {CATALOG_NAME}")
print(f"Schemas:         {SCHEMA_BRONZE}, {SCHEMA_SILVER}, {SCHEMA_GOLD}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validar acceso a raw vía Managed Identity
# MAGIC Si esto falla, hay un problema con la external location o el role del Access Connector.

# COMMAND ----------

print(f"Listando contenido de RAW: {URL_RAW}\n")
files = dbutils.fs.ls(URL_RAW)

for f in files:
    print(f"  📁 {f.name}  ({f.size} bytes)")

print(f"\n✅ Acceso a RAW funciona vía Managed Identity")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear catalog principal
# MAGIC `MANAGED LOCATION` necesaria porque el default storage del metastore puede no estar configurado.

# COMMAND ----------

spark.sql(f"""
  CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}
  MANAGED LOCATION '{URL_BRONZE}'
  COMMENT 'Trabajo Final - Retail ETL con arquitectura medallion'
""")

display(spark.sql("SHOW CATALOGS"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear los 3 schemas (bronze / silver / gold)
# MAGIC Cada schema apunta a su external location → tablas creadas heredan ubicación.

# COMMAND ----------

spark.sql(f"""
  CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_BRONZE}
  MANAGED LOCATION '{URL_BRONZE}'
  COMMENT 'Capa Bronze - Datos crudos en Delta'
""")

spark.sql(f"""
  CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_SILVER}
  MANAGED LOCATION '{URL_SILVER}'
  COMMENT 'Capa Silver - Datos limpios y normalizados'
""")

spark.sql(f"""
  CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_GOLD}
  MANAGED LOCATION '{URL_GOLD}'
  COMMENT 'Capa Gold - Datos agregados para analytics'
""")

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG_NAME}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificación final

# COMMAND ----------

for schema in [SCHEMA_BRONZE, SCHEMA_SILVER, SCHEMA_GOLD]:
    print(f"\n📂 Schema: {CATALOG_NAME}.{schema}")
    df = spark.sql(f"DESCRIBE SCHEMA EXTENDED {CATALOG_NAME}.{schema}")
    df.show(truncate=False)

# COMMAND ----------

print("=" * 60)
print(f"✅ PrepAmb completado")
print("=" * 60)
print(f"\nCatalog creado:  {CATALOG_NAME}")
print(f"Schemas creados:")
print(f"  - {CATALOG_NAME}.{SCHEMA_BRONZE}  →  {URL_BRONZE}")
print(f"  - {CATALOG_NAME}.{SCHEMA_SILVER}  →  {URL_SILVER}")
print(f"  - {CATALOG_NAME}.{SCHEMA_GOLD}    →  {URL_GOLD}")
print(f"\nFuente raw verificada: {URL_RAW}")
print(f"\nListo para correr el notebook 02_bronze")
