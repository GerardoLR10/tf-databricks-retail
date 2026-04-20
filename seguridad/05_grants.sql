-- ============================================================
-- 05_grants.sql
-- Seguridad: usuarios, grupos y GRANTS sobre tf_retail
-- ============================================================
--
-- Roles definidos:
--   - tf_retail_consumers: solo lectura sobre gold (analistas, BI)
--   - tf_retail_engineers: lectura/escritura en bronze, silver, gold (ingenieros de datos)
--
-- Ejecutar en Databricks SQL Editor o desde un notebook con %sql
-- ============================================================

-- ------------------------------------------------------------
-- 1) Crear grupos (estos comandos requieren permisos de account admin)
--    Si no eres account admin, créalos vía Account Console UI:
--    https://accounts.azuredatabricks.net/users/groups
-- ------------------------------------------------------------

-- CREATE GROUP IF NOT EXISTS tf_retail_consumers;
-- CREATE GROUP IF NOT EXISTS tf_retail_engineers;

-- ------------------------------------------------------------
-- 2) Permisos a nivel de catalog
-- ------------------------------------------------------------

-- USE CATALOG: ambos grupos pueden navegar el catalog
GRANT USE CATALOG ON CATALOG tf_retail TO `tf_retail_consumers`;
GRANT USE CATALOG ON CATALOG tf_retail TO `tf_retail_engineers`;

-- ------------------------------------------------------------
-- 3) Permisos a nivel de schema
-- ------------------------------------------------------------

-- Consumers: solo gold
GRANT USE SCHEMA, SELECT ON SCHEMA tf_retail.gold TO `tf_retail_consumers`;

-- Engineers: USE + SELECT sobre las 3 capas
GRANT USE SCHEMA, SELECT ON SCHEMA tf_retail.bronze TO `tf_retail_engineers`;
GRANT USE SCHEMA, SELECT ON SCHEMA tf_retail.silver TO `tf_retail_engineers`;
GRANT USE SCHEMA, SELECT ON SCHEMA tf_retail.gold   TO `tf_retail_engineers`;

-- Engineers: capacidad de crear y modificar tablas
GRANT CREATE TABLE, MODIFY ON SCHEMA tf_retail.bronze TO `tf_retail_engineers`;
GRANT CREATE TABLE, MODIFY ON SCHEMA tf_retail.silver TO `tf_retail_engineers`;
GRANT CREATE TABLE, MODIFY ON SCHEMA tf_retail.gold   TO `tf_retail_engineers`;

-- ------------------------------------------------------------
-- 4) Permisos sobre external locations (necesarios para que
--    los engineers puedan crear tablas externas)
-- ------------------------------------------------------------

GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION extloc_bronze TO `tf_retail_engineers`;
GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION extloc_silver TO `tf_retail_engineers`;
GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION extloc_gold   TO `tf_retail_engineers`;
GRANT READ FILES                ON EXTERNAL LOCATION extloc_raw  TO `tf_retail_engineers`;

-- ------------------------------------------------------------
-- 5) Verificación de los GRANTS aplicados
-- ------------------------------------------------------------

SHOW GRANTS ON CATALOG tf_retail;
SHOW GRANTS ON SCHEMA  tf_retail.bronze;
SHOW GRANTS ON SCHEMA  tf_retail.silver;
SHOW GRANTS ON SCHEMA  tf_retail.gold;
