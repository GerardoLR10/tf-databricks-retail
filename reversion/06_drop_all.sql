-- ============================================================
-- 06_drop_all.sql
-- Reversión: elimina catalog, schemas y tablas del proyecto
--
-- ⚠️ DESTRUCTIVO: borra todas las tablas Delta de tf_retail
--    y elimina sus rutas físicas en ADLS (gracias a CASCADE).
--
-- Útil para:
--   - Limpiar el ambiente y rehacer el ETL desde cero
--   - Liberar storage al terminar el proyecto
-- ============================================================

-- ------------------------------------------------------------
-- 1) Drop tablas individuales (opcional, CASCADE ya las borra)
--    Útil si quieres ver qué se está borrando.
-- ------------------------------------------------------------

-- Bronze
DROP TABLE IF EXISTS tf_retail.bronze.ecommerce_sales;
DROP TABLE IF EXISTS tf_retail.bronze.instacart_aisles;
DROP TABLE IF EXISTS tf_retail.bronze.instacart_departments;
DROP TABLE IF EXISTS tf_retail.bronze.instacart_products;
DROP TABLE IF EXISTS tf_retail.bronze.instacart_orders;
DROP TABLE IF EXISTS tf_retail.bronze.instacart_order_products_prior;
DROP TABLE IF EXISTS tf_retail.bronze.instacart_order_products_train;

-- Silver
DROP TABLE IF EXISTS tf_retail.silver.ecommerce_sales;
DROP TABLE IF EXISTS tf_retail.silver.instacart_orders_enriched;

-- Gold
DROP TABLE IF EXISTS tf_retail.gold.ecom_monthly_by_category;
DROP TABLE IF EXISTS tf_retail.gold.ecom_segment_by_category;
DROP TABLE IF EXISTS tf_retail.gold.instacart_top_products;
DROP TABLE IF EXISTS tf_retail.gold.instacart_orders_by_dow_hour;

-- ------------------------------------------------------------
-- 2) Drop schemas
-- ------------------------------------------------------------

DROP SCHEMA IF EXISTS tf_retail.bronze CASCADE;
DROP SCHEMA IF EXISTS tf_retail.silver CASCADE;
DROP SCHEMA IF EXISTS tf_retail.gold   CASCADE;

-- ------------------------------------------------------------
-- 3) Drop catalog
-- ------------------------------------------------------------

DROP CATALOG IF EXISTS tf_retail CASCADE;

-- ------------------------------------------------------------
-- 4) Verificación: el catalog ya no debe aparecer
-- ------------------------------------------------------------

SHOW CATALOGS;

-- ============================================================
-- NOTA: las external locations, storage credential, y los
-- containers de ADLS NO se borran con este script.
-- Para limpieza completa de Azure:
--   az group delete --name rg-tf-databricks --yes
-- (borra todo el resource group de un solo comando)
-- ============================================================
