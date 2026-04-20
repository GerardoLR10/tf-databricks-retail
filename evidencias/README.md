# Evidencias

Screenshots que demuestran que el proyecto fue ejecutado correctamente.

## Lista de evidencias incluidas

### Azure Portal
- `01_azure_resource_group.png` — Resource Group con los 3 servicios (Storage, Access Connector, Databricks)
- `02_storage_containers.png` — Containers raw, bronze, silver, gold creados
- `03_raw_ecommerce.png` — Archivo Ecommerce_Sales_Prediction_Dataset.csv en raw/ecommerce/
- `04_raw_supermarket.png` — 6 archivos Instacart en raw/supermarket/
- `05_access_connector_identity.png` — Managed Identity activa en el Access Connector
- `06_role_assignment.png` — Storage Blob Data Contributor asignado al Access Connector

### Databricks
- `07_storage_credential.png` — Credential cred-tf-databricks creada en Unity Catalog
- `08_external_locations.png` — Las 4 external locations creadas y validadas
- `09_cluster_running.png` — Cluster Single Node en estado Running
- `10_catalog_explorer.png` — Catalog tf_retail con bronze/silver/gold expandidos

### Ejecución del ETL
- `11_notebook_prep_ambiente.png` — Salida exitosa del notebook 01_prep_ambiente
- `12_notebook_bronze.png` — Salida del 02_bronze (7 tablas creadas)
- `13_notebook_silver.png` — Salida del 03_silver (quality report bronze→silver)
- `14_notebook_gold.png` — Salida del 04_gold (4 tablas analytics-ready)

### Dashboard
- `15_dashboard_published.png` — Dashboard Lakeview publicado

### CI/CD
- `16_github_actions_success.png` — Pipeline de GitHub Actions ejecutado en verde
- `17_databricks_job.png` — Job desplegado vía Asset Bundle visible en Databricks Workflows

## Para qué sirven

El profesor las revisa para verificar que:
1. Aprovisionaste los servicios Azure reales (no solo dibujaste arquitectura)
2. La conexión Managed Identity → ADLS funciona end-to-end
3. El ETL se ejecutó completo (las 7+2+4 tablas existen)
4. El CI/CD está funcional (no solo definido)
