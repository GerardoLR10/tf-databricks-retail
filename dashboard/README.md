# Dashboard

Esta carpeta contiene el dashboard Lakeview del proyecto.

## Archivos

- `Retail_Analytics_TF_Databricks.lvdash.json` — definición del dashboard exportada desde Databricks.
- `dashboard_pagina1_revenue.png` — screenshot del visual "Revenue mensual por categoría".
- `dashboard_pagina2_segmento.png` — screenshot del visual "Revenue por segmento y categoría".
- `dashboard_pagina3_top_products.png` — screenshot del visual "Top 20 productos Instacart".
- `dashboard_pagina4_heatmap.png` — screenshot del visual "Patrones de compra por día y hora".

## Cómo exportar el JSON desde Databricks

1. Abre el dashboard en Databricks.
2. Click en los **3 puntos** arriba a la derecha → **Export** → **Download as JSON**.
3. Guarda el archivo aquí con extensión `.lvdash.json`.

## Cómo importar el dashboard en otro workspace

1. Menú izquierdo → **Dashboards** → botón **Create dashboard** → **Import from JSON**.
2. Selecciona el archivo `Retail_Analytics_TF_Databricks.lvdash.json`.
3. Asegúrate de tener las tablas `tf_retail.gold.*` creadas (correr el ETL completo primero).
4. Selecciona el SQL Warehouse a usar.

## Datasets de origen del dashboard

Las 4 visualizaciones leen de las siguientes tablas Gold:

| Visual | Tabla origen |
|---|---|
| Revenue mensual por categoría | `tf_retail.gold.ecom_monthly_by_category` |
| Revenue por segmento y categoría | `tf_retail.gold.ecom_segment_by_category` |
| Top 20 productos Instacart | `tf_retail.gold.instacart_top_products` |
| Patrones de compra por día/hora | `tf_retail.gold.instacart_orders_by_dow_hour` |
