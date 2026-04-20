# Datasets

Esta carpeta documenta los **datasets de origen** del ETL.

## ⚠️ Los CSVs no se commitean al repo

Los archivos CSV están excluidos vía `.gitignore` por dos razones:
1. Son grandes (~70 MB en total) y GitHub no es para almacenamiento de datos.
2. Tienen licencia Kaggle (cada usuario los descarga con su cuenta).

## Datasets utilizados

### 1. E-commerce Sales Prediction Dataset
- **Source:** https://www.kaggle.com/datasets/nevildhinoja/e-commerce-sales-prediction-dataset
- **Tamaño:** ~52 KB (1,000 filas)
- **Contenido:** ventas online con precio, descuento, categoría, segmento de cliente y gasto en marketing
- **Archivo esperado:** `Ecommerce_Sales_Prediction_Dataset.csv`

### 2. Instacart Market Basket Analysis (subido como "Supermarket Superstore Bundle")
- **Source:** https://www.kaggle.com/datasets/amunsentom/supermarket-superstore-dataset-bundle
- **Tamaño:** ~70 MB (~3.2M filas en 6 archivos)
- **Contenido:** órdenes reales de supermercado online con productos, pasillos, departamentos
- **Archivos esperados:**
  - `aisles.csv`
  - `departments.csv`
  - `products.csv`
  - `orders.csv`
  - `order_products__prior.csv`
  - `order_products__train.csv`

## Cómo cargar los datasets

1. Descarga ambos datasets desde Kaggle (necesitas cuenta).
2. Descomprime los ZIPs.
3. Sube los archivos a tu Storage Account ADLS Gen2 con esta estructura:

```
abfss://raw@<storage_account>.dfs.core.windows.net/
├── ecommerce/
│   └── Ecommerce_Sales_Prediction_Dataset.csv
└── supermarket/
    ├── aisles.csv
    ├── departments.csv
    ├── products.csv
    ├── orders.csv
    ├── order_products__prior.csv
    └── order_products__train.csv
```

## ⚠️ Hallazgo de calidad de datos

Los archivos `orders.csv`, `order_products__prior.csv` y `order_products__train.csv` están **truncados a 1,048,575 filas exactas** (límite de Excel = 2^20 - 1).
El dataset original de Instacart tiene millones más de filas; quien lo subió a Kaggle lo abrió en Excel y guardó solo lo que cabía. Esto es relevante para interpretar los conteos de `instacart_orders_enriched` en silver.
