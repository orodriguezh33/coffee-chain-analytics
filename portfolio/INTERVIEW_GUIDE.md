# Interview Guide

## Cómo resumir el proyecto
Plataforma analítica end-to-end para retail de café con `S3 + Athena + dbt + Airflow + Power BI`, enfocada en cuatro problemas operativos:
- margen
- merma
- riesgo de stockout
- eficiencia laboral

La entrega actual de BI incluye `3 dashboards` funcionales:
- `Executive Dashboard`
- `Labor & Waste Dashboard`
- `Inventory Dashboard`

## Bullets de CV

### Data Engineer
- Construí un pipeline analítico para retail de café con S3, Athena, dbt y Airflow, integrando ventas POS y fuentes sintéticas en Bronze, Silver y Gold.
- Implementé validaciones antes y después de dbt para mejorar la confiabilidad de los datos entregados al dashboard.

### Analytics Engineer
- Diseñé modelos dbt y marts dimensionales para KPIs de margen, merma, labor cost y stockout risk.
- Resolví diferencias de nombres de producto entre ventas, costos y recetas para estabilizar los joins del modelo.

## Narrativa corta de entrevista

### Problema
En operación retail, el POS por sí solo no explica el negocio. Para medir merma, margen real o eficiencia laboral necesitas combinar ventas, costos, recetas e inventario físico.

### Arquitectura
- S3 como data lake
- Athena como capa analítica
- dbt para modelado y testing
- Airflow para orquestación
- Power BI para consumo mediante snapshot exportado desde Athena

### Decisión clave
El modelo se diseñó con grano explícito. `fct_sales` opera a nivel línea de producto y `fct_waste` a nivel ingrediente x tienda x día. Esa decisión hace posible medir merma de forma defendible.

### Diferenciador
La pieza fuerte del proyecto es `Inventory` y la capa de `Labor & Waste`, porque unen POS, BOM y snapshot físico de inventario. La mayoría de proyectos públicos con este dataset se quedan en ventas y margen.

## Preguntas difíciles

### ¿Por qué no Redshift?
Porque para este alcance Athena + dbt resolvió el flujo end-to-end con menor costo y menor complejidad operativa. Si la latencia o concurrencia crecieran, Redshift sería la siguiente evolución, no un requisito inicial.

### ¿Los datos sintéticos invalidan el proyecto?
No. El objetivo es demostrar arquitectura de integración y modelado cross-source. Los datos sintéticos replican fronteras realistas entre sistemas upstream.

### ¿Cómo escalarías esto?
Mantendría S3, Athena y dbt como base. Si creciera el volumen, revisaría particionamiento y luego evaluaría una capa de consumo más especializada.

### ¿Cómo manejas schema drift?
Bronze absorbe cambios con baja fricción y staging/test detectan rápido los cambios incompatibles antes de que lleguen al dashboard.

## Checklist antes de publicar
- `README.md` alineado al alcance real
- `.env` fuera de Git
- artefactos locales fuera del repo
- screenshots reales de dashboards exportadas desde Power BI
- sample queries listas en `portfolio/sample_queries/`
- docs técnicos consistentes con la arquitectura actual

## Plantilla breve para LinkedIn
- Problema: decisiones operativas sin datos integrados
- Solución: `S3 + Athena + dbt + Airflow + Power BI`
- Diferenciador: merma basada en ventas + receta + inventario
- Cierre: foco en decisión operativa, no solo reporting
