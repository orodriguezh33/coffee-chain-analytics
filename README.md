# Plataforma Analítica Coffee Chain

> Proyecto end-to-end de ingeniería de datos para operación retail de café, enfocado en margen, merma, inventario y eficiencia laboral.

## Problema
En retail de café se toman decisiones diarias (turnos, compras, promociones) con datos incompletos. El POS por sí solo no explica margen real, merma operativa ni eficiencia de personal.

Este proyecto construye una plataforma analítica completa que une ventas, costos, recetas, inventario y labor para generar KPIs accionables.

## Qué habilita este sistema

| Decisión | KPI | Frecuencia |
|---|---|---|
| Qué productos promover | Margen bruto % por producto | Diario |
| Cuándo reordenar ingredientes | Riesgo de quiebre de stock | Diario |
| Cómo planificar turnos | Costo laboral % por turno | Diario |
| Si una promoción funcionó | Métricas de impacto promocional | Por evento |
| Dónde está la merma | Waste rate % por ingrediente | Diario |

## Arquitectura (alcance actual)

```
Fuentes de datos (POS + sintéticos ERP/WMS/Nómina/CRM)
        -> Ingesta Python
S3 Bronze (CSV crudo)
        -> Tablas externas Athena
        -> dbt (adapter Athena)
           staging -> intermediate -> marts
S3 Gold + Athena (capa de consumo)
        -> Dashboards Power BI
Orquestación: Airflow
Calidad: Quality gates en Athena (CRITICAL/WARNING/INFO)
```

Nota: Redshift se excluyó intencionalmente del alcance final por costo/control. El proyecto opera completo con Athena + dbt Gold.

## Stack

| Herramienta | Rol | Motivo |
|---|---|---|
| S3 | Data lake | Almacenamiento Bronze/Silver/Gold escalable y de bajo costo |
| Athena | Motor SQL + validaciones | Serverless, sin infraestructura a gestionar |
| dbt (Athena) | Transformaciones, tests, linaje | Modelado SQL con pruebas y documentación |
| Airflow | Orquestación | Reintentos, dependencias y observabilidad |
| Python | Ingesta + fuentes sintéticas | Integración flexible de múltiples fuentes |
| Power BI | BI y dashboards | Capa de consumo orientada a negocio |

## Modelo de datos
Marts principales implementados en el esquema Gold de dbt:

- Hechos: `fct_sales`, `fct_waste`, `fct_labor`, `fct_inventory_snapshot`
- Dimensiones: `dim_date`, `dim_store`, `dim_product`, `dim_ingredient`

## Dashboards

| Dashboard | Audiencia | Decisión |
|---|---|---|
| Executive Summary | Dirección / operaciones | ¿Vamos en línea con el mes? |
| Financial Deep Dive | Finanzas / analista | ¿Qué productos tienen margen no sostenible? |
| Waste & Inventory | Gerente de tienda | ¿Qué se debe reordenar antes de abrir? |
| Labor Efficiency | Operaciones / RRHH | ¿El staffing está alineado con la demanda? |
| Branch & Product | Director de operaciones | ¿Qué tienda necesita intervención esta semana? |

## Estructura del repositorio

```
coffee-chain-analytics/
├── ingestion/
├── scripts/
├── dbt/
├── airflow/
├── sql/
├── data/
├── docs/
└── portfolio/
```

## Documentación
- Arquitectura: `docs/ARCHITECTURE.md`
- Diccionario de datos: `docs/DATA_DICTIONARY.md`
- Decisiones de diseño: `docs/DECISIONS.md`
- Capa BI y dashboards: `portfolio/BI_DASHBOARDS.md`
- Material de entrevista: `portfolio/INTERVIEW_GUIDE.md`

## Cómo correr el proyecto
El proyecto incluye un CLI simple en la raíz para ejecutar los pasos principales.

```bash
python main.py --help
python main.py ingest-local --run-date 2023-06-15
python main.py upload-bronze --run-date 2023-06-15
python main.py athena-ddl
python main.py quality-gates completeness integrity --run-date 2023-06-15
python main.py export-bi
```

## Ejecución local (Airflow)

```bash
docker compose --profile airflow run --rm airflow-init
docker compose --profile airflow up -d airflow-db airflow-webserver airflow-scheduler
```

UI de Airflow: [http://localhost:8080](http://localhost:8080)  
DAG principal: `coffee_chain_daily`

## Export para Power BI (snapshot CSV)
Si no puedes conectar Power BI directo a Athena (por ejemplo, VM en Parallels), exporta un snapshot CSV:

```bash
docker compose --env-file .env run --rm \
  -v $(pwd)/sql:/app/sql \
  -v $(pwd)/scripts:/app/scripts \
  -v $(pwd)/exports:/app/exports \
  ingestion \
  python /app/scripts/bi/export_powerbi_snapshot.py
```

Salida esperada:
- `exports/bi_snapshot/<snapshot_id>/*.csv`
- `exports/bi_snapshot/<snapshot_id>/manifest.json`

## Sobre el proyecto
El objetivo del repositorio es mostrar un flujo completo y entendible:
- ingesta
- validación
- transformación
- orquestación
- consumo en BI

La prioridad fue dejar un proyecto claro, reproducible y defendible para un primer portafolio serio de datos.
