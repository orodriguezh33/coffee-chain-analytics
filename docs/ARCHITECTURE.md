# ARQUITECTURA

## Flujo end-to-end

```
POS CSV + fuentes sintéticas
        |
        v
Scripts de ingesta en Python
        |
        v
S3 Bronze (CSV crudo, particiones por ingestion_date)
        |
        v
Tablas externas Athena (coffee_chain.*)
        |
        v
dbt (Athena adapter)
  - staging (views silver)
  - intermediate (views silver)
  - marts (tablas gold)
        |
        v
Esquema Gold en Athena (gold.*)
        |
        v
Dashboards en Power BI
```

## Orquestación
El DAG de Airflow (`coffee_chain_daily`) ejecuta:
1. Ingesta POS
2. Ingesta sintética
3. Carga Bronze a S3
4. Repair de particiones en Athena
5. Quality gates pre-dbt
6. dbt run
7. dbt test
8. Quality gates post-dbt
9. Marcador de éxito/fallo en S3

## Calidad de datos
Los quality gates en Athena usan niveles de severidad:
- CRITICAL: detiene el pipeline
- WARNING: continúa con alerta
- INFO: diagnóstico informativo

## Capa de consumo
El alcance actual del portafolio usa Athena + dbt Gold como capa de consumo.
Redshift no es necesario en este repositorio para entregar BI utilizable para decisión.
