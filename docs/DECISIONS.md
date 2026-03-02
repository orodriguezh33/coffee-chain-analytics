# Decisiones de Diseño

## 1) Bronze se mantiene inmutable en CSV
- Motivo: preservar el estado exacto de llegada para trazabilidad y reproceso seguro.
- Compromiso: escaneos más costosos si se consulta directamente.

## 2) Silver/Gold usan Parquet vía dbt
- Motivo: menor costo de escaneo y mejor rendimiento en Athena.
- Compromiso: paso adicional de transformación.

## 3) Particionamiento por fecha de ingesta en Bronze
- Motivo: separa fecha de llegada de fecha de negocio y soporta archivos tardíos.
- Compromiso: las consultas analíticas requieren lógica adicional de fechas aguas abajo.

## 4) Seed explícito de mapeo de productos
- Motivo: los nombres en POS difieren de costos/recetas; el mapeo evita joins rotos y márgenes en NULL.
- Compromiso: requiere mantenimiento cuando aparecen productos nuevos.

## 5) Quality gates con severidad
- Motivo: no todos los problemas de datos deben bloquear entrega; CRITICAL detiene, WARNING/INFO mantienen continuidad con visibilidad.
- Compromiso: requiere gobernanza clara sobre criterios de severidad.

## 6) Athena como capa de consumo actual (sin Redshift en alcance)
- Motivo: el portafolio prioriza corrección end-to-end, reproducibilidad y control de costos.
- Compromiso: la latencia del tablero puede ser mayor que un almacén dedicado bajo alta concurrencia.
