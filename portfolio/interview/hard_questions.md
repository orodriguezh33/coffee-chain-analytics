# Hard Questions & Defensible Answers

## Why no Redshift?
For this scope, Athena + dbt Gold provided full end-to-end delivery with tighter cost control and simpler operations. If concurrency/latency requirements grow, a dedicated serving warehouse is the next step.

## Are synthetic sources valid?
The objective is integration architecture validity, not source authenticity. Synthetic datasets mirror realistic upstream system boundaries and enforce cross-source modeling patterns used in production.

## How would this scale to hundreds of stores?
Keep S3/Athena/dbt foundations, optimize partitioning strategy, and tune orchestration parallelism. Introduce additional serving optimization only when observed query SLAs demand it.

## How do you handle schema drift from POS?
Bronze ingestion is permissive, while staging models and tests in dbt fail fast on breaking changes. Quality gates catch parity and integrity regressions before downstream propagation.

## How reliable is waste rate if inventory has noise?
Waste is treated as an operational signal with data-quality context. Continuity checks flag suspicious inventory transitions; trends over time drive action, not single-day anomalies alone.
