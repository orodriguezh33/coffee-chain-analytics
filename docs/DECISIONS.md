# DECISIONS

## 1) Bronze remains immutable CSV
- Why: preserve exact arrival state for traceability and safe reprocessing.
- Tradeoff: larger scans if queried directly.

## 2) Silver/Gold use Parquet through dbt
- Why: lower scan cost and better performance in Athena.
- Tradeoff: additional transformation step.

## 3) Partition by ingestion date in Bronze
- Why: separates arrival timing from business date, supports late-arriving files.
- Tradeoff: analytical queries require date logic in downstream models.

## 4) Explicit product mapping seed
- Why: POS naming differs from cost/recipe systems; mapping prevents broken joins and hidden NULL margins.
- Tradeoff: requires ongoing curation as new product names appear.

## 5) Quality gates with severity levels
- Why: not all data issues should block delivery; CRITICAL stops pipeline, WARNING/INFO preserve continuity with visibility.
- Tradeoff: requires clear governance on severity policy.

## 6) Athena as current serving layer (no Redshift in scope)
- Why: portfolio focuses on end-to-end correctness, reproducibility, and cost control.
- Tradeoff: dashboard latency can be higher than dedicated warehouse serving under heavy concurrency.
