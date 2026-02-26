# Phase 8: BI & Decision Layer (Athena-only)

## Serving strategy
Redshift is intentionally excluded from this project scope.
The BI serving layer uses:
- Athena as query engine
- dbt Gold tables in schema `gold`
- Power BI in Import mode for operational dashboards

## Dashboard set

### 1) Executive Summary
- Audience: GM / Operations director
- Frequency: daily and weekly
- Core questions: Are we on track? Which store drags margin?

### 2) Financial Deep Dive
- Audience: finance / cost analyst
- Frequency: weekly
- Core questions: Which products are profitable? Where is COGS pressure?

### 3) Waste & Inventory Control
- Audience: store manager / operations
- Frequency: daily
- Core questions: Which ingredients are over-wasted? What is at stockout risk?

### 4) Labor & Operational Efficiency
- Audience: store manager / HR operations
- Frequency: daily + weekly planning
- Core questions: Is labor % on target? Is staffing aligned with demand peaks?

### 5) Branch & Product Performance
- Audience: operations leadership
- Frequency: weekly review
- Core questions: Which branch needs intervention? Which products should be promoted?

## Recommended Power BI mode
- Import mode for manager-facing dashboards (faster interaction)
- Optional DirectQuery for ad-hoc analyst exploration

## Interview narrative
"In production I would evaluate Redshift Serverless for concurrent low-latency BI. For this portfolio, I prioritized cost-efficient architecture and delivered end-to-end decisions on Athena + dbt Gold."
