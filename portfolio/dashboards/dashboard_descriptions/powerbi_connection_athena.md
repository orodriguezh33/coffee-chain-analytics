# Power BI Connection (Athena, no Redshift)

## Connection target
Use Athena as BI serving layer over dbt Gold tables.

- Database: `gold` (plus optional `silver` for advanced analysis)
- Tables/views: `gold.fct_*`, `gold.dim_*`

## Power BI connector setup
1. Install Amazon Athena ODBC driver.
2. Create DSN with your Athena workgroup and result location.
3. In Power BI Desktop: `Get Data` -> `Amazon Athena`.
4. Choose DSN and select connectivity mode:
   - Import (recommended for operational dashboards)
   - DirectQuery (optional for ad-hoc exploration)

## Recommended mode for this project
- Import mode + hourly refresh for manager dashboards.
- Reason: faster interactions with stable cost profile.

## Dataset loading order
1. Load dimensions first: `dim_date`, `dim_store`, `dim_product`, `dim_ingredient`
2. Load facts: `fct_sales`, `fct_waste`, `fct_labor`, `fct_inventory_snapshot`
3. Validate relationships in the model before creating visuals.
