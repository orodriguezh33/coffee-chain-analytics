# Key DAX Measures

```dax
Gross Margin % =
DIVIDE(
    SUM(fct_sales[gross_profit]),
    SUM(fct_sales[gross_revenue]),
    0
) * 100

Labor Cost % =
DIVIDE(
    SUM(fct_labor[total_labor_cost]),
    SUM(fct_labor[daily_revenue]),
    0
) * 100

Waste Rate % =
DIVIDE(
    SUM(fct_waste[waste_qty]),
    SUM(fct_waste[theoretical_consumption]),
    0
) * 100

Revenue per Labor Hour =
DIVIDE(
    SUM(fct_labor[daily_revenue]),
    SUM(fct_labor[total_hours_worked]),
    0
)

MoM Revenue Growth % =
VAR current_month =
    CALCULATE(
        SUM(fct_sales[gross_revenue]),
        DATESMTD(dim_date[full_date])
    )
VAR prior_month =
    CALCULATE(
        SUM(fct_sales[gross_revenue]),
        DATEADD(DATESMTD(dim_date[full_date]), -1, MONTH)
    )
RETURN
DIVIDE(current_month - prior_month, prior_month, 0) * 100

Stockout HIGH Count =
CALCULATE(
    COUNTROWS(fct_inventory_snapshot),
    fct_inventory_snapshot[stockout_risk_flag] = "HIGH",
    dim_date[full_date] = TODAY() - 1
)
```
