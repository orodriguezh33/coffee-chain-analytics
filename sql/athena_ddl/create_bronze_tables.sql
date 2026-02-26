-- Crea las tablas externas de Bronze en Athena
-- Athena lee directamente desde S3 - no mueve datos
-- IMPORTANTE: Bronze se modela mayormente como STRING
-- Reemplazo de bucket se hace desde run_ddl.py usando {{S3_BUCKET}}

CREATE EXTERNAL TABLE IF NOT EXISTS coffee_chain.bronze_pos_transactions (
    transaction_id      STRING,
    transaction_date    STRING,
    transaction_time    STRING,
    transaction_qty     STRING,
    store_id            STRING,
    store_location      STRING,
    product_id          STRING,
    unit_price          STRING,
    product_category    STRING,
    product_type        STRING,
    product_detail      STRING,
    ingestion_timestamp STRING,
    source_file         STRING
)
PARTITIONED BY (ingestion_date STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 's3://{{S3_BUCKET}}/bronze/pos/transactions/'
TBLPROPERTIES (
    'skip.header.line.count' = '1',
    'classification' = 'csv'
);

CREATE EXTERNAL TABLE IF NOT EXISTS coffee_chain.bronze_product_costs (
    product_name      STRING,
    product_category  STRING,
    unit_price_std    STRING,
    unit_cost         STRING,
    cogs_pct          STRING,
    valid_from        STRING,
    valid_to          STRING,
    source_system     STRING
)
PARTITIONED BY (ingestion_date STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 's3://{{S3_BUCKET}}/bronze/synthetic/product_costs/'
TBLPROPERTIES ('skip.header.line.count' = '1');

CREATE EXTERNAL TABLE IF NOT EXISTS coffee_chain.bronze_recipes_bom (
    product_name              STRING,
    ingredient_name           STRING,
    qty_required_per_unit     STRING,
    unit_of_measure           STRING,
    ingredient_cost_per_unit  STRING,
    last_updated              STRING
)
PARTITIONED BY (ingestion_date STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 's3://{{S3_BUCKET}}/bronze/synthetic/recipes_bom/'
TBLPROPERTIES ('skip.header.line.count' = '1');

CREATE EXTERNAL TABLE IF NOT EXISTS coffee_chain.bronze_daily_inventory (
    inventory_date    STRING,
    store_id          STRING,
    store_name        STRING,
    ingredient_name   STRING,
    opening_stock     STRING,
    units_received    STRING,
    actual_consumed   STRING,
    closing_stock     STRING
)
PARTITIONED BY (ingestion_date STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 's3://{{S3_BUCKET}}/bronze/synthetic/daily_inventory/'
TBLPROPERTIES ('skip.header.line.count' = '1');

CREATE EXTERNAL TABLE IF NOT EXISTS coffee_chain.bronze_staff_shifts (
    shift_date          STRING,
    store_id            STRING,
    store_name          STRING,
    shift_type          STRING,
    shift_start         STRING,
    shift_end           STRING,
    employees_on_shift  STRING,
    hours_per_employee  STRING,
    total_hours         STRING,
    hourly_rate_usd     STRING,
    labor_cost_usd      STRING
)
PARTITIONED BY (ingestion_date STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 's3://{{S3_BUCKET}}/bronze/synthetic/staff_shifts/'
TBLPROPERTIES ('skip.header.line.count' = '1');

CREATE EXTERNAL TABLE IF NOT EXISTS coffee_chain.bronze_promotions (
    promotion_id         STRING,
    promotion_name       STRING,
    discount_type        STRING,
    discount_value       STRING,
    applicable_category  STRING,
    start_date           STRING,
    end_date             STRING,
    applicable_days      STRING,
    applicable_hours     STRING,
    source_system        STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 's3://{{S3_BUCKET}}/bronze/synthetic/promotions/'
TBLPROPERTIES ('skip.header.line.count' = '1');
