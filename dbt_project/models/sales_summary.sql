SELECT
    order_id,
    customer_name,
    product,
    quantity,
    price,
    quantity * price AS total_amount
FROM read_csv_auto('/opt/airflow/data/sales.csv')