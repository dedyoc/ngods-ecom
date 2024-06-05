from dagster import asset, AssetIn 
@asset(
    ins={"fact_sales": AssetIn(key=["silver", "brazilian_ecommerce", "fact_sales"], input_manager_key="iceberg_io_manager")},
    io_manager_key="iceberg_io_manager",
    key=["gold", "brazilian_ecommerce", "order_frequency_monthly"]
)
def order_frequency_monthly(context, fact_sales, dim_orders):
    context.log.info("Creating gold layer table 'order_frequency_monthly'")
    transform_query = f"""
    SELECT 
        year(o.order_delivered_customer_date) AS year, 
        month(o.order_delivered_customer_date) AS month,
        COUNT(*) AS order_frequency
    FROM 
        iceberg.silver.fact_sales AS f
    JOIN 
        iceberg.silver.dim_orders AS o ON f.order_id = o.order_id
    WHERE 
        o.order_delivered_customer_date IS NOT NULL
    GROUP BY 
        year(o.order_delivered_customer_date), 
        month(o.order_delivered_customer_date)  
    ORDER BY 
        year(o.order_delivered_customer_date),
        month(o.order_delivered_customer_date)
    """
    return transform_query