from dagster import asset, AssetIn

@asset(
        ins={"order_items": AssetIn(key=["bronze", "brazilian_ecommerce","order_items"], input_manager_key="iceberg_io_manager"),
            "orders": AssetIn(key=["bronze", "brazilian_ecommerce","orders"], input_manager_key="iceberg_io_manager"),
            "order_payments": AssetIn(key=["bronze", "brazilian_ecommerce","order_payments"], input_manager_key="iceberg_io_manager"),
            "order_reviews": AssetIn(key=["bronze", "brazilian_ecommerce","order_reviews"], input_manager_key="iceberg_io_manager")},
        io_manager_key="iceberg_io_manager",
        key=["silver","brazilian_ecommerce","fact_sales"]
       )
def fact_sales(context, order_items, orders, order_payments, order_reviews):
    context.log.info(f"Creating fact_sales table from {order_items}, {orders}, {order_payments}, {order_reviews}")
    transform_query = f"""
SELECT 
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    oi.seller_id,
    o.customer_id,
    oi.price,
    oi.freight_value,
    op.payment_type,
    op.payment_value,
    review.review_score
FROM 
    iceberg.bronze.order_items AS oi
JOIN 
    iceberg.bronze.orders AS o ON oi.order_id = o.order_id
JOIN 
    iceberg.bronze.order_payments AS op ON oi.order_id = op.order_id
JOIN 
    iceberg.bronze.order_reviews AS review ON oi.order_id = review.order_id
    """
    return transform_query

@asset(
    ins={
        "products": AssetIn(key=["bronze", "brazilian_ecommerce", "products"], input_manager_key="iceberg_io_manager"),
        "product_category_name_translation": AssetIn(
            key=["bronze", "brazilian_ecommerce", "product_category_name_translation"], input_manager_key="iceberg_io_manager"
        ),
    },
    io_manager_key="iceberg_io_manager",
    key=["silver", "brazilian_ecommerce", "dim_products"],
)
def dim_products(context, products, product_category_name_translation):
    context.log.info(
        f"Creating dim_products table from {products}, {product_category_name_translation}"
    )
    transform_query = f"""
    SELECT 
        p.product_id,
        p.product_category_name,
        pc.product_category_name_english,
        p.product_name_length,
        p.product_description_length,
        p.product_photos_qty,
        p.product_weight_g,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm
    FROM 
        iceberg.bronze.products AS p
    JOIN 
        iceberg.bronze.product_category_name_translation AS pc ON p.product_category_name = pc.product_category_name
    """
    return transform_query


@asset(
    ins={"customers": AssetIn(key=["bronze", "brazilian_ecommerce", "customers"], input_manager_key="iceberg_io_manager")},
    io_manager_key="iceberg_io_manager",
    key=["silver", "brazilian_ecommerce", "dim_customers"],
)
def dim_customers(context, customers):
    context.log.info(f"Creating dim_customers table from {customers}")
    transform_query = f"""
    SELECT 
        *
    FROM 
        iceberg.bronze.customers
    """
    return transform_query


@asset(
    ins={"sellers": AssetIn(key=["bronze", "brazilian_ecommerce", "sellers"], input_manager_key="iceberg_io_manager")},
    io_manager_key="iceberg_io_manager",
    key=["silver", "brazilian_ecommerce", "dim_sellers"],
)
def dim_sellers(context, sellers):
    context.log.info(f"Creating dim_sellers table from {sellers}")
    transform_query = f"""
    SELECT 
        *
    FROM 
        iceberg.bronze.sellers
    """
    return transform_query


@asset(
    ins={"geolocation": AssetIn(key=["bronze", "brazilian_ecommerce", "geolocation"], input_manager_key="iceberg_io_manager")},
    io_manager_key="iceberg_io_manager",
    key=["silver", "brazilian_ecommerce", "dim_geolocation"],
)
def dim_geolocation(context, geolocation):
    context.log.info(f"Creating dim_geolocation table from {geolocation}")
    transform_query = f"""
    SELECT 
        *
    FROM 
        iceberg.bronze.geolocation
    """
    return transform_query


@asset(
    ins={"orders": AssetIn(key=["bronze", "brazilian_ecommerce", "orders"], input_manager_key="iceberg_io_manager")},
    io_manager_key="iceberg_io_manager",
    key=["silver", "brazilian_ecommerce", "dim_orders"],
)
def dim_orders(context, orders):
    context.log.info(f"Creating dim_orders table from {orders}")
    transform_query = f"""
SELECT 
    customer_id,
    CASE 
        WHEN order_approved_at = '' THEN NULL 
        ELSE CAST(order_approved_at AS TIMESTAMP(6)) 
    END AS order_approved_at, 
    CASE 
        WHEN order_delivered_carrier_date = '' THEN NULL
        ELSE CAST(order_delivered_carrier_date AS TIMESTAMP(6))
    END AS order_delivered_carrier_date,
    CASE 
        WHEN order_delivered_customer_date = '' THEN NULL
        ELSE CAST(order_delivered_customer_date AS TIMESTAMP(6))
    END AS order_delivered_customer_date,
    CASE 
        WHEN order_estimated_delivery_date = '' THEN NULL
        ELSE CAST(order_estimated_delivery_date AS TIMESTAMP(6))
    END AS order_estimated_delivery_date,
    order_id,
    CASE 
        WHEN order_purchase_timestamp = '' THEN NULL
        ELSE CAST(order_purchase_timestamp AS TIMESTAMP(6))
    END AS order_purchase_timestamp,
    order_status
FROM 
    iceberg.bronze.orders
    """
    return transform_query


@asset(
    ins={
        "order_payments": AssetIn(key=["bronze", "brazilian_ecommerce", "order_payments"], input_manager_key="iceberg_io_manager")
    },
    io_manager_key="iceberg_io_manager",
    key=["silver", "brazilian_ecommerce", "dim_payments"],
)
def dim_payments(context, order_payments):
    context.log.info(f"Creating dim_payments table from {order_payments}")
    transform_query = f"""
    SELECT 
        *
    FROM 
        iceberg.bronze.order_payments
    """
    return transform_query


@asset(
    ins={
        "order_reviews": AssetIn(key=["bronze", "brazilian_ecommerce", "order_reviews"], input_manager_key="iceberg_io_manager")
    },
    io_manager_key="iceberg_io_manager",
    key=["silver", "brazilian_ecommerce", "dim_reviews"],
)
def dim_reviews(context, order_reviews):
    context.log.info(f"Creating dim_reviews table from {order_reviews}")
    transform_query = f"""
    SELECT 
        *
    FROM 
        iceberg.bronze.order_reviews
    """
    return transform_query