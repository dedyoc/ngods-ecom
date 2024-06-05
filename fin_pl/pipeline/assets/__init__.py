from .bronze_layer import (
        products,
        orders,
        order_items,
        order_payments,
        order_reviews,
        product_category_name_translation,
        sellers,
        customers,
        geolocation        
)
from .silver_layer import fact_sales, dim_customers, dim_geolocation, dim_orders, dim_products, dim_sellers
from .gold_layer import order_frequency_monthly
assets =  (orders, products, order_items, order_payments, order_reviews, product_category_name_translation) + \
        (sellers, customers, geolocation) + \
        (fact_sales, dim_customers, dim_geolocation, dim_orders, dim_products, dim_sellers) + \
        (order_frequency_monthly,)
