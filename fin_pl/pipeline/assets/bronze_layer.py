import pandas as pd
from dagster import asset, Output, Definitions

@asset(
    io_manager_key="mysql_iceberg_io_manager",
    key_prefix=["bronze", "brazilian_ecommerce"],
    compute_kind="MySQL"
)
def product_category_name_translation(context) -> None:
    """
    This function defines an asset for the `product_category_name_translation` table.
    """
    pass

@asset(
    io_manager_key="mysql_iceberg_io_manager",
    key_prefix=["bronze", "brazilian_ecommerce"],
    compute_kind="MySQL"
)
def products(context) -> None:
    """
    This function defines an asset for the `products` table.
    """
    pass

@asset(
    io_manager_key="mysql_iceberg_io_manager",
    key_prefix=["bronze", "brazilian_ecommerce"],
    compute_kind="MySQL"
)
def orders(context) -> None:
    """
    This function defines an asset for the `orders` table.
    """
    pass

@asset(
    io_manager_key="mysql_iceberg_io_manager",
    key_prefix=["bronze", "brazilian_ecommerce"],
    compute_kind="MySQL"
)
def order_items(context) -> None:
    """
    This function defines an asset for the `order_items` table.
    """
    pass

@asset(
    io_manager_key="mysql_iceberg_io_manager",
    key_prefix=["bronze", "brazilian_ecommerce"],
    compute_kind="MySQL"
)
def order_payments(context) -> None:
    """
    This function defines an asset for the `order_payments` table.
    """
    pass

@asset(
    io_manager_key="mysql_iceberg_io_manager",
    key_prefix=["bronze", "brazilian_ecommerce"],
    compute_kind="MySQL"
)
def order_reviews(context) -> None:
    """
    This function defines an asset for the `order_reviews` table.
    """
    pass

@asset(
    io_manager_key="mysql_iceberg_io_manager",
    key_prefix=["bronze", "brazilian_ecommerce"],
    compute_kind="MySQL"
)
def sellers(context) -> None:
    """
    This function defines an asset for the `sellers` table.
    """
    pass

@asset(
    io_manager_key="mysql_iceberg_io_manager",
    key_prefix=["bronze", "brazilian_ecommerce"],
    compute_kind="MySQL"
)
def customers(context) -> None:
    """
    This function defines an asset for the `customers` table.
    """
    pass

@asset(
    io_manager_key="mysql_iceberg_io_manager",
    key_prefix=["bronze", "brazilian_ecommerce"],
    compute_kind="MySQL"
)
def geolocation(context) -> None:
    """
    This function defines an asset for the `geolocation` table.
    """
    pass