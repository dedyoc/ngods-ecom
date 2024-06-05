# from dagster import (
#     Definitions,
#     ScheduleDefinition,
#     define_asset_job,
#     load_assets_from_package_module,
# )

# from . import assets

# daily_refresh_schedule = ScheduleDefinition(
#     job=define_asset_job(name="all_assets_job"), cron_schedule="0 0 * * *"
# )

# defs = Definitions(
#     assets=load_assets_from_package_module(assets), schedules=[daily_refresh_schedule]
# )

import os
from dagster import Definitions
from .assets import assets  # Assuming your assets are defined in assets.py
from .resources.iceberg_io_manager import IcebergIOManager
from .resources.trino_io_manager import TrinoTransformIOManager
from .resources.mysql_iceberg_io_manager import MySQLIcebergIOManager
from sqlalchemy import create_engine
MINIO_CONFIG = {
    "endpoint_url": os.getenv("MINIO_ENDPOINT") or "http://localhost:9000",
    "bucket": os.getenv("DATALAKE_BUCKET") or "lakehouse",
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID") or "minio",
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY") or "minio123",
}

TRINO_CONFIG = {
    "host": os.getenv("TRINO_HOST") or "trino",
    "port": os.getenv("TRINO_PORT") or 8060,
    "user": os.getenv("TRINO_USER") or "trino",
}

# Combine configurations for the IOManager
MINIO_ICEBERG_CONFIG = {
    **MINIO_CONFIG,  # Unpack the MINIO_CONFIG
    **TRINO_CONFIG  # Unpack the TRINO_CONFIG 
}

engine = create_engine('trino://trino@localhost:8060/iceberg/default') 
defs = Definitions(
    assets=assets,
    resources={
        "mysql_iceberg_io_manager": MySQLIcebergIOManager(MINIO_ICEBERG_CONFIG),
        "iceberg_io_manager": IcebergIOManager(MINIO_ICEBERG_CONFIG),
    },
)