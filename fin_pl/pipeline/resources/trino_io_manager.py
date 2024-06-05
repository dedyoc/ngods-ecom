from dagster import IOManager
from trino.dbapi import connect
from typing import Any, Dict
import pandas as pd

class TrinoTransformIOManager(IOManager):
    def __init__(self):
        pass

    def _get_conn(self):
        return connect(
            host='localhost', 
            port=8060,  
            user='trino', 
        )

    def handle_output(self, context, obj):
        select_query, partition_by = obj
        conn = self._get_conn()
        cur = conn.cursor()
        output_table_name = context.name

        # Drop table if exists
        drop_table_query = f"DROP TABLE IF EXISTS {output_table_name}"
        context.log.info(f"Dropping table if exists: {output_table_name}")
        cur.execute(drop_table_query)

        if partition_by:
            transform_query = f"""
                CREATE TABLE {output_table_name}  
                    WITH (
                        format = 'PARQUET',
                        partitioning = ARRAY['{partition_by}']
                    )
                AS {select_query}
            """
        else:
            transform_query = f"CREATE TABLE {output_table_name} AS {select_query}"

        context.log.info(f"Running transformation query in Trino")
        context.log.info(f"Query: {transform_query}")
        cur.execute(transform_query)

        # Get the number of rows affected
        rowcount = cur.rowcount
        context.log.info(f"Transformation query completed, {rowcount} rows affected")

        conn.commit()

    def load_input(self, context):
        return context.upstream_output.get_path().read_text()