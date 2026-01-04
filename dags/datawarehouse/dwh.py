from datawarehouse.data_utils import get_conn_cursor, close_conn_cursor, create_schema, create_table, get_video_ids
from datawarehouse.data_loading import load_data
from datawarehouse.data_modification import insert_rows, update_rows, delete_rows
from datawarehouse.data_transformation import transform_data

import logging
from airflow.decorators import task

logger = logging.getLogger(__name__)
table = "yt_api"  # Fixed: should be 'table' not 'tables'

@task
def stagging_table():

    schema = 'staging'  # Fixed: changed from 'stagging' to 'staging' (or keep 'stagging' if that's intentional)

    conn, cur = None, None

    try:
        cur, conn = get_conn_cursor()  # Fixed: order is cursor, conn (not conn, cur)

        YT_data = load_data()

        create_schema(schema)
        create_table(schema)

        table_ids = get_video_ids(cur, schema)

        for row in YT_data:
            if len(table_ids) == 0:
                insert_rows(cur, conn, schema, row)

            else:
                if row['video_id'] not in table_ids:
                    insert_rows(cur, conn, schema, row)
                else:
                    update_rows(cur, conn, schema, row)

        ids_in_json = {row['video_id'] for row in YT_data}
        ids_to_delete = set(table_ids) - ids_in_json

        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)

        logger.info(f"{schema} table updated completed")

    except Exception as e:
        logger.error(f"An error occurred during the update of {schema} table: {e}")
        raise e

    finally:
        if conn and cur:
            close_conn_cursor(cur, conn)  # Fixed: order is cursor, conn


@task
def core_table():
    
    schema = 'core'
    conn, cur = None, None

    try:
        cur, conn = get_conn_cursor()  # Fixed: order is cursor, conn

        create_schema(schema)
        create_table(schema)

        table_ids = get_video_ids(cur, schema)

        current_video_ids = set()

        cur.execute(f"SELECT * FROM staging.{table};")  # Fixed: use 'staging' schema (or 'stagging' if that's the actual schema name)

        rows = cur.fetchall()

        for row in rows:
            current_video_ids.add(row['video_id'])

            if len(table_ids) == 0:
                transform_row = transform_data(row)
                insert_rows(cur, conn, schema, transform_row)

            else:
                transform_row = transform_data(row)

                if transform_row['video_id'] in table_ids:
                    update_rows(cur, conn, schema, transform_row)

                else:
                    insert_rows(cur, conn, schema, transform_row)

        ids_to_delete = set(table_ids) - current_video_ids  # Fixed: was set{table_ids} (missing parentheses)

        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)

        logger.info(f"{schema} table updated completed")

    except Exception as e:
        logger.error(f"An error occurred during the update of {schema} table: {e}")
        raise e

    finally:
        if conn and cur:
            close_conn_cursor(cur, conn)  # Fixed: order is cursor, conn