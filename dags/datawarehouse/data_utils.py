from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

table = "yt_api"

def get_conn_cursor():
    hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt", database="elt_db")
    conn = hook.get_conn()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    return cursor, conn

def close_conn_cursor(cursor, conn):
    cursor.close()
    conn.close()

def create_schema(schema):
    cur, conn = get_conn_cursor()
    
    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"
    
    cur.execute(schema_sql)
    
    conn.commit()
    
    close_conn_cursor(cur, conn)

def create_table(schema):
    cur, conn = get_conn_cursor()
    
    if schema == 'staging':
        table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                "video_id" VARCHAR(11) PRIMARY KEY,
                "video_title" TEXT NOT NULL,
                "upload_date" TIMESTAMP NOT NULL,
                "duration" VARCHAR(20) NOT NULL,
                "video_views" INT,
                "likes_count" INT,
                "comments_count" INT
            );
        """
    else:
        table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                "video_id" VARCHAR(11) PRIMARY KEY,
                "video_title" TEXT NOT NULL,
                "upload_date" TIMESTAMP NOT NULL,
                "duration" TIME NOT NULL,
                "video_type" VARCHAR(10) NOT NULL,
                "video_views" INT,
                "likes_count" INT,
                "comments_count" INT
            );
        """
    
    cur.execute(table_sql)
    conn.commit()
    close_conn_cursor(cur, conn)

def get_video_ids(cur, schema):
    cur.execute(f"""SELECT "video_id" FROM {schema}.{table}""")
    id = cur.fetchall()
    
    video_ids = [row["video_id"] for row in id]
    
    return video_ids