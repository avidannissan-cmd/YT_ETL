from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json
from datawarehouse.dwh import stagging_table, core_table
from datetime import datetime, timedelta
from dataquality.soda import yt_elt_data_quality
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

local_timezone = pendulum.timezone("Asia/Jerusalem")

default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "data@engineers.com",
    # "retries": 1,
    # "retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2025, 1, 1, tzinfo=local_timezone),
    # "end_date": datetime(2025, 12, 31, tzinfo=local_timezone),
}

staging_schema = 'staging'
core_schema = 'core'

# DAG 1
with DAG(
    dag_id="produce_json",
    default_args=default_args,
    description="DAG to produce JSON file with raw data",
    schedule='0 14 * * *',
    catchup=False,
) as dag_produce:
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extracted_data = extract_video_data(video_ids)
    save_json_task = save_to_json(extracted_data)

    trigger_update_db = TriggerDagRunOperator(
        task_id="trigger_update_db",
        trigger_dag_id="update_db",
    )

    playlist_id >> video_ids >> extracted_data >> save_json_task >> trigger_update_db

# DAG 2
with DAG(
    dag_id="update_db",
    default_args=default_args,
    description="DAG to process JSON file and insert data into both staging and core schemas",
    catchup=False,
    schedule=None,
) as dag_update:
    # Step 2: Load data into staging and core tables
    update_stagging_table = stagging_table()
    update_core_table = core_table()

    trigger_data_quality = TriggerDagRunOperator(
        task_id="trigger_data_quality",
        trigger_dag_id="data_quality",
    )

    # Dependencies
    update_stagging_table >> update_core_table >> trigger_data_quality
    
# DAG 3
with DAG(
    dag_id="data_quality",
    default_args=default_args,
    description="DAG to check the data quality on both layers in db",
    catchup=False,
    schedule=None,
) as dag_quality:

    # Data quality checks
    soda_validation_staging = yt_elt_data_quality('staging')
    soda_validation_core = yt_elt_data_quality('core')

    # Dependencies
    soda_validation_staging >> soda_validation_core