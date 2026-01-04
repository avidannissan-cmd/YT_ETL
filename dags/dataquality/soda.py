import logging
from airflow.operators.bash import BashOperator

logger = logging.getLogger(__name__)

SODA_PATH = "/opt/airflow/include/soda"
DATASOURCE = "pg_datasource"

def yt_elt_data_quality(schema):
    try:
        task = BashOperator(
            task_id=f"soda_test_{schema}",
            bash_command=f"SCHEMA={schema} soda scan -d {DATASOURCE} -c {SODA_PATH}/configuration.yml {SODA_PATH}/checks.yml || true",
        )
        return task
    except Exception as e:
        logger.error(f"Error running soda test for {schema}")
        raise e