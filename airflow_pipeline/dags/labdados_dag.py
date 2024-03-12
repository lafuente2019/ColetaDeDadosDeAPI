import sys
sys.path.append("airflow_pipeline")

from airflow.models import DAG
from datetime import datetime, timedelta
from operators.labdados_operator import labdadosOperator
from os.path import join   

with DAG(dag_id = "labdados_extract", start_date=datetime.now()) as dag:

    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"
    end_time = datetime.now().strftime(TIMESTAMP_FORMAT)
    start_time = (datetime.now() + timedelta(-1)).date().strftime(TIMESTAMP_FORMAT)
    query = "datascience"

    to = labdadosOperator(file_path=join("datalake/labdados_datascience",
        "extract_data={{ ds }}",
        f"datascience_{{ ds_nodash }}.json"),
        query=query, start_time=start_time, end_time=end_time, task_id="test_run")
    


    