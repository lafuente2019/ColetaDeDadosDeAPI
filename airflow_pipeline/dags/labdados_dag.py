import sys
sys.path.append("airflow_pipeline")

from airflow.models import DAG
from datetime import datetime, timedelta
from operators.labdados_operator import labdadosOperator
from os.path import join   
from airflow.utils.dates import days_ago
        
with DAG(dag_id = "LabDadosExtract", start_date=days_ago(2), schedule_interval="@daily") as dag:

    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"
    query = "datascience"

    to = labdadosOperator(file_path=join("datalake/labdados_datascience",
        "extract_data={{ ds }}",
        "datascience_{{ ds_nodash }}.json"),
        query=query, 
        start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}", 
        end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}", 
        task_id="labdados_datascience")  
