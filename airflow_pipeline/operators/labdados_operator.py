import sys
sys.path.append("airflow_pipeline")

from airflow.models import BaseOperator, DAG, TaskInstance
from hook.labdados_hook import labdados_hook
import json
from datetime import datetime, timedelta
from os.path import join
from pathlib import Path


class labdadosOperator(BaseOperator):

    def __init__(self, file_path, end_time, start_time, query, **Kwargs):
        self.file_path = file_path
        self.end_time = end_time
        self.start_time = start_time
        self.query = query
        super().__init__(**Kwargs)

    def create_parent_folder(self):
        (Path(self.file_path).parent).mkdir(parents=True, exist_ok=True)

    def execute(self, context):

        end_time = self.end_time
        start_time =  self.start_time
        query = self.query

        self.create_parent_folder()

        with open(self.file_path, "w") as output_file:
            for pg in labdados_hook(end_time, start_time, query).run():
                json.dump(pg, output_file, ensure_ascii=False)
                output_file.write("\n")

if __name__ == "__main__":

   #montando url
   TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

   end_time = datetime.now().strftime(TIMESTAMP_FORMAT)
   start_time = (datetime.now() +      timedelta(-1)).date().strftime(TIMESTAMP_FORMAT)   
   query = "datascience"


   with DAG(dag_id = "labdadosTest", start_date=datetime.now()) as dag:
       
       lo = labdadosOperator(file_path=join("datalake/twitter_datascience",
                            f"extract_date={datetime.now().date()}",
                            f"datascience_{datetime.now().date().strftime('%Y%m%d')}.json"),
                            query=query, start_time=start_time, end_time=end_time, task_id="test_run")
       ti = TaskInstance(task=lo)
       lo.execute(ti.task_id)