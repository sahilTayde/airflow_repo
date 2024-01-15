from airflow import DAG , Dataset
from datetime import datetime
from airflow.decorators import task

myfile = Dataset("/tmp/landing/file_*.txt.done")

with DAG(

    dag_id = "producer_dag" ,
    catchup = False,
    start_date = datetime(2024,1,12),
    schedule = "@daily"
) as dag:
    @task(outlets=[myfile])
    def update_file():
        with open (myfile.uri,"a+") as f:
            f.write ("producer update")
    update_file()
