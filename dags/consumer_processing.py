from airflow import DAG , Dataset
from airflow.decorators import task
from datetime import datetime 
import glob 

myfile = Dataset("/tmp/file_1.txt.done")

with DAG (
    dag_id = "consumer_processing" ,
    schedule = [myfile] ,
    start_date = datetime(2024,1,12) ,
    catchup = False
) as dag :
    @task
    def read_dataset():
        with open(myfile.uri,"r") as f :
            print(f.read())
read_dataset()