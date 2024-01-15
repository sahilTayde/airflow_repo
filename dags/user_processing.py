from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.http_operator import SimpleHttpOperator
import json
from airflow.operators.python_operator import PythonOperator
from pandas import json_normalize
from airflow.providers.postgres.hooks.postgres import PostgresHook


def _process_user(ti):
    user = ti.xcom_pull (task_ids="extract_user")
    user=user['results'][0]
    processed_user=json_normalize(
        {
        'firstname' : user['name']['first'], 
        'lastname' : user['name']['last'] ,
        'country'   : user['location']['country'], 
        'username'  : user['login']['username'] ,
        'password'  : user['login']['password'] ,
        'email' : user['email']
        }
    )
    processed_user.to_csv("/tmp/processed_user.csv" , index=None , header = False)

def _store_user():
        hook = PostgresHook(postgres_conn_id = "postgres" )
        hook.copy_expert(
            sql="""
            COPY users from stdin WITH DELIMITER as ','    
            """ ,
            filename = "/tmp/processed_user.csv" 

        ) 
with DAG(
    'user_processing' , start_date=datetime(2024,1,12),schedule_interval='@daily',catchup=True 
) as dag :
    create_table = PostgresOperator(
        task_id = "create_table",
        postgres_conn_id = "postgres",
        sql="""
        create table if not exists users(
        firstname text not null,
        lastname text not null ,
        country text not null ,
        username text not null,
        password text not null,
        email text not null 
        )
        """


    )
    is_api_available = HttpSensor(

        task_id = "is_api_available",
        http_conn_id = "user_api",
        endpoint = "api/"
    )

    extract_user = SimpleHttpOperator(
        task_id="extract_user" ,
        http_conn_id="user_api",
        endpoint="api/",
        method="GET",
        response_filter=lambda response : json.loads(response.text),
        log_response=True
    )
    process_user = PythonOperator (
        task_id = "process_user",
        python_callable = _process_user
    )

    store_user = PythonOperator(
        task_id = "store_user" ,
        python_callable = _store_user
    )

    create_table >> is_api_available >> extract_user >> process_user >> store_user

