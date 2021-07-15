import datetime
import requests
from airflow import DAG
from google.cloud import bigquery
from airflow.operators import python_operator
from airflow.operators.dummy_operator import DummyOperator
from pytz import timezone
import json

tz = timezone('EST')



DAG_NAME = 'Actual_Price_DAG'

big_query_client = bigquery.Client()


args = {'owner':'airflow', 'start_date' : datetime.datetime(2021,7,6)}

  
def fetch_and_push_data():
     
    myResponse = requests.get("https://cloud.iexapis.com/stable/stock/aapl/quote?token=sk_72e2ddec9b87466f83d346fe4d97a061")
    
    if myResponse.status_code == 200:
        actual_price = json.loads(myResponse.content)['latestPrice']
        timestamp = datetime.datetime.strftime(datetime.datetime.now(tz),"%Y-%m-%d %H:%M:%S")
        print(actual_price)
        print(timestamp)

        table_id = "innate-mix-317300.price_prediction.actual_price_table"

        rows_to_insert = [
        {u"Date_Time": timestamp,u"Actual_Price_IEX":  actual_price}
        ]
        
        errors = big_query_client.insert_rows_json(
            table_id, rows_to_insert, row_ids=[None] * len(rows_to_insert)
        )  # Make an API request.
        if errors == []:
            print("New rows have been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))

   

    
with DAG(dag_id=DAG_NAME, default_args=args, schedule_interval='1 * * * *',) as dag:
    
    start = DummyOperator(task_id='start')
    
    task_1 = python_operator.PythonOperator(
        task_id='Fetch_And_Push_Data',
        python_callable=fetch_and_push_data,
        dag=dag
    )

    end = DummyOperator(task_id='end')

start >> task_1  >> end

