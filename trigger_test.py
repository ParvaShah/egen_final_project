import datetime
from google.cloud import storage
import pandas as pd
from io import BytesIO
import numpy as np
import h5py
import gcsfs
from airflow import DAG
from google.cloud import bigquery
from airflow.operators import python_operator
from airflow.operators.dummy_operator import DummyOperator
from sklearn.preprocessing import MinMaxScaler
from keras.models import load_model


DAG_NAME = 'prediction_dag'


storage_client = storage.Client()
bucket = storage_client.get_bucket("stock-prediction-project-bucket")
dest_bucket = storage_client.get_bucket("stock-prediction-project-bucket-old-data")
big_query_client = bigquery.Client()


args = {'owner':'airflow', 'start_date' : datetime.datetime(2021,7,6), 'schedule_interval':'None', 'provide_context': True}

def fetch_and_transform_data(**kwargs):
    fileNames = []
    blobs = bucket.list_blobs()
    
    
    for file_csv in blobs:
        if '.csv' in file_csv.name:
            # logic to transform data

            blob=bucket.get_blob(file_csv.name)
            file = blob.download_as_string()
            test_dataset = pd.read_csv(BytesIO(file),index_col=0)
            test_dataset = test_dataset.sort_index(ascending=True,axis=0)
            
            # get last timestamp
            endTime = test_dataset['Date'].iloc[-1]
            endTime = datetime.datetime.strptime(endTime, '%Y-%m-%d %H:%M:%S')
            
            
            scaler=MinMaxScaler(feature_range=(0,1))
            test_dataset.index=test_dataset.Date
            test_dataset.drop("Date",axis=1,inplace=True)
            
            scaled_test_data=scaler.fit_transform(test_dataset)

            x_test_data,y_test_data=[],[]

            for i in range(20,len(scaled_test_data)):
                x_test_data.append(scaled_test_data[i-20:i,:])
                y_test_data.append(scaled_test_data[i,0])
                
            x_test_data,y_test_data=np.array(x_test_data),np.array(y_test_data)
            X_test=np.reshape(x_test_data,(x_test_data.shape[0],x_test_data.shape[1],2))

            PROJECT_NAME = 'innate-mix-317300'
            MODEL_PATH = 'gs://stock-prediction-project-bucket/model/saved_model.h5'
            FS = gcsfs.GCSFileSystem(project=PROJECT_NAME)
            with FS.open(MODEL_PATH, 'rb') as model_file:
                model_gcs = h5py.File(model_file, 'r')
                model = load_model(model_gcs, compile=False)

            predicted_closing_price=model.predict(X_test)
            result = np.zeros(shape=(len(x_test_data), 2) )
            for i in range(len(predicted_closing_price)):
                result[i,0] = predicted_closing_price[i]
            predicted_closing_price=scaler.inverse_transform(result)[:,0]
            print(predicted_closing_price)

            dateList = [str(endTime + datetime.timedelta(0,i*60)) for i in range(1,len(predicted_closing_price)+1)]
            pred_for_df = {'DateTime':dateList,
                            'Price':predicted_closing_price.tolist()}
            
            print("Final df to store as a csv",pred_for_df)
            kwargs['ti'].xcom_push(key='dataframe_result', value=pred_for_df)






            ############################################
            bucket.copy_blob(file_csv,dest_bucket,new_name = file_csv.name)
            bucket.delete_blob(file_csv.name)
            
        else:    
            fileNames.append(file_csv.name)
    
    print('all files are',fileNames)
    
def push_data_bigquery(**kwargs):

    ti = kwargs['ti']

    # get value_1
    pulled_value = ti.xcom_pull(key='dataframe_result', task_ids='Fetch_And_Transform_Data')
    date_list = pulled_value['DateTime']
    value_list = pulled_value['Price']
    table_id = "innate-mix-317300.price_prediction.prediction_table"

    rows_to_insert = [
    {u"Date_Time": i, u"Predicted_Price": j,u"Actual_Price": j } for i,j in zip(date_list,value_list)
    ]
    
    errors = big_query_client.insert_rows_json(
        table_id, rows_to_insert, row_ids=[None] * len(rows_to_insert)
    )  # Make an API request.
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))

    
with DAG(dag_id=DAG_NAME, default_args=args) as dag:
    start = DummyOperator(task_id='start')
    task_1 = python_operator.PythonOperator(
        task_id='Fetch_And_Transform_Data',
        python_callable=fetch_and_transform_data,
        dag=dag
    )
   
    
    task_2 = python_operator.PythonOperator(
    task_id='Push_Data_To_BigQuery',
    python_callable=push_data_bigquery,
    dag=dag
    )

    end = DummyOperator(task_id='end')

start >> task_1 >> task_2  >> end