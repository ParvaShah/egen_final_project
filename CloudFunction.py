import os
from google.cloud import storage
import json
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError
from datetime import datetime
import pandas as pd

timeout = 3.0
storage_client = storage.Client()
bucket = storage_client.get_bucket("test_egen_pubsub_parva")
project = "innate-mix-317300"
subscription = "egen-pubsub-test-sub"




def process_payload(message):
    print(f"Received {message}.")
    blob = bucket.blob(str(datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S"))+".csv")
    data  = message.data
    tmp_data = json.loads(data)
    df = pd.DataFrame(tmp_data['data']['results'],index=[0])
    blob.upload_from_string(df.to_csv(),content_type='text/csv')
    message.ack() 

def consume_payload(data, context):
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project, subscription)
    print(f"Listening for messages on {subscription_path}..\n")
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=process_payload)
    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.                
            streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
            streaming_pull_future.cancel()







