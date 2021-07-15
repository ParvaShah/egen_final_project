import os
import requests
import json
from concurrent.futures import TimeoutError
from datetime import datetime
import time
from publisher import push_payload
from twitter_data import get_data 
from statistics import mean
from pytz import timezone

tz = timezone('EST')
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="publisher_key.json"
# GCP topic, project & subscription ids
PUB_SUB_TOPIC = "stock-price-prediction"
PUB_SUB_PROJECT = "innate-mix-317300"

timeout = 3.0

error_json = {'data': {'symbol': 'AAPL', 'companyName': 'Apple Inc', 
                'iexRealtimePrice': 149.22, 'iexRealtimeSize': 27, 
                'iexLastUpdated': 1626291836108, 'delayedPrice': None}, 'timestamp': '2021-07-14 19:43:57', 'sentiment': 0.0721875}


def main():
    past_data = []
    
    while True:
        
        if len(past_data) >= 40:
            past_data = past_data[len(past_data)-20:]

        loopVar = 40 if len(past_data) < 20 else 20
        
        for i in range(loopVar):
            print("Current Time: ",datetime.now(),i)
            sentiments = get_data()
            sentiments = mean(sentiments)
            myResponse = requests.get("https://cloud.iexapis.com/stable/stock/aapl/quote?token=sk_72e2ddec9b87466f83d346fe4d97a061")
            
            if myResponse.status_code == 200:
                print('Data added to json')
                

                past_data.append({"data" : json.loads(myResponse.content), "timestamp": datetime.strftime(datetime.now(tz),"%Y-%m-%d %H:%M:%S"),'sentiment':sentiments})
            else:
                print('getting error')
                past_data.append({"data" : error_json, "timestamp": datetime.strftime(datetime.now(tz),"%Y-%m-%d %H:%M:%S"),'sentiment':sentiments})
            
            time.sleep(60)
        
        print(f"Sending payload: {past_data}.")
        push_payload(past_data, PUB_SUB_TOPIC, PUB_SUB_PROJECT)
        


if __name__ == "__main__":
    main()