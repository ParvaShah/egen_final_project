import json
from google.cloud import pubsub_v1
from concurrent import futures

publish_futures = []

    
def get_callback(future, data):
    def callback(future):
        try:
            # Wait 60 seconds for the publish call to succeed.
            print(future.result(timeout=60))
        except futures.TimeoutError:
            print(f"Publishing {data} timed out.")

    return callback

# producer function to push a message to a topic
def push_payload(payload, topic, project):        
    publisher = pubsub_v1.PublisherClient() 
    topic_path = publisher.topic_path(project, topic)        
    data = json.dumps(payload).encode("utf-8")           
    future = publisher.publish(topic_path, data=data)
    future.add_done_callback(get_callback(future, data))
    publish_futures.append(future)
    print("Pushed message to topic.") 

    # Wait for all the publish futures to resolve before exiting.
    futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)
    print(f"Published messages with error handler to {topic}.")

