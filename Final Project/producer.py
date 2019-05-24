from time import sleep
from json import dumps
from kafka import KafkaProducer
import os
import pandas as pd

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

#Load Dataset
counter = 0
dataset_file_path = os.path.join(os.getcwd(), 'kindle_reviews.csv')
with open(dataset_file_path,"rt", encoding="utf-8") as f:
    for row in f:
        counter += 1
        producer.send('kindlereview', value=row)
        print(row)
        sleep(2)