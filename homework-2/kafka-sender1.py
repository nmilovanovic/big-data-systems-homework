from time import sleep
from json import dumps
from kafka import KafkaProducer
from kafka.errors import KafkaError
import os

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'),api_version=(0,1,0))

import glob
for filename in glob.glob('/home/aleksandra.stojnev/apps/nikola.milovanovic/big-data-projects/spark-project/dataset/*.csv'):
    with open(os.path.join("/home/aleksandra.stojnev/apps/nikola.milovanovic/big-data-projects/spark-project/dataset/", filename), 'r') as fp:
        for cnt, line in enumerate(fp):
            future = producer.send('test', value=line)
            producer.flush()
            try:
                record_metadata = future.get(timeout=10)
            except KafkaError:
                # Decide what to do if produce request failed...
                print("Exception")
            pass
            print(line)
            #sleep(1)
    sleep(10)
    