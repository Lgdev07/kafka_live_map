from pykafka import KafkaClient
import json
from datetime import datetime
import uuid
import time

# READ COORDINATES FROM GEOJSON
with open('./data/bus3.json') as file:
  json_array = json.load(file)
  coordinates = json_array['features'][0]['geometry']['coordinates']

# GENERATE_UUID
def generate_uuid():
  return uuid.uuid4()

# KAFKA PRODUCER
client = KafkaClient(hosts="localhost:9092")
topic = client.topics['geodata_final']
producer = topic.get_sync_producer()


# CONSTRUCT MESSAGE
data = {}
data['busline'] = '00003'

def generate_checkpoint(coordinates):
  i = 0
  while i < len(coordinates):
    data['key'] = f"{data['busline']}_{str(generate_uuid())}"
    data['timestamp'] = str(datetime.utcnow())
    data['latitude'] = coordinates[i][1]
    data['longitude'] = coordinates[i][0]
    message = json.dumps(data)
    producer.produce(message.encode('ascii'))
    time.sleep(1)

    if i == len(coordinates)-1:
      i = 0
    else:
      i += 1  

generate_checkpoint(coordinates)
