from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from config import config

def set_consumer_configs():
    config['group.id'] = 'temp_group'
    config['auto.offset.reset'] = 'earliest'

class Temperature(object):
    def __init__(self, city, reading, unit, timestamp):
        self.city = city
        self.reading = reading
        self.unit = unit
        self.timestamp = timestamp

schema_str = """{
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "Temperature",
    "description": "Temperature sensor reading",
    "type": "object",
    "properties": {
      "city": {
        "description": "City name",
        "type": "string"
      },
      "reading": {
        "description": "Current temperature reading",
        "type": "number"
      },
      "unit": {
        "description": "Temperature unit (C/F)",
        "type": "string"
      },
      "timestamp": {
        "description": "Time of reading in ms since epoch",
        "type": "number"
      }
    }
  }"""

def dict_to_temp(dict, ctx):
    return Temperature(dict['city'], dict['reading'], dict['unit'], dict['timestamp'])

if __name__ == '__main__':
    topic = 'temp_readings'

    json_deserializer = JSONDeserializer(schema_str, from_dict=dict_to_temp)

    set_consumer_configs()
    consumer = Consumer(config)
    consumer.subscribe([topic])

    while True:
        try:
            event = consumer.poll(1.0)
            if event is None:
                continue
            temp = json_deserializer(event.value(), 
                SerializationContext(topic, MessageField.VALUE))
            if temp is not None:
                print(f'Latest temp in {temp.city} is {temp.reading} {temp.unit}.')

        except KeyboardInterrupt:
            break

    consumer.close()
