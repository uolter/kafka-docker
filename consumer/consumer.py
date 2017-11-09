import json
from kafka import KafkaConsumer
import time

HOST = '0.0.0.0'
PORT = 9092
TOPIC = 'clann'


# To consume latest messages and auto-commit offsets
"""
consumer = KafkaConsumer(TOPIC,
                         group_id='my-group',
                         bootstrap_servers=['{host}:{port}'.format(host=HOST,port=PORT)])
for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))
"""
# consume earliest available messages, don't commit offsets
# KafkaConsumer(auto_offset_reset='earliest', enable_auto_commit=False)

# consume json messages
for _ in range(3):
  try:
    consumer = KafkaConsumer(
      TOPIC,
      group_id='my-group',
      bootstrap_servers=['{host}:{port}'.format(host=HOST,port=PORT)],
      value_deserializer=lambda m: json.loads(m.decode('ascii')))

    for message in consumer:
      # message value and key are raw bytes -- decode if necessary!
      # e.g., for unicode: `message.value.decode('utf-8')`
      print ("%s:%d:%d: value=%s" % (message.topic, message.partition,
                                            message.offset,
                                            message.value))

    break
  except:
    print("Connection error. Try to reconnect in 10 sec.")
    time.sleep(10)




# consume msgpack
#KafkaConsumer(value_deserializer=msgpack.unpackb)

# StopIteration if no message after 1sec
#KafkaConsumer(consumer_timeout_ms=1000)

# Subscribe to a regex topic pattern
# consumer = KafkaConsumer()
# consumer.subscribe(pattern='^awesome.*')

# Use multiple consumers in parallel w/ 0.9 kafka brokers
# typically you would run each on a different server / process / CPU
# consumer1 = KafkaConsumer('my-topic',
#                          group_id='my-group',
#                          bootstrap_servers='my.server.com')
#consumer2 = KafkaConsumer('my-topic',
#                          group_id='my-group',
#                          bootstrap_servers='my.server.com')s