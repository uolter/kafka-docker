from kafka import KafkaProducer
from kafka.errors import KafkaError

HOST = '0.0.0.0'
PORT = 9092
TOPIC = 'test'

producer = KafkaProducer(bootstrap_servers=['{host}:{port}'.format(
    host=HOST, port=PORT)])

# produce keyed messages to enable hashed partitioning
future = producer.send(TOPIC, key=b'foo', value=b'bar')

try:
    record_metadata = future.get(timeout=10)
except KafkaError as e:
    # Decide what to do if produce request failed...
    # log.exception()
    print("Error ", e)
    pass

# Successful result returns assigned partition and offset
print ("Toic ", record_metadata.topic)
print ("Partition ", record_metadata.partition)
print ("Offset", record_metadata.offset)

# encode objects via msgpack
# producer = KafkaProducer(value_serializer=msgpack.dumps)
# producer.send(TOPIC, {'key': 'value'})

# produce json messages
# producer = KafkaProducer(bootstrap_servers=['{host}:{port}'.format(
#    host=HOST, port=PORT)], 
#    value_serializer=lambda m: json.dumps(m).encode('ascii'))


#producer.send(TOPIC, {'key': 'value'})

# produce asynchronously
#for _ in range(100):
#    producer.send('my-topic', b'msg')

# block until all async messages are sent
producer.flush()

# configure multiple retries
producer = KafkaProducer(retries=5)
