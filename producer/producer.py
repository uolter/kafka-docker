import json
import sys, getopt

from kafka import KafkaProducer
from kafka.errors import KafkaError

HOST = '0.0.0.0'
PORT = 9092
TOPIC = 'clann'

def show_help(script):
    print('%s -i <inputfile>' %script)


def main(argv):

    if len(argv) < 2:
        show_help(argv[0])
        sys.exit(2)

    try:
      opts, args = getopt.getopt(argv[1:], "hi:o:",["ifile=",])
    except getopt.GetoptError:
      show_help(argv[0])
      sys.exit(2)

    data = {}
   
    for opt, arg in opts:
        if opt in ("-i", "--ifile"):
            inputfile = arg
            with open(inputfile) as file: 
                data = file.read() 
        else:
            show_help(argv[0])
            sys.exit()

    # produce json messages
    producer = KafkaProducer(bootstrap_servers=['{host}:{port}'.format(
        host=HOST, port=PORT)], 
        value_serializer=lambda m: json.dumps(m).encode('ascii'),
        retries=5)

    future = producer.send(TOPIC, json.loads(data))

    try:
        record_metadata = future.get(timeout=10)
    except KafkaError as e:
        print("Error ", e)
        pass

    print ("Topic ", record_metadata.topic)
    print ("Partition ", record_metadata.partition)
    print ("Offset", record_metadata.offset)

    # block until all async messages are sent
    producer.flush()



if __name__ == "__main__":
    main(sys.argv)
