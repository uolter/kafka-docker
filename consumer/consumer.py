import json
import sys, getopt
from kafka import KafkaConsumer
import time

HOST = '0.0.0.0'
PORT = 9092
TOPIC = 'clann'


def show_help(script):
    print('%s -t <topic>' % script)


def main(argv):

    topic = TOPIC

    try:
        opts, args = getopt.getopt(argv[1:], "ht:", ["topic="])
    except getopt.GetoptError:
        show_help(argv[0])
        sys.exit(2)

    for opt, arg in opts:
        if opt in ("-t", "--topic"):
            topic = arg
        else:
            show_help(argv[0])
            sys.exit()

    print("Read from topic ", topic)

    for _ in range(3):
        try:
            consumer = KafkaConsumer(
                topic,
                group_id='my-group',
                bootstrap_servers=['{host}:{port}'.format(host=HOST, port=PORT)],
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


if __name__ == "__main__":
    main(sys.argv)
