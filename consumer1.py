#!/usr/bin/env python3
from confluent_kafka import Consumer, KafkaError, KafkaException
import argparse
import sys
import pdb

running = True


def consume_loop(consumer, topics):
    try:
        consumer.subscribe([topics])

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                print('Received message: %s' % msg.value().decode('utf-8'))
    except KeyboardInterrupt:
        #pdb.set_trace()
        print("Stopping consume_loop...")
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--servers", default="172.20.10.189")
    parser.add_argument("-g", "--group", default="mygroup")
    parser.add_argument("-t", "--topic", default="test")
    args = parser.parse_args()
    print("args s {}, g {}, t {}".format(args.servers, args.group, args.topic))
    c = Consumer({'bootstrap.servers': args.servers, 'group.id': args.group,
              'default.topic.config': {'auto.offset.reset': 'smallest'}})
    consume_loop(c, args.topic)

if __name__ == '__main__':
    main()
