from faker import Factory
import dexml
from dexml import fields
from kafka import SimpleProducer, KafkaClient
import time
import random
import argparse
import logging

logger = logging.getLogger(__name__)

class Address(dexml.Model):
    street = fields.String(tagname="street")
    city = fields.String(tagname="city")
    state = fields.String(tagname="state")
    zip = fields.String(tagname="zip")


class RandomXMLAddressProducer:
    #universal fake factory for producer class
    fake_factory = Factory.create('en_US')
    @classmethod
    def create_address(cls):
        addr = cls.fake_factory.address()
        (street, local) = addr.split("\n")
        lsplits = local.split()
        if len(lsplits) == 4:
            # two words in city name
            city = lsplits[0] + " " + lsplits[1]
            (state, zip) = (lsplits[2:])
        elif len(lsplits) == 3:
            #one word city name
            (city, state, zip) = lsplits
            city = city.replace(",","")
        address = Address(street=street, city=city, state=state, zip=zip)
        return address.render(fragment=True)
    @classmethod
    def produce(cls, producer, topic, min_delay=0, max_delay=50, max_messages=None):
        ct = 0
        topic = bytes(topic)
        addr = None
        while True:
            if max_messages and ct == max_messages:
                logger.debug("Shutting down")
                break
            try:
                addr = cls.create_address()
            except Exception as e:
                logger.debug(e)
                #not going to worry for fake data
                continue
            producer.send_messages(topic, bytes(addr))
            #hackitude: sleep some milliseconds in range
            sleep_time = random.randrange(min_delay, max_delay)/1000.0
            logger.debug("Sleeping %.6f" % sleep_time)
            time.sleep(sleep_time)
            ct += 1

#TODO: add support for multiple generators, tornado/multiprocessing

def get_args():
    parser = argparse.ArgumentParser(
        description="produce random US addresses in XML format and put them on\
                     a kafka topic\n" +
                    "ex: <Address><street>698 Arlin Crescent</street> \
                     <city>Violetmouth</city><state>MO</state> \
                     <zip>66549-5530</zip></Address>"
    )
    # Add arguments
    parser.add_argument(
        '-k', '--kafka', type=str, help='Kafka host:port, usually somehost:9092', required=True)
    parser.add_argument(
        '-t', '--topic', type=str, help='Kafka Topic', required=True)
    parser.add_argument(
        '-m', '--mindelay', type=int, help='Minimum delay between messages in ms, def: 0', required=False, default=0)
    parser.add_argument(
        '-M', '--maxdelay', type=int, help='Maximum delay between messages in ms, def: 50', required=False, default=50)
    parser.add_argument(
        '-l', '--limit', type=int, help='Maximum number of messages to send, def: Inf', required=False, default=None)
    parser.add_argument(
        '-v', '--verbose', help='Turn on debug logging', action="store_true")
    args = parser.parse_args()
    return args.kafka, args.topic, args.mindelay, args.maxdelay, args.limit, args.verbose


if __name__ == '__main__':
    (kafka_host, topic, min_delay, max_delay, limit, verbose) = get_args()
    print verbose
    if verbose:
        logging.basicConfig(
            format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s', 
            level=logging.DEBUG
        )
    kafka = KafkaClient(kafka_host)
    producer = SimpleProducer(kafka)
    RandomXMLAddressProducer.produce(producer, topic, min_delay, max_delay, limit)

