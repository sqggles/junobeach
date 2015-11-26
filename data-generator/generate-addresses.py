from faker import Factory
import dexml
from dexml import fields
from kafka import SimpleProducer, KafkaClient
import time
import random

num_messages = 10

class Address(dexml.Model):
    street = fields.String(tagname="street")
    city = fields.String(tagname="city")
    state = fields.String(tagname="state")
    zip = fields.String(tagname="zip")


class RandomXMLAddressProducer:
    #universal fake factory for producer class
    fake = Factory.create('en_US')
    def __init__():
        pass
    @classmethod
    def create_address(cls, fake_factory):
        addr = fake_factory.address()
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
        def push_addresses_to_kafka(cls, producer, topic, min_delay=0, max_delay=50, max_messages=None):
            ct = 0
            while True:
                if max_messages and ct == max_messages:
                    break
                try:
                    addr = create_address()
                except:
                    #not going to worry for fake data
                    continue
                producer.send_messages(topic, create_address())
                #hackitude: sleep some milliseconds in range
                time.sleep(random.randrange(min_delay, max_delay)/1000.0)
                ct += 1

def get_args():
    parser = argparse.ArgumentParser(
        description='produce random US addresses in XML format and put them on a kafka topic')
    # Add arguments
    parser.add_argument(
        '-z', '--zookeeper', type=str, help='Kafka Zookeeper host:port', required=True)
    parser.add_argument(
        '-t', '--topic', type=str, help='Kafka Topic', required=True)
    parser.add_argument(
        '-m', '--mindelay', type=int, help='Minimum delay between messages in ms, def: 0', required=False, default=0)
    parser.add_argument(
        '-M', '--maxdelay', type=int, help='Maximum delay between messages in ms, def: 50', required=False, default=50)
    parser.add_argument(
        '-l', '--limit', type=int, help='Maximum number of messages to send, def: Inf', required=False, default=None)
    args = parser.parse_args()
    return args.zookeeper, args.topic, args.min_delay, args.max_delay, args.limit


if __name__ == '__main__':
    (zookeeper, topic, min_delay, max_delay, limit) = get_args()
    kafka = KafkaClient(zookeeper)
    producer = SimpleProducer(kafka)
    RandomXMLAddressProducer.push_addresses_to_kafka(producer, topic, min_delay, max_delay, limit)

