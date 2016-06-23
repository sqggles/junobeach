from faker import Factory
import dexml
from dexml import fields
from kafka import SimpleProducer, KafkaClient
import time
import random
import argparse
import logging
import json

LOG_FMT='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s'
logger = logging.getLogger(__name__)

class Rx(dexml.Model):
    npi = fields.Integer(tagname = "npi")
    date_received = fields.Integer(tagname = "date_received")
    svcbr_prescription_id = fields.Integer(tagname = "svcbr_prescription_id") 
    drug_name = fields.String(tagname="drug_name")
    patient_id = fields.Integer(tagname="patient_id")
    prescription_id = fields.Integer(tagname="prescription_id")
    status= fields.String(tagname="status_text")
    svcbr_id = fields.Integer(tagname="svcbr_id")

# prescription staging table

fake_factory = Factory.create('en_US')
class RandomRecordProducer:
    #universal fake factory for producer class
    def create_record(self):
        npi = fake_factory.ean8()
        date_received = fake_factory.unix_time()
        rx_id = fake_factory.ean8()
        drug_name = "Drug"
        pt_id = fake_factory.ean8()
        status = "TEST STATUS"
        svc_id = fake_factory.ean8()
        t = Rx(npi=npi, date_received=date_received, svcbr_prescription_id=rx_id, drug_name=drug_name, patient_id=pt_id, prescription_id=rx_id, status=status, svc_id=svc_id)
        return t
    def produce(self, producer, topic, min_delay=0, max_delay=50, max_messages=None, pr=False):
        ct = 0
        topic = bytes(topic)
        while True:
            if max_messages and ct >= max_messages:
                logger.debug("Shutting down")
                break
            try:
                addr = self.create_record()
                if pr:
                    print json.dumps(addr.__dict__)
                producer.send_messages(topic, bytes(addr.__dict__))
            except Exception as e:
                logger.debug(e)
                #not going to worry for fake data
                continue
            if ct % 10000 == 0:
                logger.info("Produced %d messages. Last record: %s" % (ct, addr))
            #hackitude: sleep some milliseconds in range
            sleep_time = random.randrange(min_delay, max_delay)/1000.0
            logger.debug("Sleeping %.6f" % sleep_time)
            time.sleep(sleep_time)
            ct += 1

#TODO: add support for multiple generators, tornado/multiprocessing

def get_args():
    parser = argparse.ArgumentParser(
        "generate records according to given cassandra schema"
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
    parser.add_argument(
        '-p', '--printed', help='show messages on stdout', action="store_true")
    args = parser.parse_args()
    return args.kafka, args.topic, args.mindelay, args.maxdelay, args.limit, args.verbose, args.printed


if __name__ == '__main__':
    (kafka_host, topic, min_delay, max_delay, limit, verbose, pr) = get_args()
    if verbose:
        logging.basicConfig(
            format=LOG_FMT,
            level=logging.DEBUG
        )
    else:
        logging.basicConfig(
            format=LOG_FMT,
            level=logging.INFO
        )

    kafka = KafkaClient(kafka_host)
    producer = SimpleProducer(kafka)
    rp = RandomRecordProducer()
    rp.produce(producer, topic, min_delay, max_delay, limit, pr)

