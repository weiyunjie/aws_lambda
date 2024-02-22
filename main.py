from __future__ import print_function
from kafka import KafkaProducer
import json


def lambda_handler(event, context):
    for record in event['Records']:
        record = {'data': record, 'createTime': None, 'clientIp': None}
        data = json.dumps(record, ensure_ascii=False)
        producer(data)


def producer(msg):
    producer = KafkaProducer(bootstrap_servers=["192.168.1.1:9092", "192.168.1.2:9092", "192.168.1.3:9092"])
    producer.send('test', msg.encode(), partition=0)
    producer.close()


if __name__ == '__main__':
    lambda_handler('', '')
