import click
from confluent_kafka import Consumer, KafkaError
import random
import signal
import avro.schema
import io
import fastavro

def signal_handler(sig, frame):
    print('EXITING SAFELY!')
    exit(0)

signal.signal(signal.SIGTERM, signal_handler)

c = Consumer({
    'bootstrap.servers': '13.49.128.80:19093,13.49.128.80:29093,13.49.128.80:39093',
    'group.id': f"{random.random()}",
    'auto.offset.reset': 'latest',
    'security.protocol': 'SSL',
    'ssl.ca.location': './auth/ca.crt',
    'ssl.keystore.location': './auth/kafka.keystore.pkcs12',
    'ssl.keystore.password': 'cc2023',
    'enable.auto.commit': 'true',
    'ssl.endpoint.identification.algorithm': 'none',
})

@click.command()
@click.argument('topic')
def consume(topic: str):
    c.subscribe([topic], on_assign=lambda _, p_list: print(p_list))

    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        avro_message = msg.value()
        
       
        try:
            decoded_message = fastavro.schemaless_reader(io.BytesIO(avro_message))
            record_name = decoded_message['record_name']
            data = decoded_message['deserialized_message']
            print(record_name)
            print(data)
        except fastavro.error.FastavroError as e:
            print(f"Fastavro error: {e}")
        except Exception as e:
            print(f"Error: {e}")

consume()
