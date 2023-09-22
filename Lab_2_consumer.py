import click
import random
import signal
import io
import json
import fastavro
from fastavro.schema import parse_schema
from fastavro.schema import load_schema
from fastavro.schema import extract_record_type

from avro.io import BinaryDecoder, DatumReader
from confluent_kafka import Consumer, KafkaError

# Συνάρτηση για τη διαχείριση σήματος τερματισμού
def signal_handler(sig, frame):
    print('EXITING SAFELY!')
    exit(0)

# Ορισμός σήματος τερματισμού
signal.signal(signal.SIGTERM, signal_handler)

# Δημιουργία καταναλωτή Kafka
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

# Εντολή για τη διαχείριση του CLI με το Click
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
            avro_message_dict = json.loads(avro_message)
            avro_schema = avro_message_dict.get('schema')
            avro_payload = avro_message_dict.get('payload')

            if avro_schema and avro_payload:
                schema = parse_schema(avro_schema)
                record_type = extract_record_type(schema)
                reader = fastavro.schemaless_reader(io.BytesIO(avro_payload), schema, record_type=record_type)
                print(reader)
            else:
                print("Invalid Avro message format.")
        except Exception as e:
            print(f"Error decoding Avro message: {e}")

# Εκτέλεση της εφαρμογής καταναλωτή
consume()
