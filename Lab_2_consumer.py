import click
import random
import signal
import io
import fastavro
from confluent_kafka import Consumer, KafkaError

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
            # Αποκωδικοποίηση των μηνυμάτων Avro χρησιμοποιώντας το fastavro
            reader = fastavro.reader(io.BytesIO(avro_message))
            for decoded_message in reader:
                record_name = decoded_message['record_name']
                print(f"First message record name: {record_name}")
                print(decoded_message)
        except Exception as e:
            print(f"Error decoding Avro message: {e}")

if __name__ == "__main__":
    consume()
