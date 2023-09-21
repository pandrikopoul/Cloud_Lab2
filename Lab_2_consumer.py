import click
from confluent_kafka import Consumer, KafkaError
from avro.io import DatumReader, BinaryDecoder

c = Consumer({
    'bootstrap.servers': '13.49.128.80:19093',
    'group.id': 'simple_consumer',
    'auto.offset.reset': 'latest',
    'security.protocol': 'SSL',
    'ssl.ca.location': '/Cloud_Lab2/client3/ca.crt',
    'ssl.keystore.location': '/Cloud_Lab2/client3/kafka.keystore.pkcs12',
    'ssl.keystore.password': 'cc2023',
    'enable.auto.commit': 'true',
    'ssl.endpoint.identification.algorithm': 'none',
})


@click.command()
@click.argument('topic')
def consume(topic: str):
    c.subscribe([topic])

    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print('Reached end of partition')
            else:
                print('Error while polling message:', msg.error())
            continue


        decoder = BinaryDecoder(msg.value())
        reader = DatumReader()
        decoded_message = reader.read(decoder)


        record_name = decoded_message['record_name']
        data = decoded_message['deserialized_message']

        print(record_name)
        print(data)


consume()
