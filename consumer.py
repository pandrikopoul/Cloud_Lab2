import click
import random
import signal
import io
import fastavro
from confluent_kafka import Consumer, KafkaError
from enum import Enum
import asyncio
import grpc
#from generated_code import notification_pb2
#from generated_code import notification_pb2_grpc
server_address = 'localhost:50051'  # Replace with the actual server address and port
experiment_k = 0

# gia na steilw thn thermokrasia sthn vash epidi ena pirama exei polous sensores egw prepei na steilw to average apo olous tous sensores tou piramatos
async def send_notification(stub,notificatio_type,researcher,measurment_id,experiment_id,cipher_data):
    await stub.SendNotification(
        NotifierRequest(
            notification_type=notificatio_type,
            researcher=researcher,
            measurement_id=measurment_id,
            experiment_id=experiment_id,
            cipher_data=cipher_data,
        )
    )

class notifcation_type(Enum):
    Stabilised = 'Stabilized'
    out_of_range = 'OutOfRange'


experiment_dict = {}
#stabilization_flag= False
#out_of_rng=False
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
    experiment_k =0

    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        avro_message = msg.value()
        try:

            reader = fastavro.reader(io.BytesIO(avro_message))
            for decoded_message in reader:
                #print("Another message ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                
                
                #print(decoded_message)
                
                if msg.headers()[0][1] == b'experiment_configured': # store the values related to the configuration of the experiment in to a dictionary
                    
                    print(decoded_message['experiment'])
                    print(decoded_message['researcher'])
                    print(decoded_message['sensors'])
                    print(decoded_message['temperature_range'])
                    experiment_k +=1 #str(decoded_message['experiment'])
                    experiment_dict[experiment_k]['experiment_id'] = decoded_message['experiment']
                    experiment_dict[experiment_k]['out_of_rng'] = False
                    experiment_dict[experiment_k]['stabilization_flag'] = False
                    experiment_dict[experiment_k]['avg_temp'] = 0
                    experiment_dict[experiment_k]['sensor_counter'] = 0
                    experiment_dict[experiment_k] ['researcher'] = decoded_message['researcher']
                    experiment_dict[experiment_k] ['sensors'] = decoded_message['sensors']
                    data_dict[experiment_k] ['temperature_range'] = decoded_message['temperature_range']
                if msg.headers()[0][1] == b'stabilization_started':
                
                    experiment_dict[experiment_k]['stabilization_flag'] = True

                if msg.headers()[0][1] == b'sensor_temperature_measured' and experiment_dict[experiment_k]['stabilization_flag']==True:
                    
                    experiment_dict[experiment_k]['sensor_counter']+=1
                    experiment_dict[experiment_k]['avg_temp']+=decoded_message['temperature']
                    if experiment_dict[experiment_k]['sensor_counter'] == len(experiment_dict[experiment_k]['sensors']):
                        experiment_dict[experiment_k]['avg_temp'] = experiment_dict[experiment_k]['avg_temp']/len(experiment_dict[experiment_k]['sensors'])

                    if experiment_dict[experiment_k]['avg_temp'] <= experiment_dict[experiment_k]['temperature_range']['upper_threshold'] and experiment_dict[experiment_k]['avg_temp'] >= experiment_dict[experiment_k]['temperature_range']['lower_threshold'] and experiment_dict[experiment_k]['senor_counter']==len(experiment_dict[experiment_k]['sensors']):
                        #send notification
                        print('-----------------------------------The temperature is stabilised. Send notification.-----------------------------------------------')
                        #print(notifcation_type.Stabilised,experiment_dict[experiment_k]['researcher'],decoded_message['measurement_id'],experiment_dict[experiment_k]['experiment_id'],decoded_message['measurement_hash'])
                        # async with grpc.aio.insecure_channel(server_address) as channel:
                        #     stub = NotifierServiceStub(channel)
                        #
                        #     tasks = [asyncio.create_task(send_notification(stub,notifcation_type.Stabilised,experiment_dict[experiment_k]['researcher'],decoded_message['measurment_id'],experiment_dict[experiment_k]['experiment_id'],decoded_message['measurement_hash']))]
                        #
                        #     await asyncio.gather(*tasks)
                        experiment_dict[experiment_k]['stabilization_flag']=False
                        experiment_dict[experiment_k]['sensor_counter'] = 0
                        experiment_dict[experiment_k]['avg_temp'] = 0

                elif msg.headers()[0][1] == b'sensor_temperature_measured' and experiment_dict[experiment_k]['stabilization_flag']==False :
                    
                    experiment_dict[experiment_k]['sensor_counter'] += 1
                    experiment_dict[experiment_k]['avg_temp'] += decoded_message['temperature']
                    if experiment_dict[experiment_k]['sensor_counter'] == len(experiment_dict[experiment_k]['sensors']):
                        experiment_dict[experiment_k]['avg_temp'] = experiment_dict[experiment_k]['avg_temp'] / len(
                            experiment_dict[experiment_k]['sensors'])

                    if experiment_dict[experiment_k]['sensor_counter'] == len(experiment_dict[experiment_k]['sensors']):
                        #add temperature
                        print('-----------------------------------Add tepmerature to the database.-----------------------------------------------')
                        #print(experiment_dict[experiment_k]['experiment_id'],experiment_dict['experiment_k']['temperature_range'],experiment_dict[experiment_k]['avg_temp'],decoded_message['timestamp'])
                        #print(add_temperature(experiment_dict[experiment_k]['experiment_id'],experiment_dict['experiment_k']['temperature_range'],experiment_dict[experiment_k]['avg_temp'],decoded_message['timestamp']))


                        if experiment_dict[experiment_k]['out_of_rng']==True and (decoded_message['temperature'] <= decoded_message['temperature_range']['upper_threshold'] and decoded_message['temperature'] >= decoded_message['temperature_range']['lower_threshold']) :
                            #send notification
                            print(
                                '-----------------------------------The temperature was out of range but is stabilised Again . Send notification.-----------------------------------------------')
                            #print(notifcation_type.Stabilised, experiment_dict[experiment_k]['researcher'],
                             #     decoded_message['measurement_id'], experiment_dict[experiment_k]['experiment_id'],
                              #    decoded_message['measurement_hash'])
                        
                            
                            # async with grpc.aio.insecure_channel(server_address) as channel:
                            #     stub = NotifierServiceStub(channel)
                            # 
                            #     tasks = [asyncio.create_task(send_notification(stub, notifcation_type.Stabilised,experiment_dict[experiment_k]['researcher'],decoded_message['measurment_id'],experiment_dict[experiment_k]['experiment_id'],decoded_message['measurement_hash']))]
                            # 
                            #     await asyncio.gather(*tasks)

                            experiment_dict[experiment_k]['out_of_rng']=False


                        if  experiment_dict[experiment_k]['out_of_rng'] == False and not (decoded_message['temperature'] <= decoded_message['temperature_range']['upper_threshold'] and decoded_message['temperature'] >= decoded_message['temperature_range']['lower_threshold']):
                            print('-----------------------------------The temperature is out of range. Send notification.-----------------------------------------------')
                            #print(notifcation_type.out_of_range, experiment_dict[experiment_k]['researcher'],
                             #     decoded_message['measurement_id'], experiment_dict[experiment_k]['experiment_id'],
                              #    decoded_message['measurement_hash'])
                            
                            # async with grpc.aio.insecure_channel(server_address) as channel:
                            #     stub = NotifierServiceStub(channel)
                            # 
                            #     tasks = [asyncio.create_task(send_notification(stub, notifcation_type.out_of_range,experiment_dict[experiment_k]['researcher'],decoded_message['measurment_id'],experiment_dict[experiment_k]['experiment_id'],decoded_message['measurement_hash']))]
                            # 
                            #     await asyncio.gather(*tasks)
                            experiment_dict[experiment_k]['out_of_rng']=True
                    experiment_dict[experiment_k]['sensor_counter'] = 0
                    experiment_dict[experiment_k]['avg_temp'] = 0

                #print(decoded_message)
        except Exception as e:
            print(f"Error decoding Avro message: {e}")


if __name__ == "__main__":
    consume()
