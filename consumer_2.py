import asyncio
import io
import logging
import random
import signal
import threading

import fastavro
from confluent_kafka import Consumer
from fastavro.types import AvroMessage

from forward_data import (
    NotificationType,
    notify,
    save_temperature_to_db,
)

logging.basicConfig(level=logging.WARNING)

c = Consumer(
    {
        "bootstrap.servers": "13.49.128.80:19093,13.49.128.80:29093,13.49.128.80:39093",
        "group.id": f"{random.random()}",
        "auto.offset.reset": "latest",
        "security.protocol": "SSL",
        "ssl.ca.location": "./auth/ca.crt",
        "ssl.keystore.location": "./auth/kafka.keystore.pkcs12",
        "ssl.keystore.password": "cc2023",
        "enable.auto.commit": "true",
        "ssl.endpoint.identification.algorithm": "none",
    }
)

background_loop = asyncio.new_event_loop()


def signal_handler(_sig, _frame):
    logging.debug("exiting safely")
    c.close()
    exit(0)


signal.signal(signal.SIGTERM, signal_handler)


def consume(topic: str = "client4"):
    experiment_dict = {}

    c.subscribe([topic], on_assign=lambda _, p_list: logging.debug(p_list))

    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue

        if msg.error():
            logging.debug("Consumer error: {}".format(msg.error()))
            continue

        avro_message = msg.value()
        reader = fastavro.reader(io.BytesIO(avro_message))
        message_type = msg.headers()[0][1]

        for decoded_message in reader:
            logging.debug(message_type)
            logging.debug(decoded_message["experiment"])

            if message_type == b"experiment_configured":
                experiment_configured(
                    experiment_dict=experiment_dict, decoded_message=decoded_message
                )
            elif message_type == b"stabilization_started":
                stabilization_started(
                    experiment_dict=experiment_dict, decoded_message=decoded_message
                )

            elif message_type == b"sensor_temperature_measured":
                sensor_temperature_measured(
                    experiment_dict=experiment_dict, decoded_message=decoded_message
                )

            elif message_type == b"experiment_terminated":
                experiment_terminated(
                    experiment_dict=experiment_dict, decoded_message=decoded_message
                )

            logging.debug(experiment_dict)


def experiment_configured(experiment_dict: dict, decoded_message: AvroMessage):
    experiment_id = decoded_message["experiment"]
    researcher = decoded_message["researcher"]
    sensors = decoded_message["sensors"]
    # sometimes these two values are switched
    lower_threshold = decoded_message["temperature_range"]["lower_threshold"]
    upper_threshold = decoded_message["temperature_range"]["upper_threshold"]

    experiment_dict[experiment_id] = {
        "out_of_range": False,
        "stabilizing": False,
        "sensor_count": len(sensors),
        "temperatures": [],
        "researcher": researcher,
        "temperature_range": [
            min(lower_threshold, upper_threshold),
            max(lower_threshold, upper_threshold),
        ],
    }
    logging.debug(f"{experiment_id} configured: {experiment_dict[experiment_id]}")


def stabilization_started(experiment_dict: dict, decoded_message: AvroMessage):
    experiment_id = decoded_message["experiment"]

    experiment_dict[experiment_id]["stabilizing"] = True
    logging.debug(f"{experiment_id} stabilization started")


def sensor_temperature_measured(experiment_dict: dict, decoded_message: AvroMessage):
    experiment_id = decoded_message["experiment"]
    temperature = decoded_message["temperature"]
    timestamp = decoded_message["timestamp"]

    experiment_dict[experiment_id]["temperatures"].append(temperature)
    if not _all_sensors_have_measured_temperature(experiment_dict, experiment_id):
        return

    mean_temperature = (
        sum(experiment_dict[experiment_id]["temperatures"])
        / experiment_dict[experiment_id]["sensor_count"]
    )
    experiment_dict[experiment_id]["temperatures"] = []
    logging.debug(f"{experiment_id} mean temperature: {mean_temperature}")

    if experiment_dict[experiment_id]["stabilizing"]:
        _check_for_stabilization(
            experiment_dict=experiment_dict,
            experiment_id=experiment_id,
            temperature=mean_temperature,
            measurement_id=decoded_message["measurement_id"],
            measurement_hash=decoded_message["measurement_hash"],
        )
    else:
        _check_for_out_of_range(
            experiment_dict=experiment_dict,
            experiment_id=experiment_id,
            temperature=mean_temperature,
            measurement_id=decoded_message["measurement_id"],
            measurement_hash=decoded_message["measurement_hash"],
        )
        save_temperature_to_db(
            experiment_id=experiment_id,
            out_of_bounds=experiment_dict[experiment_id]["out_of_range"],
            timestamp=timestamp,
            temperature=mean_temperature,
            background_loop=background_loop,
        )


def experiment_terminated(experiment_dict: dict, decoded_message: AvroMessage):
    experiment_id = decoded_message["experiment"]
    del experiment_dict[experiment_id]
    logging.debug(f"{experiment_id} terminated")


def _all_sensors_have_measured_temperature(
    experiment_dict: dict, experiment_id: str
) -> bool:
    return (
        len(experiment_dict[experiment_id]["temperatures"])
        == experiment_dict[experiment_id]["sensor_count"]
    )


def _check_for_stabilization(
    experiment_dict: dict,
    experiment_id: str,
    temperature: float,
    measurement_id: str,
    measurement_hash: str,
):
    if (
        experiment_dict[experiment_id]["temperature_range"][0]
        <= temperature
        <= experiment_dict[experiment_id]["temperature_range"][1]
    ):
        notify(
            notification_type=NotificationType.STABLE,
            researcher=experiment_dict[experiment_id]["researcher"],
            measurement_id=measurement_id,
            experiment_id=experiment_id,
            cipher_data=measurement_hash,
            background_loop=background_loop,
        )
        experiment_dict[experiment_id]["stabilizing"] = False


def _check_for_out_of_range(
    experiment_dict: dict,
    experiment_id: str,
    temperature: float,
    measurement_id: str,
    measurement_hash: str,
):
    if experiment_dict[experiment_id]["out_of_range"]:
        if (
            experiment_dict[experiment_id]["temperature_range"][0]
            <= temperature
            <= experiment_dict[experiment_id]["temperature_range"][1]
        ):
            experiment_dict[experiment_id]["out_of_range"] = False
            logging.debug("back in range")

        return

    if (
        temperature < experiment_dict[experiment_id]["temperature_range"][0]
        or temperature > experiment_dict[experiment_id]["temperature_range"][1]
    ):
        notify(
            notification_type=NotificationType.OUT_OF_RANGE,
            researcher=experiment_dict[experiment_id]["researcher"],
            measurement_id=measurement_id,
            experiment_id=experiment_id,
            cipher_data=measurement_hash,
            background_loop=background_loop,
        )
        experiment_dict[experiment_id]["out_of_range"] = True


def start_background_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


if __name__ == "__main__":
    t = threading.Thread(target=start_background_loop, args=(background_loop,))
    t.start()

    consume()