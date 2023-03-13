import json
import logging
import random
import time

from faker import Faker
from kafka import KafkaProducer

KAFKA_TOPIC = "UserTracker"


def setup_logger(name: str) -> logging.Logger:
    """Setup producer logger to writer to producer.org"""
    logging.basicConfig(
        format="%(asctime)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        filename="producer.log",
        filemode="w"
    )
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    return logger


logger = setup_logger("kafka-basics-producer")


def main():
    # init
    fake = Faker()
    producer = KafkaProducer(bootstrap_servers="localhost:9092")

    # start messaging:
    for i in range(20):
        data = {
            "user_id": fake.random_int(min=20_000, max=100_000),
            "user_name": fake.name(),
            "user_address": fake.street_address() + " | " + fake.city(),
            "platform": random.choice(['Mobile', 'Laptop', 'Tablet']),
            "signup_at": str(fake.date_time_this_month())
        }
        payload = json.dumps(data).encode("utf-8")
        producer.send(KAFKA_TOPIC, payload)

        logger.info(f"Produces message inside topic `{KAFKA_TOPIC}` with value `{payload.decode('utf-8')}`")

        time.sleep(3)


if __name__ == "__main__":
    main()
