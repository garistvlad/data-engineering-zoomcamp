import json

from kafka import KafkaConsumer

KAFKA_TOPIC = "UserTracker"


def main():
    # init consumer:
    consumer = KafkaConsumer(KAFKA_TOPIC)

    # read all messages:
    for msg in consumer:
        if msg is None:
            continue

        payload = json.loads(msg.value.decode("utf-8"))
        payload["meta_data"] = {
            "topic": msg.topic,
            "partition": msg.partition,
            "offset": msg.offset,
            "timestamp": msg.timestamp,
            "timestamp_type": msg.timestamp_type,
            "key": msg.key,
        }
        print(payload)
        print("-" * 10)


if __name__ == "__main__":
    main()
