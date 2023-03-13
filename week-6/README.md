## Week 6 Homework 

The artefacts related to this home assignment could be found here:
- Docker-compose with Kafka to run locally [docker-compose.yaml](./docker-compose.yaml) 
- Basic Kafka consumer implemented just for deeper understanding [consumer.py](./kafka-basics/consumer.py)
- Basic Kafka producer [producer.py](./kafka-basics/producer.py)
- Basic Streaming example with Faust library [streaming.py](./faust-streaming/streaming.py)

**HowTo run the example**

1. Start Kafka server locally
```shell
docker-compose up -d --build
```
2. Run producer
```shell
python ./kafka-basics/producer.py
```

3. Run consumer
```shell
python ./kafka-basics/consumer.py
```

4. Run streaming
```shell
cd ./faust-streaming
faust -A streaming worker -l info
```