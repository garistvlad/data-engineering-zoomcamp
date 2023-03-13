"""
HOWTO run: `faust -A streaming worker -l info`s
"""
import faust

KAFKA_TOPIC = "UserTracker"


class User(faust.Record):
    user_id: int
    user_name: str
    user_address: str
    platform: str
    signup_at: str


app = faust.App("my-streaming-app", broker='kafka://localhost:9092')
topic = app.topic(KAFKA_TOPIC, value_type=User)


@app.agent(topic)
async def process(users):  # consumer
    async for user in users:
        print("Consumed data:", user.asdict())


if __name__ == "__main__":
    app.main()
