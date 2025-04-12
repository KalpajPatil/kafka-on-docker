import json
import traceback
from confluent_kafka import Consumer


def create_consumer():
    config = {
        "bootstrap.servers": "broker:29094",
        "group.id": "test-consumer2",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    }

    return Consumer(config)


if __name__ == "__main__":
    consumer = None
    message_list = []
    try:
        consumer = create_consumer()
        consumer.subscribe(["squares-2"])
        print("consumer starting...")
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                print("Got a message with no value")
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            msg_val = msg.value()
            raw_val = msg_val.decode('utf-8')
            try:
                with open('data.txt', 'a') as data_file:
                    data_file.write(raw_val + "\n")
            except IOError:
                print(f"Failed to write to file: {traceback.format_exc()}")
            consumer.commit(message= msg)
    except KeyboardInterrupt:
        print("Interrupted by user")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        print("in the finally block")
        if consumer is not None:
            consumer.close()
