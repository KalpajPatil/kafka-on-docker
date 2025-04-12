import json

from confluent_kafka import Producer


TOPIC = "squares-2"

def create_producer():
    conf = {
        "bootstrap.servers" : "broker:29094",
    }
    return Producer(conf)

def delivery_reports(err, msg):
    if err is not None:
        print(f'error = {err}')
    else:
        print(f'msg value = {msg.value().decode('utf-8')}, topic = {msg.topic()} partition = {msg.partition()}, offset = {msg.offset()}')

def send_messages(producer, topic, msg):
        try:
            producer.poll(0)
            producer.produce(
                topic = topic,
                value = json.dumps(msg).encode('utf-8'),
                callback = delivery_reports,
            )
        except BufferError as bf:
            producer.flush()
            print(f'error = {bf}')
        except Exception as e:
            print(f'exception occurred = {e}')
        producer.flush()

if __name__ == "__main__":
    prod = create_producer()
    for x in range(10):
        send_messages(prod, TOPIC, str(x**2))