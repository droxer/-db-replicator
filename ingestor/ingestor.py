import json

from kafka import KafkaConsumer


def main():
    consumer = KafkaConsumer('roomis', bootstrap_servers='192.168.99.100:9092')
    for msg in consumer:
        dispatch(msg)

def _extract(msg):
    value = str(msg.value, 'utf-8')
    return json.loads(value)

def dispatch(msg):
    changes = _extract(msg)
    print("Update: table = {}".format(changes['table']))
    print("Update: type = {}".format(changes['type']))
    print("Update: data = {}".format(changes['data']))

if __name__ == '__main__':
    main()