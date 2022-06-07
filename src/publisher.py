import sys
import json
import time
import pika
import config
from generators.publication_generator import generate_publications
from datetime import datetime, timedelta


def main() -> None:
    if len(sys.argv) != 2:
        print("Usage: broker.py instance_index")
        sys.exit(0)

    if not int(sys.argv[1]) in config.RABBIT_MQ_PORTS.keys():
        print("Invalid instance_index.")
        sys.exit(0)

    credentials = pika.PlainCredentials(config.RABBIT_MQ_USER, config.RABBIT_MQ_PASSWORD)
    rabbit_mq_port = config.RABBIT_MQ_PORTS[int(sys.argv[1])]
    connection_parameters = pika.ConnectionParameters(config.HOST_IP, rabbit_mq_port,
                                                      config.RABBIT_MQ_VIRTUAL_HOST, credentials)

    connection = pika.BlockingConnection(connection_parameters)
    channel = connection.channel()

    end_time = datetime.now() + timedelta(minutes=3)
    while datetime.now() <= end_time:
        publications = generate_publications(config.PUBLICATIONS_COUNT)
        for publication in publications:
            message = {
                "rabbit_mq_port": rabbit_mq_port,
                "publication": json.dumps(publication.elements, default=lambda element: element.to_json(), indent=4)
            }
            channel.basic_publish(exchange='', routing_key=config.PUBLICATIONS_QUEUE,
                                  body=json.dumps(message, indent=4),
                                  properties=pika.BasicProperties(timestamp=(int(datetime.now().timestamp()) * 1000)))
            time.sleep(0.001)
        time.sleep(0.1)

    connection.close()


if __name__ == "__main__":
    main()
