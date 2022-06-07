import sys
import uuid
import json
import pika
import config
from apscheduler.schedulers.background import BackgroundScheduler
from generators.subscription_generator import generate_subscriptions
from datetime import datetime

number_of_received_publications = 0
total_latency = 0


def receive_matching_publication_callback(channel, method, properties, body) -> None:
    global number_of_received_publications, total_latency

    number_of_received_publications += 1
    total_latency += (int(datetime.now().timestamp() * 1000) - properties.timestamp)

    # Received matching publication
    message = json.loads(body)
    string_publication = json.loads(message["publication"])
    publication = list(map(lambda element: json.loads(element), string_publication))


def display_status() -> None:
    global number_of_received_publications, total_latency

    if number_of_received_publications > 0:
        print("Time: %r" % datetime.now())
        print("Number of received publications: %d" % number_of_received_publications)
        print("Mean latency: %.3f ms" % (total_latency / number_of_received_publications))
        print("---------------------------")


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

    matching_publications_queue_name = uuid.uuid4().__str__()
    channel.queue_declare(queue=matching_publications_queue_name)
    channel.basic_consume(queue=matching_publications_queue_name,
                          on_message_callback=receive_matching_publication_callback,
                          auto_ack=True)

    subscriptions = generate_subscriptions(config.SUBSCRIPTIONS_COUNT)
    for subscription in subscriptions:
        message = {
            "rabbit_mq_port": rabbit_mq_port,
            "matching_publications_queue": matching_publications_queue_name,
            "subscription": json.dumps(subscription.elements, default=lambda element: element.to_json(), indent=4)
        }
        channel.basic_publish(exchange='', routing_key=config.SUBSCRIPTIONS_QUEUE, body=json.dumps(message, indent=4))
    print("Subscriptions were sent")

    scheduler = BackgroundScheduler()
    scheduler.add_job(display_status, 'interval', seconds=5)
    scheduler.start()

    channel.start_consuming()


if __name__ == "__main__":
    main()
