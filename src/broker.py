import json
import sys
from typing import List

import pika
import config

routing_table = {}


def receive_subscription_callback(channel, method, properties, body) -> None:
    global routing_table

    message = json.loads(body)
    string_subscription = json.loads(message["subscription"])
    subscription = list(map(lambda element: json.loads(element), string_subscription))
    rabbit_mq_port = message["rabbit_mq_port"]
    matching_publications_queue_name = message["matching_publications_queue"]
    print("Received subscription: %r" % subscription)
    print("RabbitMQ port: %d" % rabbit_mq_port)
    print("Matching publications queue: %r" % matching_publications_queue_name)
    print("---------------------------")

    if not (rabbit_mq_port, matching_publications_queue_name) in routing_table:
        routing_table[(rabbit_mq_port, matching_publications_queue_name)] = []
    routing_table[(rabbit_mq_port, matching_publications_queue_name)].append(subscription)

    rabbit_mq_ports_of_neighbors = get_rabbit_mq_ports_of_neighbors(config.RABBIT_MQ_PORTS[int(sys.argv[1])])
    for port in rabbit_mq_ports_of_neighbors:
        if port != rabbit_mq_port:
            credentials = pika.PlainCredentials(config.RABBIT_MQ_USER, config.RABBIT_MQ_PASSWORD)
            connection_parameters = pika.ConnectionParameters(config.HOST_IP, port,
                                                              config.RABBIT_MQ_VIRTUAL_HOST, credentials)
            connection = pika.BlockingConnection(connection_parameters)

            channel = connection.channel()
            subscription_message = {
                "rabbit_mq_port": config.RABBIT_MQ_PORTS[int(sys.argv[1])],
                "matching_publications_queue": config.PUBLICATIONS_QUEUE,
                "subscription": message["subscription"]
            }
            channel.basic_publish(exchange='', routing_key=config.SUBSCRIPTIONS_QUEUE,
                                  body=json.dumps(subscription_message, indent=4))
            connection.close()


def receive_publication_callback(channel, method, properties, body) -> None:
    global routing_table

    message = json.loads(body)
    string_publication = json.loads(message["publication"])
    publication = list(map(lambda element: json.loads(element), string_publication))
    rabbit_mq_port = message["rabbit_mq_port"]
    print("RabbitMQ port: %d" % rabbit_mq_port)
    print("Received publication: %r" % publication)
    print("---------------------------")

    for destination in routing_table:
        destination_rabbit_mq_port, matching_publications_queue_name = destination
        if destination_rabbit_mq_port != rabbit_mq_port:
            for subscription in routing_table[destination]:
                if does_publication_match_subscription(publication, subscription):
                    credentials = pika.PlainCredentials(config.RABBIT_MQ_USER, config.RABBIT_MQ_PASSWORD)
                    connection_parameters = pika.ConnectionParameters(config.HOST_IP, destination_rabbit_mq_port,
                                                                      config.RABBIT_MQ_VIRTUAL_HOST, credentials)
                    connection = pika.BlockingConnection(connection_parameters)

                    channel = connection.channel()
                    publication_message = {
                        "rabbit_mq_port": config.RABBIT_MQ_PORTS[int(sys.argv[1])],
                        "publication": message["publication"]
                    }
                    channel.basic_publish(exchange='', routing_key=matching_publications_queue_name,
                                          body=json.dumps(publication_message, indent=4))
                    connection.close()
                    break


def get_rabbit_mq_ports_of_neighbors(broker_port: int) -> List[int]:
    if broker_port == config.RABBIT_MQ_PORTS[1]:
        return [config.RABBIT_MQ_PORTS[2]]
    elif broker_port == config.RABBIT_MQ_PORTS[2]:
        return [config.RABBIT_MQ_PORTS[1], config.RABBIT_MQ_PORTS[3]]

    return [config.RABBIT_MQ_PORTS[2]]


def does_publication_match_subscription(publication, subscription) -> bool:
    for subscription_element in subscription:
        subscription_field = subscription_element["field"]
        subscription_operator = subscription_element["operator"]
        if subscription_operator == "=":
            subscription_operator = "=="
        subscription_value = subscription_element["value"]
        for publication_element in publication:
            publication_field = publication_element["field"]
            if subscription_field == publication_field:
                publication_value = publication_element["value"]
                expression_to_evaluate = "publication_value %s subscription_value" % subscription_operator
                if not eval(expression_to_evaluate):
                    return False

    return True


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
    channel.queue_declare(queue=config.SUBSCRIPTIONS_QUEUE)
    channel.queue_declare(queue=config.PUBLICATIONS_QUEUE)

    channel.basic_consume(queue=config.SUBSCRIPTIONS_QUEUE, on_message_callback=receive_subscription_callback,
                          auto_ack=True)
    channel.basic_consume(queue=config.PUBLICATIONS_QUEUE, on_message_callback=receive_publication_callback,
                          auto_ack=True)

    channel.start_consuming()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit()
