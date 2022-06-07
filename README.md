# Event-Based-Systems-Project

### [Report](https://docs.google.com/document/d/196CEY1oWiaWEdUhWDFgXy5Yd3SG7C5RRRFBlJQV3A8o/edit?usp=sharing)

### How to start the RabbitMQ instances:
```shell
docker run -d --hostname rabbitmq-1 --name rabbitmq-1 -p 5672:5672 -p 15672:15672 rabbitmq:3-management
docker run -d --hostname rabbitmq-2 --name rabbitmq-2 -p 5673:5672 -p 15673:15672 rabbitmq:3-management
docker run -d --hostname rabbitmq-3 --name rabbitmq-3 -p 5674:5672 -p 15674:15672 rabbitmq:3-management
```

### How to start the brokers:
```shell
python3 src/broker.py 1 # to connect to the first RabbitMQ instance
python3 src/broker.py 2 # to connect to the second RabbitMQ instance
python3 src/broker.py 3 # to connect to the third RabbitMQ instance
```

### How to start the subscribers:
```shell
python3 src/subscriber.py 1 # to connect to the first RabbitMQ instance
python3 src/subscriber.py 2 # to connect to the second RabbitMQ instance
```

### How to start the publisher:
```shell
python3 src/publisher.py 3 # to connect to the third RabbitMQ instance
```
