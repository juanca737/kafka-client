# kafka-client
Basic example of Kafka producer and consumer clients

This project is intended to be a basic example of use for KafkaProducer 
and KafkaConsumer clients.

The project is a Spring Boot application that expose some end-points to
easily interact with the clients, for instructing to send a batch or 
custom messages to the test topics; also we can retrieve all the messages 
from the selected topic.

In the examples we can se how to send messages to a topic with one partition, 
and we can also send messages to other partition with a CustomPartitioner
which sends messages to a specific partition in the topic based on the condition.

The producers use a simple callback implementation just to understand how
callback works when a message is sent to a topic.

### Environment
The Kafka server was set up by running a docker image that contains a Kafka Broker, 
Zookeeper and Kafka Schema Registry .

### End-point reference

**Create a schema**:<br>
```
localhost:8080/schemas?resourceName=acs-value.avsc&subject=topic-1-value
```
Schemas are created based on the schema files located in the resources directory.<br>
resourceName must match an existing file located in the resources directory.


**View schemas**:<br>
```
localhost:8081/schemas
```

**Delete a schema**:<br>
```
localhost:8081/schemas
```
