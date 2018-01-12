# AttunityKafkaConsumer

Consume messages from Kafka that have been created by Attunity and create difference records


## Build instructions
MINT doesn't seem to like Maven, so I've had to bundle the dependencies locally into the `libs` directory.

Use something like IntelliJ to compile it.

## How to run it if you built it yourself
`java steveyg.kafka.attunity.Main <topic>`


## How to run it if you are lazy and used the JAR file
`java -jar AttunityKafkaConsumer.jar <topic>`

