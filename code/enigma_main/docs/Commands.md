## Comandos de Docker para Kafka

* Lista los topics: `docker exec -ti kafka /opt/kafka/bin/kafka-topics.sh --list --zookeeper zookeeper:2181`
* Crear un nuevo topic: `docker exec -ti kafka /opt/kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic newTopic`
* Comienza un consumidor de Kafka: `docker exec -ti kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test_topic --from-beginning`
* Comienza a un productor de Kafka: `docker exec -ti kafka /opt/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test_topic`


## Comandos CURL
* Agregar un asunto: `curl -i -X POST -H 'Content-Type: application/json' -d '{"name":"topic1","author":"Luis","description":"","databaseName":"", "observationDataClassName":"cu.uclv.mfc.enigma.observations.data.custom.DoubleObservationData", "interval": 60}' http://localhost:14080/api/addtopic`