docker exec -it kafka_setup kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic user-attributes-source
docker exec -it kafka_setup kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic user-attributes-sink

kafka-console-producer --broker-list localhost:9092 --topic user-attributes-source --property "parse.key=true" --property "key.separator=:"
#
user3:{"userId":"user3","eventDateTime": "2021-12-31T16:28:40.516401320Z","properties": {"name":"Nery 3"}}
