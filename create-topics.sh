docker exec -it kafka_setup kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic user-attributes-source
docker exec -it kafka_setup kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic user-attributes-sink

kafka-console-producer --broker-list localhost:9092 --topic user-attributes-source --property "parse.key=true" --property "key.separator=:"
3630b501-16e9-4b6a-a6ac-07325eb8149b:{"userId":"3630b501-16e9-4b6a-a6ac-07325eb8149b","eventDateTime": "2021-12-31T21:54:45.516401320Z","properties": {"fullName":"Leticia Nery", "age": 23 }}
