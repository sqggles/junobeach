TOPIC=testing
./bin/kafka-topics.sh --zookeeper sandbox:2181 --create --topic $TOPIC --partitions 1 --replication-factor 1
