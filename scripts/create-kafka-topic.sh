ZKHOST=localhost:2181
TOPIC=testing
kafka-topics --zookeeper $ZKHOST --create --topic $TOPIC --partitions 1 --replication-factor 1
