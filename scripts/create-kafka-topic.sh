KAFKA_HOME=/opt/local/kafka_2.11-0.9.0.0
ZKHOST=localhost:2181
TOPIC=testing
$KAFKA_HOME/bin/kafka-topics.sh --zookeeper $ZKHOST --create --topic $TOPIC --partitions 1 --replication-factor 1
