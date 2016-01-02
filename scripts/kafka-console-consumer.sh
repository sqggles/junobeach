KAFKA_HOME=/opt/local/kafka_2.11-0.9.0.0
ZKHOST=localhost:2181
TOPIC=testing
$KAFKA_HOME/bin/kafka-console-consumer.sh --zookeeper $ZKHOST --topic $TOPIC --from-beginning 
