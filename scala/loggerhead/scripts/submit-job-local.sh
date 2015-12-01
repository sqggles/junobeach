SPARK14=/opt/local/spark-1.4.1-bin-hadoop2.6
SPARK15=/opt/local/spark-1.5.2-bin-hadoop2.6

SPARK_MASTER_LOCAL="local[4]"

KAFKA_BROKER_LOCAL=localhost:9092

HADOOP_CONF=~/hdp23
SPARK_HOME=$SPARK14
KAFKA_BROKERS=$KAFKA_BROKER_LOCAL
KAFKA_TOPICS=testing

SPARK_HOME=$SPARK14 HADOOP_CONF_DIR=$HADOOP_CONF $SPARK_HOME/bin/spark-submit \
  --class com.nvent.loggerhead.KafkaAddressStream \
  target/scala-2.10/loggerhead.jar $KAFKA_BROKERS $KAFKA_TOPICS
