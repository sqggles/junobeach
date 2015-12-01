SPARK14=/opt/local/spark-1.4.1-bin-hadoop2.6
SPARK15=/opt/local/spark-1.5.2-bin-hadoop2.6

SPARK_MASTER_HWX=yarn-cluster

KAFKA_BROKER_LOCAL=localhost:9092
KAFKA_BROKER_HWX=sandbox.hortonworks.com:6667

HADOOP_CONF=~/hdp23
SPARK_HOME=$SPARK14
SPARK_MASTER=$SPARK_MASTER_HWX
KAFKA_BROKERS=$KAFKA_BROKER_HWX
KAFKA_TOPICS=testing

SPARK_HOME=$SPARK14 HADOOP_CONF_DIR=$HADOOP_CONF $SPARK_HOME/bin/spark-submit \
  --class com.nvent.loggerhead.KafkaAddressStream \
  --master $SPARK_MASTER \
  --driver-class-path config/hive-site.xml \
  --verbose \
  target/scala-2.10/loggerhead.jar $KAFKA_BROKERS $KAFKA_TOPICS
