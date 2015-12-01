SPARK14=/opt/local/spark-1.4.1-bin-hadoop2.6
SPARK14_SRC=/opt/local/spark-1.4.1
SPARK15=/opt/local/spark-1.5.2-bin-hadoop2.6

#needed for working with hive metastore
DATANUCLEUS_JARS=`ls -1 $SPARK14_SRC/lib_managed/jars/datanucleus-* | tr "\n" ","`

SPARK_MASTER_HWX=yarn-cluster

KAFKA_BROKER_LOCAL=localhost:9092
KAFKA_BROKER_HWX=sandbox.hortonworks.com:6667

HADOOP_CONF=~/hdp23
SPARK_HOME=$SPARK14_SRC
SPARK_MASTER=$SPARK_MASTER_HWX
KAFKA_BROKERS=$KAFKA_BROKER_HWX
KAFKA_TOPICS=testing

SPARK_HOME=$SPARK14_SRC HADOOP_CONF_DIR=$HADOOP_CONF $SPARK_HOME/bin/spark-submit \
  --class com.nvent.loggerhead.KafkaAddressStream \
  --master $SPARK_MASTER \
  --jars $DATANUCLEUS_JARS $HIVE_SITE \
  --verbose \
  target/scala-2.10/loggerhead.jar $KAFKA_BROKERS $KAFKA_TOPICS
