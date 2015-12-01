SPARK14=/opt/local/spark-1.4.1-bin-hadoop2.6
SPARK14_SRC=/opt/local/spark-1.4.1
SPARK15=/opt/local/spark-1.5.2-bin-hadoop2.6

#needed for working with hive metastore
DATANUCLEUS_JARS=`ls -1 $SPARK14_SRC/lib_managed/jars/datanucleus-* | tr "\n" ","`

SPARK_MASTER_LOCAL="local[4]"

KAFKA_BROKER_LOCAL=localhost:9092

HADOOP_CONF=~/hdp23
SPARK_HOME=$SPARK14_SRC
KAFKA_BROKERS=$KAFKA_BROKER_LOCAL
KAFKA_TOPICS=testing

HADOOP_HOME=/opt/local/hadoop-2.7.1 SPARK_HOME=$SPARK14_SRC HADOOP_CONF_DIR=$HADOOP_CONF $SPARK_HOME/bin/spark-submit \
  --conf "spark.driver.allowMultipleContexts=true" \
  --class com.nvent.loggerhead.KafkaAddressStream \
  --jars $DATANUCLEUS_JARS \
  target/scala-2.10/loggerhead.jar $KAFKA_BROKERS $KAFKA_TOPICS
