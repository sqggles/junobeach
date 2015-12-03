SPARK14=/opt/local/spark-1.4.1-bin-hadoop2.6
SPARK14_SRC=/opt/local/spark-1.4.1
SPARK15=/opt/local/spark-1.5.2-bin-hadoop2.6

#needed for working with hive metastore
DATANUCLEUS_JARS=`ls -1 lib/datanucleus-* | tr "\n" "," | sed -e "s/,$//"`

SPARK_MASTER_LOCAL="local[3]"

KAFKA_BROKER_LOCAL=localhost:9092
KAFKA_BROKER_HWX=sandbox.hortonworks.com:6667

HADOOP_CONF=~/hdp23
SPARK_HOME=$SPARK14_SRC
KAFKA_BROKERS=$KAFKA_BROKER_HWX
KAFKA_TOPICS=testing

HIVE_SITE=$SPARK14_SRC/conf/hive-site.xml

spark-submit \
  --class com.nvent.loggerhead.KafkaAddressStream \
  target/scala-2.10/loggerhead.jar $KAFKA_BROKERS $KAFKA_TOPICS
  

#--jars $DATANUCLEUS_JARS /opt/local/hive-1.2.1/lib/hive-exec-1.2.1.jar \
