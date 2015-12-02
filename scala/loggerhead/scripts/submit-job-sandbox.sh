SPARK_HOME=/opt/local/spark-1.4.1-bin-hadoop2.6

#needed for working with hive metastore
DATANUCLEUS_JARS=`ls -1 lib/datanucleus-* | tr "\n" "," | sed -e "s/,$//"`
HIVE_SITE=$SPARK14_SRC/conf/hive-site.xml

SPARK_MASTER_HWX=yarn-cluster

KAFKA_BROKER_HWX=sandbox.hortonworks.com:6667

SPARK_HOME=$SPARK14_SRC
SPARK_MASTER=$SPARK_MASTER_HWX
KAFKA_BROKERS=$KAFKA_BROKER_HWX
KAFKA_TOPICS=testing

spark-submit \
  --class com.nvent.loggerhead.KafkaAddressStream \
  --master $SPARK_MASTER \
  --verbose \
  --jars $DATANUCLEUS_JARS \
  --files /etc/spark/conf/hive-site.xml \
  target/scala-2.10/loggerhead.jar $KAFKA_BROKERS $KAFKA_TOPICS

