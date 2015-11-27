SPARK14=/opt/local/spark-1.4.1-bin-hadoop2.6
SPARK15=/opt/local/spark-1.5.2-bin-hadoop2.6
SPARK_HOME=$SPARK14

SPARK_MASTER="local[4]"

KAFKA_HOST=localhost:9092

$SPARK_HOME/bin/spark-submit --class com.nvent.loggerhead.KafkaAddressStream --master $SPARK_MASTER target/scala-2.10/loggerhead.jar $KAFKA_HOST testing
