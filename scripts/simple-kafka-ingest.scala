import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
 
val ssc = new StreamingContext(sc, Seconds(1))
 
val consumerThreadsPerInputDstream = 3
val topics = Map("testing" -> consumerThreadsPerInputDstream)
val kafkaParams = Map( "zookeeper.connect" -> "localhost:2181", "group.id" -> "1")
 
val kafkaStream = KafkaUtils.createStream(ssc, "localhost:2181", "1", topics, StorageLevel.MEMORY_ONLY)
 
kafkaStream.foreachRDD(rdd => { rdd.foreach(println) })
 
ssc.start()
ssc.awaitTermination()
