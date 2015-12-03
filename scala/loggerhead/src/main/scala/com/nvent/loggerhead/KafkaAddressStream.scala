package com.nvent.loggerhead

import kafka.serializer.StringDecoder

import scala.xml._
import argonaut._, Argonaut._
import java.sql.Timestamp

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.sql.hive._
import org.apache.spark.sql.hive.orc._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.log4j.{ LogManager, Level }

/**
 * Consumes messages from one or more topics in Kafka.
 * Usage: KafkaAddressStream <brokers> <topics>
 *   <brokers> is a list of one or more Kafka brokers
 *   <topics> is a list of one or more kafka topics to consume from
 *
 * Example:
 *    $ bin/run-example com.nvent.loggerhead.KafkaAddressStream broker1-host:port,broker2-host:port \
 *    topic1,topic2
 */


case class Address ( street: String, city: String, state: String, zip: String )

object Address {

  def nodeSeqToString(nodes:scala.xml.NodeSeq): String =
    nodes.map(_.text).mkString("")

  def parseXml(xstr: String): Address = {
    val x = XML.loadString(xstr)
    Address(
      nodeSeqToString(x \ "street").replaceAll("[^A-Za-z0-9 ]", ""),
      nodeSeqToString(x \ "city").replaceAll("[^A-Za-z0-9 ]", ""),
      nodeSeqToString(x \ "state").replaceAll("[^A-Za-z0-9 ]", ""),
      (nodeSeqToString(x \ "zip").split("-")(0)).replaceAll("[^0-9]", "")
    )
  }

  // implicit conversion to json with argonaut
  implicit def AddressEncodeJson: EncodeJson[Address] =
  EncodeJson((addr: Address) =>
    ("street" := addr.street) ->:
    ("city" := addr.city) ->:
    ("state" := addr.state) ->:
    ("zip" := addr.zip) ->: 
    jEmptyObject
  )

}

case class StateOutageAggregate( ts: java.sql.Timestamp, state: String, count: Long, latm: Double, longm: Double )

object KafkaAddressStream {
  
  val WINDOW_LENGTH = new Duration(30 * 1000)
  val SLIDE_INTERVAL = new Duration(5 * 1000)
  val BATCH_INTERVAL = new Duration(1 * 1000)

  LogManager.getRootLogger().setLevel(Level.WARN)

  // the 51 states
  val stateCodes = List(
    "AK","AL","AR","AZ","CA","CO","CT","DC","DE","FL",
    "GA","HI","IA","ID","IL","IN","KS","KY","LA","MA",
    "MD","ME","MI","MN","MO","MS","MT","NC","ND","NE",
    "NH","NJ","NM","NV","NY","OH","OK","OR","PA","RI",
    "SC","SD","TN","TX","UT","VA","VT","WA","WI","WV","WY"
  )

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(s"""
        |Usage: KafkaAddressStream <brokers> <topics>
        |  <brokers> is a list of one or more Kafka brokers
        |  <topics> is a list of one or more kafka topics to consume from
        |
        """.stripMargin)
      System.exit(1)
    }
    //kafka arguments
    val Array(brokers, topics) = args

    val sparkConf = new SparkConf().setAppName("KafkaAddressStream")
    val sc = new SparkContext(sparkConf)
    
    //create a Hive Context
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    import sqlContext.implicits._

    // fetch the lookup table
    sqlContext.sql("USE loggerhead")  
    val latlongLookup = sqlContext.sql("SELECT zip, latitude, longitude, timezone, dst FROM us_zip_to_lat_long")
    latlongLookup.cache()
    latlongLookup.show(5)
    //latlongLookup.toJSON.saveAsTextFile("hdfs://sandbox.hortonworks.com:8020/user/ajish/latLongExtract")

    // check that the output tables are set up in Hive
    // TODO: go over Ranger HiveAuthorizer permission in secure cluster mode

    //sqlContext.sql("CREATE TABLE IF NOT EXISTS outage_addresses(street STRING, city STRING, state STRING, zip STRING) STORED AS orc")
    //sqlContext.sql("CREATE TABLE IF NOT EXISTS state_outage_centroids(ts TIMESTAMP, state STRING, count BIGINT, latm DOUBLE, longm DOUBLE) STORED AS orc")
    //case class Address ( street: String, city: String, state: String, zip: String )
    //case class StateOutageAggregate( ts: java.sql.Timestamp, state: String, count: Long, latm: Double, longm: Double )


    // Create context with 
    val ssc = new StreamingContext(sc, BATCH_INTERVAL)

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    val xmlfragsDSm = messages.map(_._2)
    
    // -> parse XML 
    // -> filter to include only the 51 states
    val addressesDSm = xmlfragsDSm.map(Address.parseXml).filter( s => stateCodes.contains(s.state) )
    addressesDSm.print(2)
    val windowDSm = addressesDSm.window(WINDOW_LENGTH, SLIDE_INTERVAL) 

    // TODO: repartition as required for efficiency
    windowDSm.foreachRDD( (addrs:RDD[Address], dstrTime:Time) => {
      
      val ts = new java.sql.Timestamp(dstrTime.milliseconds)

      if (addrs.count() == 0) {
        println("No addresses received in this time interval")
      } else {
        val addrsDF = addrs.toDF()

        // TODO: move timestamping logic here and add column to address stream before Hive write
        
        // Persist to Hive
        // addrsDF.write().mode(SaveMode.Append).insertInto("outage_addresses");
        
        // TODO: set flag to prevent agg key from appearing in output (default behaviour in 1.5+)
        
        val stateOutageCentroidsAgg = addrsDF.join(latlongLookup, "zip")
                                                  .groupBy("state")
                                                  .agg( $"state", count("state"), avg("latitude"), avg("longitude") )
                                                  .map( row => StateOutageAggregate(ts, row.getString(1), row.getLong(2), row.getDouble(3), row.getDouble(4)) )
                                                  .toDF()
        // debug/demo
        stateOutageCentroidsAgg.show(10) 
        
        // Persist to Parquet
        // save("/tmp/stateOutageCentroidsAggregate.parquet", "parquet", SaveMode.Append)
        
        // Persist to Hive
        // stateOutageCentroidsAgg.write().mode(SaveMode.Append).insertInto("state_outage_centroids");
        
        // TODO: pick format and partitioning for hive table
        // TODO: stress test and wrap connection in forEachPartition
      }
    })
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}

// vim: ft=scala tw=0 sw=2 ts=2 et
