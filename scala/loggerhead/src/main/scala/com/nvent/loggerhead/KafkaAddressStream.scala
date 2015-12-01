package com.nvent.loggerhead

import kafka.serializer.StringDecoder

import scala.xml._
import argonaut._, Argonaut._

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.sql.hive._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.log4j.{ LogManager, Level }

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
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
      nodeSeqToString(x \ "street"),
      nodeSeqToString(x \ "city"),
      nodeSeqToString(x \ "state"),
      nodeSeqToString(x \ "zip").split("-")(0)
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


object KafkaAddressStream {
	
  val WINDOW_LENGTH = new Duration(86400 * 1000)
  val SLIDE_INTERVAL = new Duration(10 * 1000)


	LogManager.getRootLogger().setLevel(Level.WARN)

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

    //latlongLookup.toJSON.saveAsTextFile("hdfs://sandbox.hortonworks.com:8020/user/ajish/latLongExtract")

    // Create context with 2 second batch interval
    val ssc = new StreamingContext(sparkConf, Seconds(2))

		// Create direct kafka stream with brokers and topics
		val topicsSet = topics.split(",").toSet
		val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
		val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
			ssc, kafkaParams, topicsSet)

		val xmlfragsDSm = messages.map(_._2)
    
		// -> parse XML 
    // -> filter to include only the 51 states
    val addressesDSm = xmlfragsDSm.map(Address.parseXml).filter( s => stateCodes.contains(s.state) )
    val windowDSm = addressesDSm.window(WINDOW_LENGTH, SLIDE_INTERVAL) 

		windowDSm.foreachRDD(addrs => {
			if (addrs.count() == 0) {
				println("No addresses received in this time interval")
			} else {
				addrs.toDF()

				val top10StatesLast24Hr = addrs.groupBy("state").count()
				val stateOutageCentroidsLast24Hr = addrs.join(latlongLookup, $"zip" === $"zip")
																						    .groupBy("state")
																								.agg( $"state", count(), avg("latitude"), avg("longitude") )

				// Persist 
				top10StatesLast24Hr.save("/tmp/topTenPostsLast24Hour.parquet", "parquet", SaveMode.Overwrite)
				stateOutageCentroidsLast24Hr.save("/tmp/stateOutageCentroidsLast24Hour.parquet", "parquet", SaveMode.Overwrite)
			}
		})
		// Start the computation
		ssc.start()
		ssc.awaitTermination()
	}
}

// vim: ft=scala tw=0 sw=2 ts=2 et
