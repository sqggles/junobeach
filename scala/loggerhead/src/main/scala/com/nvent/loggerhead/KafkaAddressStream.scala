package com.nvent.loggerhead

import kafka.serializer.StringDecoder

import scala.xml._
import argonaut._, Argonaut._

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf

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
      nodeSeqToString(x \ "zip") 
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
	
	LogManager.getRootLogger().setLevel(Level.WARN)

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

		val Array(brokers, topics) = args

		// Create context with 2 second batch interval
		val sparkConf = new SparkConf().setAppName("KafkaAddressStream")
		val ssc = new StreamingContext(sparkConf, Seconds(2))

		// Create direct kafka stream with brokers and topics
		val topicsSet = topics.split(",").toSet
		val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
		val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
			ssc, kafkaParams, topicsSet)

		val xmlfrags = messages.map(_._2)
    xmlfrags.print()
		val addresses = xmlfrags.map(Address.parseXml)
    addresses.map( x => x.asJson ).print()

		// Start the computation
		ssc.start()
		ssc.awaitTermination()
	}
}

// vim: ft=scala tw=0 sw=2 ts=2 et
