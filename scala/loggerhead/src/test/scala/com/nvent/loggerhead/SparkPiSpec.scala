package com.nvent.loggerhead

import org.scalatest.FunSpec
import org.scalatest.BeforeAndAfter
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

class SparkPiSpec extends FunSpec with BeforeAndAfter{

  val sparkConf = new SparkConf().setMaster("local").setAppName("SparkPi")
  val sc = new SparkContext(sparkConf)

  describe("Pi") {
    it("should be less than 4 and more than 3"){
      val sp = new SparkPi(sc, 1, 1000)
      val result = sp.exec()
      assert(result > 3 && result < 4)
    }
  }
}

// vim: ft=scala et ts=2 sw=2 tw=0
