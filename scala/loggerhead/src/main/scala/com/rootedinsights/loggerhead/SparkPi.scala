/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rootedinsights.loggerhead

import org.apache.spark._

object SparkPi {

  // Usage: SparkPi [<slices>] [<num_of_samples>]
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    //val sc = new SparkContext(sparkConf)
    val sc = new SparkContext()

    val slices = if (args.length > 0) args(0).toInt else 1
    val num_samples = if (args.length > 1) args(1).toInt else 10000

    val sp = new SparkPi(sc, slices, num_samples)
    val pi = sp.exec()
    println("pi: " + pi)

    sc.stop()
  }
}

class SparkPi(sc: SparkContext, slices: Int = 1, num_samples: Int = 10000) {

  val n = num_samples / slices

  def exec(): Double = {
    val count = sc.parallelize(1 to n, slices).map{i =>
      val x = Math.random()
      val y = Math.random()
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)
    val pi = 4.0 * count / n
    pi
  }
}

// vim: ft=scala tw=0 sw=2 ts=2 et
