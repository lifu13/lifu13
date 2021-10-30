package com.bgy.spark.test

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
 * @author lifu13
 * @create 2021-10-24 19:22
 */
object Broadcast_Test {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = HelloWord.getSparkContext()
    val value: RDD[String] = sc.makeRDD(List("abvfsfsadfsdafsadfsadfasdfasdfasdfasdf","b"), 2)
    val tuples: String = "a"
    val value1: Broadcast[String] = sc.broadcast(tuples)
    value.filter(x=>x.contains(value1.value)).foreach(print(_))
    sc.stop()
  }
}
