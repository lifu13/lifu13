package com.bgy.spark.test

import org.apache.spark.{HashPartitioner, RangePartitioner, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author lifu13
 * @create 2021-10-23 17:46
 */
object SparkCoreTop3 {
  def main(args: Array[String]): Unit = {
    val context: SparkContext = HelloWord.getSparkContext()
    val value: RDD[String] = context.textFile("input/agent.log")
    val value1: RDD[(String, Int)] = value.map(x => (x.split(" ")(1) + "-" + x.split(" ")(4), 1)).reduceByKey(_ + _)
    val value2: RDD[(String, (String, Int))] = value1.map(x => (x._1.split("-")(0), (x._1.split("-")(1), x._2)))
    val value3: RDD[(String, Iterable[(String, Int)])] = value2.groupByKey()

    val value4: RDD[(String, Iterable[(String, Int)])] = value3.partitionBy(new HashPartitioner(2))
    value4.partitions.foreach(println(_))
    value3.mapValues(x=>x.toList.sortBy(_._2).take(3)).foreach(println(_))
    context.stop()
  }
}
