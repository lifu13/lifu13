package com.bgy.spark.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author lifu13
 * @create 2021-10-20 20:29
 */
object Test1 {
  def main(args: Array[String]): Unit = {

    val sc: SparkContext = HelloWord.getSparkContext()
    val value: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 5, 6,7,8,9,10,11,12,13,14,15,16),3)
    val value1: RDD[Int] = value.mapPartitions(_.map(_ * 2))
    val value2: RDD[(Int, Int)] = value.mapPartitionsWithIndex((index, y) => y.map((index, _)))
    val listRDD=sc.makeRDD(List(List(List(3,4),List(5,6))), 2)
    listRDD.flatMap(list=>list).foreach(println(_))
    Thread.sleep(100000)
    sc.stop()
  }
}
