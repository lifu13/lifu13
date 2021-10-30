package com.bgy.spark.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author lifu13
 * @create 2021-10-19 20:33
 */
object HelloWord {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MyApp1").setMaster("local[1]")
    //spark 程序入口
    val sc: SparkContext = new SparkContext(conf)
    val value: RDD[String] = sc.textFile("input/agent.log",7)
    //val value1: RDD[String] = value.flatMap(x => x.split(" "))

    value.sortBy(x=>x)
    Thread.sleep(1000000)
    sc.stop()
  }
  def getSparkContext():SparkContext={
     return new SparkContext(new SparkConf().setAppName("MyApp").setMaster("local[4]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").registerKryoClasses(
       Array(classOf[User])
     ))

  }
}


