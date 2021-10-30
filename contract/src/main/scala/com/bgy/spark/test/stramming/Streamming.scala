package com.bgy.spark.test.stramming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @author lifu13
 * @create 2021-10-27 19:23
 */
object Streamming {
  def main(args: Array[String]): Unit = {
    val streamming: SparkConf = new SparkConf().setMaster("local[*]").setAppName("streamming")
    // 程序入口
    val context: StreamingContext = new StreamingContext(streamming, Seconds(2))
    //val value: ReceiverInputDStream[String] = context.socketTextStream("hadoop102", 9999)
    //val value1: DStream[String] = value.flatMap(x => x.split(" "))
    //val value2: DStream[(String, Int)] = value1.map((_, 1)).reduceByKey(_ + _)
    val queue: mutable.Queue[RDD[Int]] = new mutable.Queue[RDD[Int]]()
    val value3: InputDStream[Int] = context.queueStream(queue, false)
    val value4: DStream[Int] = value3.reduce(_ + _)
    value4.print()
    context.start()

    //value2.print()
    for (i <- 1 to 5) {
      queue += context.sparkContext.makeRDD(1 to 5)
      Thread.sleep(1000)
    }
    //value2.print()

    context.awaitTermination()
  }
}
