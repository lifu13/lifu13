package com.bgy.spark.test.action

import com.bgy.spark.test.HelloWord
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @author lifu13
 * @create 2021-10-23 18:47
 */
object ActionTest {
  def main(args: Array[String]): Unit = {
    val context: SparkContext = HelloWord.getSparkContext()
     val value: RDD[Int] = context.parallelize(Array(1, 2, 3, 4, 5))
     value.takeOrdered(2)
    Thread.sleep(1000000)
    context.stop()
  }
}
