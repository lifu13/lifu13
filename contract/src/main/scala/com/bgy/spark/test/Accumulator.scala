package com.bgy.spark.test

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

/**
 * @author lifu13
 * @create 2021-10-24 17:02
 */
object Accumulator {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = HelloWord.getSparkContext()
    val value: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5, 6), 2)
    //3.创建RDD
    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)))

    //3.1 打印单词出现的次数（a,10） 代码执行了shuffle，效率比较低
    //dataRDD.reduceByKey(_ + _).collect().foreach(println)
    val sum1: LongAccumulator = sc.longAccumulator("sum")
    //3.2 如果不用shuffle，怎么处理呢？
   // var sum = 0
    // 打印是在Executor端
   val value1: RDD[(String, Int)] = dataRDD.map(t => {
     //3.2 累加器添加数据
     sum1.add(1)
     t
   })
    // 打印是在Driver端
    value1.foreach(println)
    value1.collect();
    value1.collect();
    println(("a", sum1.value))
    sc.stop()
  }
}
