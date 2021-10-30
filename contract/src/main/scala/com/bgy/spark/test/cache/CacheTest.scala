package com.bgy.spark.test.cache

import com.bgy.spark.test.HelloWord
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @author lifu13
 * @create 2021-10-23 22:47
 */
object CacheTest {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = HelloWord.getSparkContext()
    sc.setCheckpointDir("out")
    val lineRdd: RDD[String] = sc.makeRDD(Array("hello flink","hello spark"))
    val wordRdd: RDD[String] = lineRdd.flatMap(line => line.split(" "))

    val wordToOneRdd: RDD[(String, Int)] = wordRdd.map {
      word => {

        (word, 1)
      }
    }

    // 采用reduceByKey，自带缓存
    val wordByKeyRDD: RDD[(String, Int)] = wordToOneRdd.reduceByKey(_+_)

    //3.5 cache操作会增加血缘关系，不改变原有的血缘关系
    //println(wordByKeyRDD.toDebugString)

    //3.4 数据缓存。
    //wordByKeyRDD.cache()

    //3.2 触发执行逻辑
    wordByKeyRDD.collect()

    println("-----------------")
    println(wordByKeyRDD.toDebugString)

    //3.3 再次触发执行逻辑
    wordByKeyRDD.collect()
    println("-----------------fsdfasd ffs")
    println(wordByKeyRDD.toDebugString)

    Thread.sleep(1000000)

    //4.关闭连接
    sc.stop()

  }

}
