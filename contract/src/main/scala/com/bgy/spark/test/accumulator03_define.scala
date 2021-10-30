package com.bgy.spark.test

/**
 * @author lifu13
 * @create 2021-10-24 18:50
 */
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object accumulator03_define {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3. 创建RDD
    val rdd: RDD[String] = sc.makeRDD(List("Hello", "Hello", "Hello", "Hello", "Spark", "Spark"), 2)

    //3.1 创建累加器
    val acc: MyAccumulator = new MyAccumulator()

    //3.2 注册累加器
    sc.register(acc,"wordcount")

    //3.3 使用累加器
    rdd.foreach(
      word =>{
        acc.add(word)
      }
    )

    //3.4 获取累加器的累加结果
    println(acc.value)

    //4.关闭连接
    sc.stop()
  }
}

// 声明累加器
// 1.继承AccumulatorV2,设定输入、输出泛型
// 2.重新方法
//class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {
//
//  // 定义输出数据集合
//  var map = mutable.Map[String, Long]()
//
//  // 是否为初始化状态，如果集合数据为空，即为初始化状态
//  override def isZero: Boolean = map.isEmpty
//
//  // 复制累加器
//  override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
//    new MyAccumulator()
//  }
//
//  // 重置累加器
//  override def reset(): Unit = map.clear()
//
//  // 增加数据
//  override def add(v: String): Unit = {
//    // 业务逻辑
//    if (v.startsWith("H")) {
//      map(v) = map.getOrElse(v, 0L) + 1L
//    }
//  }
//
//  // 合并累加器
//  override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
//
//    other.value.foreach{
//      case (word, count) =>{
//        map(word) = map.getOrElse(word, 0L) + count
//      }
//    }
//  }
//
//  // 累加器的值，其实就是累加器的返回结果
//  override def value: mutable.Map[String, Long] = map
//}

class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {
  val map = mutable.Map[String, Long]()
  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = new MyAccumulator()

  override def reset(): Unit = map.clear()

  override def add(v: String): Unit = {
    if(v.startsWith("H")){
      map(v)=map.getOrElse(v,0L)+1L
    }
  }

  override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
    other.value.foreach(x=>map(x._1)=map.getOrElse(x._1,0L)+x._2)
  }

  override def value: mutable.Map[String, Long] = map
}