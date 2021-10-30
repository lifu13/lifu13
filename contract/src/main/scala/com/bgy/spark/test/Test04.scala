package com.bgy.spark.test

import org.apache.spark.SparkContext

import scala.io.Source

/**
 * @author lifu13
 * @create 2021-10-25 12:39
 */
object Test04 {
  def main(args: Array[String]): Unit = {
   // val context: SparkContext = HelloWord.getSparkContext()
    //函数定义
    val findIndex: (String) =>  Int = {
      (filePath) =>
        val source = Source.fromFile(filePath, "UTF-8")
        val lines = source.getLines().toArray
        source.close()
        val searchMap = lines.zip(0 until lines.size).toMap
       // (interest) => searchMap.getOrElse(interest, -1)
        1
    }
    //val partFunc = findIndex(filePath)

    //Dataset中的函数调用
   // partFunc("体育-篮球-NBA-湖人")
  }
}
