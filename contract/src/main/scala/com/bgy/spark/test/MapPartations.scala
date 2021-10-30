package com.bgy.spark.test

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}



/**
 * @author lifu13
 * @create 2021-10-21 20:50
 */
object MapPartations {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = HelloWord.getSparkContext()
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 3)), 2)
    val value: RDD[(String, Int)] = sc.makeRDD(List(("a", 3), ("b", 4)), 1)
    //value.reduceByKey(_+_).foreach(println(_))
    //value.groupByKey().mapValues(_.sum).foreach(println(_))
    //value.aggregateByKey(0)((x,y)=>x+y,_*_).foreach(println(_))
    //value.foldByKey(0)(_+_).foreach(println(_))
    println(value.sortByKey(false).dependencies)
//   value.combineByKey(c=>(c,1),(x:(Int,Int),y)=>(x._1+y,x._2+1),(x:(Int,Int),y:(Int,Int)) =>(x._1+y._1,y._2+x._2)).foreach(
//      println(_)
//    )
    value.sortByKey(false).foreach(println(_))

    //value.mapValues({x=>x+2;2}).foreach(println(_))
    // rdd.join(value).foreach(println(_))
    //rdd.cogroup(value).foreach(println(_))
    //rdd.countByKey().foreach(println(_))
    //rdd.saveAsTextFile("outer")
    //rdd.saveAsObjectFile("outer1")
    //rdd.saveAsSequenceFile("outer2")
   value.map(x=>x).collect()
    Thread.sleep(100000)
    sc.stop()
  }
}

 class User(name:String, age:Int)