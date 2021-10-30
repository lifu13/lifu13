package com.bgy.spark.test.stramming

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * @author lifu13
 * @create 2021-10-29 19:30
 */
object WindowsOperations_Test {
  def main(args: Array[String]): Unit = {
    val context: StreamingContext = new StreamingContext(new SparkConf().setMaster("local[*]").setAppName("StramingContext_test"),Seconds(2))
    context.checkpoint("check")
    val value: ReceiverInputDStream[String] = context.socketTextStream("hadoop102", 9998)
    val value2: DStream[(String, Int)] = value.flatMap(x => x.split(" ")).map(x => (x, 1))
    //val value1: DStream[(String, Int)] = value2.window(Seconds(12), Seconds(6))
    //val value3: DStream[(String, Int)] = value1.reduceByKey(_ + _)
//    val value1: DStream[(String, Int)] = value2.reduceByKeyAndWindow((x:Int, y:Int) => x + y
//      ,(x:Int, y:Int) => x - y
//      , Seconds(12), Seconds(4)
//    ).updateStateByKey((seq:Seq[Int],op:Option[Int])=>
//      Some(seq.toList.sum+op.getOrElse(0)))
    val value1: DStream[(String, Int)] = value2.reduceByWindow((x, y) => (x._1 + y._1, x._2 + y._2), Seconds(12), Seconds(4))
    value1.print()
    context.start()

    context.awaitTermination()


  }
}
