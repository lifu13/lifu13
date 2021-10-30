package com.bgy.spark.test

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author lifu13
 * @create 2021-10-28 21:38
 */
object UpdateStageByKey_Test {

  val updateFunc = (seq: Seq[Int], state: Option[Int]) => {
    // 当前批次数据累加
    val currentCount = seq.sum
    // 历史批次数据累加结果
    val previousCount = state.getOrElse(0)
    // 总的数据累加
    Some(currentCount + previousCount)
  }
  def test(): StreamingContext  ={
    val update: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Update")
    val context: StreamingContext = new StreamingContext(update,Seconds(3))
    context.checkpoint("./check")
    val value: ReceiverInputDStream[String] = context.socketTextStream("hadoop102", 9998)
    //4 切割
    val words: DStream[String] = value.flatMap(_.split(" "))

    //5 统计单词
    val value1: DStream[(String, Int)] = words.map(word => (word, 1))
    val value2: DStream[(String, Int)] = value1.updateStateByKey[Int](
      (x: Seq[Int], y: Option[Int]) => {
        val sum: Int = x.sum
        Some(y.getOrElse(0) + sum)
      })
    value2.print()
    context.start();
    context.awaitTermination();
    context
  }
  def main(args: Array[String]): Unit = {
       test()
//    val context: StreamingContext = StreamingContext.getActiveOrCreate("./check", test)
//    context.start();
//    context.awaitTermination();
  }

}
