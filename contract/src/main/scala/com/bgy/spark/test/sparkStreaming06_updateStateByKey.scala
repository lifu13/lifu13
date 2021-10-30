package com.bgy.spark.test

/**
 * @author lifu13
 * @create 2021-10-28 22:17
 */
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object sparkStreaming06_updateStateByKey {

  // 定义更新状态方法，参数seq为当前批次单词次数，state为以往批次单词次数
//  val updateFunc = (seq: Seq[Int], state: Option[Int]) => {
//    // 当前批次数据累加
//    val currentCount = seq.sum
//    // 历史批次数据累加结果
//    val previousCount = state.getOrElse(0)
//    // 总的数据累加
//    Some(currentCount + previousCount)
//  }

  def createSCC(): StreamingContext = {

    //1 创建SparkConf
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")

    //2 创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(3))

    ssc.checkpoint("./ck")

    //3 获取一行数据
    val lines = ssc.socketTextStream("hadoop102", 9998)

    //4 切割
    val words = lines.flatMap(_.split(" "))

    //5 统计单词
    val wordToOne = words.map(word => (word, 1))

    //6 使用updateStateByKey来更新状态，统计从运行开始以来单词总的次数
    val stateDstream = wordToOne.updateStateByKey[Int]((x:Seq[Int],y:Option[Int])=>{
      val sum: Int = x.sum
      Some(y.getOrElse(0)+sum)
    })

    stateDstream.print()
    //7 开启任务
    ssc.start()
    ssc.awaitTermination()
    ssc
  }

  def main(args: Array[String]): Unit = {

//    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("./ck",()=>createSCC())

//    //7 开启任务
//    ssc.start()
//    ssc.awaitTermination()
    createSCC()
  }
}
