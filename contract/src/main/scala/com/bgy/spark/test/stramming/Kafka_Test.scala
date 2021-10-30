package com.bgy.spark.test.stramming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author lifu13
 * @create 2021-10-27 22:27
 */
object Kafka_Test {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("kafaka").setMaster("local[1]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
    //3.定义Kafka参数：kafka集群地址、消费者组名称、key序列化、value序列化
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "bgy_ct",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    //4.读取Kafka数据创建DStream
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent, //优先位置
      ConsumerStrategies.Subscribe[String, String](Set("testTopic"), kafkaPara)// 消费策略：（订阅多个主题，配置参数）
    )

    //5.将每条消息的KV取出
    val valueDStream: DStream[String] = kafkaDStream.map(record => record.value())
    valueDStream.foreachRDD(x=>x.foreach(println(_)))
    //valueDStream.print()
    //6.计算WordCount
//    valueDStream.flatMap(_.split(" "))
//      .map((_, 1))
//      .reduceByKey(_ + _)
//      .print()

    //7.开启任务
    ssc.start()
    ssc.awaitTermination()
  }
}
