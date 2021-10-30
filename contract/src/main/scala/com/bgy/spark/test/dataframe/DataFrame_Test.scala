package com.bgy.spark.test.dataframe


import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row, SparkSession}

/**
 * @author lifu13
 * @create 2021-10-25 19:59
 */
object DataFrame_Test {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("dataframe").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
   // val frame: DataFrame = spark.read.json("./datas/test.json")
    //val rdd: RDD[Row] = frame.rdd
    val value: RDD[Persion] = spark.sparkContext.makeRDD(Array(new Persion(26,"lifu"),new Persion(28,"huazai")))
    import spark.implicits._
    val f: DataFrame = value.toDF
     f.createOrReplaceTempView("test")
//    spark.sql(
//      """
//        |select *
//        |from test
//        |""".stripMargin).show()
    //f.show(1)
   spark.udf.register("MyUdf",(x:String)=>x+"ffff")
    spark.sql("""
                |select MyUdf(age)
                |from test where name = 'lifu'
                |""".stripMargin).show
    val value1: Dataset[Persion] = value.toDS()
    val value2: Dataset[Persion] = f.as[Persion]
    //value1.show()
    spark.stop()
  }
}
case class Persion(age:Int,name:String)



