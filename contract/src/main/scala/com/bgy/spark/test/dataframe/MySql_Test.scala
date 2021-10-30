package com.bgy.spark.test.dataframe

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author lifu13
 * @create 2021-10-25 21:45
 */
object MySql_Test {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Jdbc").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val frame: DataFrame = spark.read.format("jdbc").option("url", "jdbc:mysql://127.0.0.1:3306/test")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "root")
      .option("dbtable", "person")
      .load()
    frame.createOrReplaceTempView("aaa")
    spark.sql(
      """
        |select *
        |from aaa
        |""".stripMargin).show()
    frame.write.format("jdbc").option("url", "jdbc:mysql://127.0.0.1:3306/test")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "root")
      .option("dbtable", "person")
      .mode("append").save()
    spark.stop()
  }
}
