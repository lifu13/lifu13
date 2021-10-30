package com.bgy.spark.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author lifu13
 * @create 2021-10-24 22:47
 */
object require03_PageFlow {

  def main(args: Array[String]): Unit = {


    /**
    实现方式2
输入参数：模板文件路径，用户兴趣字符串
返回值：用户兴趣字符串对应的索引值
     */




    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.1 获取原始数据
    val lineRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")

    //3.2 将原始数据进行转换
    val actionRDD: RDD[UserVisitAction] = lineRDD.map {
      line => {
        val datas: Array[String] = line.split("_")

        UserVisitAction(
          datas(0),
          datas(1).toLong,
          datas(2),
          datas(3).toLong,
          datas(4),
          datas(5),
          datas(6).toLong,
          datas(7).toLong,
          datas(8),
          datas(9),
          datas(10),
          datas(11),
          datas(12).toLong
        )
      }
    }

    //3.3 定义要统计的页面（只统计集合中规定的页面跳转率）
    val ids = List(1, 2, 3, 4, 5, 6, 7)
    // 准备过滤数据
    val idZipList: List[String] = ids.zip(ids.tail).map {
      case (pageId1, pageId2) => {
        pageId1 + "-" + pageId2
      }
    }
    ids.tail.foreach(println(_))

    //4 计算分母
    val idsMap: Map[Long, Long] = actionRDD
      // 过滤出要统计的page_id(由于最后一个页面总次数，不参与运算，所以也过滤了)
      .filter(action => ids.contains(action.page_id))
      // 结构变换
      .map(action => (action.page_id, 1L))
      // 统计每个页面的总次数
      .reduceByKey(_ + _).collect().toMap

    //5 计算分子
    //5.1 将原始数据根据Session进行分组：(session,Iterable[UserVisitAction])
    val sessionGroupRDD: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy(_.session_id)

    //5.2 将分组后的数据根据时间进行排序（升序）:List((pageId1-pageId2))
    val pageFlowRDD: RDD[List[String]] = sessionGroupRDD.mapValues(
      datas => {
        //5.2.1 对分组后的数据进行排序
        val actions: List[UserVisitAction] = datas.toList.sortWith(
          (left, right) => {
            left.action_time < right.action_time
          }
        )

        //5.2.2 获取PageId
        val pageidList: List[Long] = actions.map(_.page_id)

        //5.2.3 形成单跳元组(pageId1, pageId2)  (1,2,3,4…).zip((2,3,4…))
        val pageToPageList: List[(Long, Long)] = pageidList.zip(pageidList.tail)

        //5.2.4 变换结构
        //=>List((pageId1-pageId2),(pageId2-pageId3),(pageId3-pageId4),(pageId4-pageId5),(pageId5-pageId6),(pageId6-pageId7))
        val pageJumpCounts: List[String] = pageToPageList.map {
          case (pageId1, pageId2) => {
            pageId1 + "-" + pageId2
          }
        }

        //5.2.5 再次进行过滤，减轻计算负担
        // 1-2 2-3 3-4 4-5 5-6 6-7
        pageJumpCounts.filter(data => idZipList.contains(data))
      }
    ).map(_._2)

    // pageFlowRDD.foreach(println)

    //6.聚合统计结果：(pageId1-pageId2, sum)
    val pageFlowMapRDD: RDD[(String, Long)] = pageFlowRDD.flatMap(list => list).map((_, 1L)).reduceByKey(_ + _)

    //7 计算页面单跳转换率
    pageFlowMapRDD.foreach {
      case (pageflow, sum) => {
        val pageIds: Array[String] = pageflow.split("-")
        val pageIdSum: Long = idsMap.getOrElse(pageIds(0).toLong, 1L)

        println(pageflow + "=" + sum.toDouble / pageIdSum)
      }
    }

    //8.关闭连接
    sc.stop()
  }
}