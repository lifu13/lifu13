package com.bgy.spark.test

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * @author lifu13
 * @create 2021-10-24 19:49
 */
object Test01 {

 // 需求说明：品类是指产品的分类，大型电商网站品类分多级，咱们的项目中品类只有一级，不同的公司可能对热门的定义不一样。我们按照每个品类的点击、下单、支付的量来统计热门品类。
 // 鞋			点击数 下单数  支付数
 // 衣服		点击数 下单数  支付数
 // 电脑		点击数 下单数  支付数
 // 例如，综合排名 = 点击数*20% + 下单数*30% + 支付数*50%
 // 本项目需求优化为：先按照点击数排名，靠前的就排名高；如果点击数相同，再比较下单数；下单数再相同，就比较支付数。
  def main(args: Array[String]): Unit = {
    val context: SparkContext = HelloWord.getSparkContext()
    val value: RDD[String] = context.textFile("datas/user_visit_action.txt")
   val value1: RDD[UserVisitAction] = value.map(line => {
     val datas: Array[String] = line.split("_")
     // 将解析的数据封装到 UserVisitAction
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
   })
   val value4: RDD[CategoryCountInfo] = value1.mapPartitions[CategoryCountInfo](x => {
     val infoes: ListBuffer[CategoryCountInfo] = new ListBuffer[CategoryCountInfo]()
     x.foreach(
       y => if (y.click_category_id != -1)
         infoes += CategoryCountInfo(y.click_category_id.toString, 1, 0, 0)
       else if (y.order_category_ids != null) {
         y.order_category_ids.split(",").foreach(
           id => infoes += CategoryCountInfo(id, 0, 1, 0)
         )
       } else if (y.pay_category_ids != null) {
         y.pay_category_ids.split(",").foreach(
           id => infoes += CategoryCountInfo(id, 0, 0, 1)
         )
       }
     )
     infoes.iterator
   })
   //3.5 创建累加器
   val acc: MyTest = new MyTest()

   //3.6 注册累加器
   context.register(acc, "MyTest")
   value1.foreach(x=>acc.add(x))
   val list: List[(String, CategoryCountInfo)] = acc.value.toList
   println(list)
   list.groupBy(x=>x._2.clickCount).take(3).foreach(println(_))
  //val value2: RDD[(String, Iterable[CategoryCountInfo])] = value4.groupBy(_.categoryId)
  //// 设置共享变量
  //val categoryCountInfo = new CategoryCountInfo(null,0,0,0)
  //val value3: RDD[(String, CategoryCountInfo)] = value2.mapValues(x => {
  //  x.foreach(
  //    y => {
  //      categoryCountInfo.categoryId = y.categoryId
  //      categoryCountInfo.clickCount = y.clickCount + categoryCountInfo.clickCount
  //      categoryCountInfo.orderCount = y.orderCount + categoryCountInfo.orderCount
  //      categoryCountInfo.payCount = y.orderCount + categoryCountInfo.payCount
  //    }
  //  )
  //  categoryCountInfo
  //})
  //val value5: RDD[(String, CategoryCountInfo)] = value3.sortBy(x => (x._2.clickCount, x._2.categoryId, x._2.payCount))
  // value5.take(3)
   //Thread.sleep(1000000)

    context.stop()
  }
}




//用户访问动作表
case class UserVisitAction(date: String,//用户点击行为的日期
                           user_id: Long,//用户的ID
                           session_id: String,//Session的ID
                           page_id: Long,//某个页面的ID
                           action_time: String,//动作的时间点
                           search_keyword: String,//用户搜索的关键词
                           click_category_id: Long,//某一个商品品类的ID
                           click_product_id: Long,//某一个商品的ID
                           order_category_ids: String,//一次订单中所有品类的ID集合
                           order_product_ids: String,//一次订单中所有商品的ID集合
                           pay_category_ids: String,//一次支付中所有品类的ID集合
                           pay_product_ids: String,//一次支付中所有商品的ID集合
                           city_id: Long)//城市 id
// 输出结果表
case class CategoryCountInfo(var categoryId: String,//品类id
                             var clickCount: Long,//点击次数
                             var orderCount: Long,//订单次数
                             var payCount: Long)//支付次数
//注意：样例类的属性默认是val修饰，不能修改；需要修改属性，需要采用var修饰。

// 累加器实现
class MyTest extends AccumulatorV2[UserVisitAction,Map[String,CategoryCountInfo]] {
  val stringToInfo: mutable.Map[String, CategoryCountInfo] = mutable.Map[String, CategoryCountInfo]()
  override def isZero: Boolean = stringToInfo.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, Map[String,CategoryCountInfo]] = new MyTest

  override def reset(): Unit = stringToInfo.clear()

  override def add(v: UserVisitAction): Unit = {
    if(v.click_category_id != -1){
      stringToInfo(v.click_category_id.toString) =
        CategoryCountInfo(v.click_category_id.toString
          ,stringToInfo.getOrElse(v.click_category_id.toString,new CategoryCountInfo(null,0,0,0) ).clickCount+1,0,0)

    }
  }

  override def merge(other: AccumulatorV2[UserVisitAction, Map[String, CategoryCountInfo]]): Unit = {
    other.value.foreach(
      x=> stringToInfo.getOrElse(x._1,new CategoryCountInfo(null,0,0,0)).clickCount = stringToInfo.getOrElse(x._1,new CategoryCountInfo(null,0,0,0)).clickCount+x._2.clickCount
    )
  }

  override def value: Map[String, CategoryCountInfo] = stringToInfo.toMap
}
