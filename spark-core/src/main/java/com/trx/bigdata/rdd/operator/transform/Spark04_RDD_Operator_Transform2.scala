package com.trx.bigdata.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Description: flatMap 将 List(List(1,2),3,List(4,5))进行扁平化操作
 * @Author: tangrenxin
 * @Date: 2021/3/7 23:38
 */
object Spark04_RDD_Operator_Transform2 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子 flatMap
    // 将  List(List(1,2),3,List(4,5))进行扁平化操作
    // 注意其中有个元素 3 不是list
    val rdd = sc.makeRDD(List(List(1, 2), 3, List(4, 5)))

    // 数据类型不一样时，对不同的数据类型，进行不同的处理，用到模式匹配
    val flatMapRDD = rdd.flatMap(
      data => {
        data match {
          case list: List[_] => list
          case data => List(data)
        }
      }
    )
    flatMapRDD.collect().foreach(println)

    sc.stop()

  }
}
