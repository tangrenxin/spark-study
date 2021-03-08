package com.trx.bigdata.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Description: filter
 *               从服务器日志数据 apache.log 中获取 2015 年 5 月 17 日的请求路径
 * @Author: tangrenxin
 * @Date: 2021/3/7 23:38
 */
object Spark07_RDD_Operator_Transform_filter_Test {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子
    val rdd = sc.textFile("datas/apache.log")
    val filterRDD = rdd.map(
      line => {
        val dateStr = line.split(" ")(3).split(":")(0)
        val url = line.split(" ")(6)
        (dateStr, url)
      }
    )
    filterRDD.filter(_._1 == "17/05/2015").collect().foreach(println)
    sc.stop()

  }
}
