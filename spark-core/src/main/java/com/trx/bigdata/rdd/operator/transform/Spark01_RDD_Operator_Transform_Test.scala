package com.trx.bigdata.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Description: map 的例子
 * @Author: tangrenxin
 * @Date: 2021/3/7 23:38
 */
object Spark01_RDD_Operator_Transform_Test {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子 map
    val rdd = sc.textFile("datas/apache.log")

    // 长的字符串转换为短的字符串
    val mapRDD = rdd.map(
      line => {
        val datas = line.split(" ")
        val str = datas(6)
        str
      }
    )
    mapRDD.collect().foreach(println)


    sc.stop()

  }
}
