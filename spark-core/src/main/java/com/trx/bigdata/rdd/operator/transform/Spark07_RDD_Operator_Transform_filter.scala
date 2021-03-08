package com.trx.bigdata.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Description: filter
 *              输出奇数
 * @Author: tangrenxin
 * @Date: 2021/3/7 23:38
 */
object Spark07_RDD_Operator_Transform_filter {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子 groupBy
    val rdd = sc.makeRDD(List(1, 2, 3, 4))
    val filterRDD = rdd.filter(_ % 2 == 1)
    filterRDD.collect().foreach(println)
    sc.stop()

  }
}
