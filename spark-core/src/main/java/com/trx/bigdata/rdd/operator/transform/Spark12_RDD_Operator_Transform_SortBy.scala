package com.trx.bigdata.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Description: sortBy
 *               排序
 * @Author: tangrenxin
 * @Date: 2021/3/7 23:38
 */
object Spark12_RDD_Operator_Transform_SortBy {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子
    val rdd = sc.makeRDD(List(4, 1, 2, 3, 5, 6), 2)
    // 分区数量不变
    // 底层有shuffle操作
    val newRDD = rdd.sortBy(num => num)
    newRDD.saveAsTextFile("output")
    sc.stop()

  }
}
