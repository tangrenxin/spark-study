package com.trx.bigdata.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Description: glom
 * @Author: tangrenxin
 * @Date: 2021/3/7 23:38
 */
object Spark05_RDD_Operator_Transform_Test {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子 glom
    // 计算所有分区最大值求和（分区内取最大值，分区间最大值求和）
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)
    // [1，2] ,[ 3,4]
    // [2],[4]
    // [6]
    // 将一个分区的数据当做数组返回
    val glomRDD = rdd.glom()
    // 每个分区的最大值
    val maxRDD = glomRDD.map(
      // 一个数组
      array => {
        array.max // 获取数组的最大值
      })
    println(maxRDD.collect().sum)
    sc.stop()

  }
}
