package com.trx.bigdata.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Description: 双Value
 * @Author: tangrenxin
 * @Date: 2021/3/7 23:38
 */
object Spark13_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子
    val rdd1 = sc.makeRDD(List(1, 2, 3, 4))
    val rdd2 = sc.makeRDD(List(3, 4, 5, 6))

    // 交集：
    val rdd3 = rdd1.intersection(rdd2)
    println(rdd3.collect().mkString(","))

    // 并集：
    val rdd4 = rdd1.union(rdd2)
    println(rdd4.collect().mkString(","))

    // 差集：
    val rdd5 = rdd1.subtract(rdd2)
    println(rdd5.collect().mkString(","))

    // 拉链：将相同位置的
    val rdd6 = rdd1.zip(rdd2)
    println(rdd6.collect().mkString(","))

    sc.stop()

  }
}
