package com.trx.bigdata.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Description: groupBy
 * @Author: tangrenxin
 * @Date: 2021/3/7 23:38
 */
object Spark06_RDD_Operator_Transform_GroupBy1 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子 groupBy
    val rdd = sc.makeRDD(List("hello","spark","hadoop","scala"), 2)
    // 按单词的第一个字母分组，相同的手写字母将会放在一起
    // 分组和分区没有必然的关系
    val groupRDD = rdd.groupBy(_.charAt(0))
    groupRDD.collect().foreach(println)

    sc.stop()

  }
}
