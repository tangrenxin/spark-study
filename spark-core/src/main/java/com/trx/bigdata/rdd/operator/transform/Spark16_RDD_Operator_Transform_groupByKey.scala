package com.trx.bigdata.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Description: key-value类型
 *               reduceByKey
 * @Author: tangrenxin
 * @Date: 2021/3/7 23:38
 */
object Spark16_RDD_Operator_Transform_groupByKey {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子
    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("b", 4),
    ))
    // groupByKey：将数据源中的数据，相同key的数据分在一个组中，形成一个对偶元组
    //            元组中的第一个元素就是key，
    //            元组中的第二个元素就是相同key的value的集合
    val groupRDD = rdd.groupByKey()

    // (a,CompactBuffer(1, 2, 3))
    //(b,CompactBuffer(4))
    groupRDD.collect().foreach(println)

    // 它与 groupBy 的区别
    val value = rdd.groupBy(_._1)
    // (a,CompactBuffer((a,1), (a,2), (a,3)))
    //(b,CompactBuffer((b,4)))
    value.collect().foreach(println)

    // groupByKey 的key是确定的，而groupBy 的key是不确定的

    sc.stop()

  }
}
