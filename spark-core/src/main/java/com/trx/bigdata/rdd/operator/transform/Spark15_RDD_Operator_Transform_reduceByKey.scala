package com.trx.bigdata.rdd.operator.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @Description: key-value类型
 *               reduceByKey
 * @Author: tangrenxin
 * @Date: 2021/3/7 23:38
 */
object Spark15_RDD_Operator_Transform_reduceByKey {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子
    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("b", 4),
    ))

    // 相同的key分在一个组中，并将他们的value进行聚合
    // scala语言中一般的聚合操作都是两两聚合，spark基于scala开发的，所以它的聚合也是两两聚合
    // [1,2,3]
    // [3,3]
    // [6]
    // reduceByKey 中，如果key的数据只有一个，是不会参与运算的
    val reduceRDD = rdd.reduceByKey((x: Int, y: Int) => {
      x + y
    })
    reduceRDD.collect().foreach(println)
    sc.stop()

  }
}
