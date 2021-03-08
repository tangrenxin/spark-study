package com.trx.bigdata.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Description: repartition
 *               扩大分区
 * @Author: tangrenxin
 * @Date: 2021/3/7 23:38
 */
object Spark11_RDD_Operator_Transform_Repartition {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    // 能不能用这种方式扩充分区呢？
    // 不能，因为coalesce可以扩大分区，但是如果不进行shuffle操作，是没有意义的，不起作用
    //    val newRDD = rdd.coalesce(3)
    // 所以，想要实现扩大分区的效果，需要使用shuffle操作
    //    val newRDD = rdd.coalesce(3, true)

    // spark 提供了一个简化操作
    // 缩减分区：使用coalesce，如果想要数据均衡，可以采用shuffle
    // 扩大分区：使用repartition 底层实现：coalesce(numPartitions, shuffle = true)
    //          底层代码调用的就是coalesce，而且肯定采用 shuffle
    val newRDD = rdd.repartition(3)
    newRDD.saveAsTextFile("output")
    sc.stop()

  }
}
