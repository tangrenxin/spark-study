package com.trx.bigdata.rdd.operator.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @Description: key-value类型
 *               partitionBy
 * @Author: tangrenxin
 * @Date: 2021/3/7 23:38
 */
object Spark14_RDD_Operator_Transform_PartitionBy {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子
    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    // 转换成 key-value 形式的数据结构 才能使用partitionby
    val mapRDD = rdd.map((_, 1))
    //  rdd => PairRDDFunctions
    // 隐式转换
    // partitionBy 根据指定的分区规则 对数据进行重分区
    //  HashPartitioner 是一个分区器
    mapRDD.partitionBy(new HashPartitioner(3)).saveAsTextFile("output")
    sc.stop()

  }
}
