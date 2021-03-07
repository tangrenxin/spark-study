package com.trx.bigdata.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Description: mapPartitionsWithIndex
 *               将待处理的数据以分区为单位发送到计算节点进行处理，
 *               这里的处理是指可以进行任意的处理，哪怕是过滤数据.数据处理是可以获取当前分区值
 * @Author: tangrenxin
 * @Date: 2021/3/7 23:38
 */
object Spark03_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子 mapPartitions
    // 获取第二个分区的数据
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)
    val mpRDD = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        if (index == 1) {
          iter
        }else{
          // Nil 表示空集合
          Nil.iterator
        }
      }
    )
    mpRDD.collect().foreach(println)

    sc.stop()

  }
}
