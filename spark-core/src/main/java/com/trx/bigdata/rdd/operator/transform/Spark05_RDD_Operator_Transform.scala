package com.trx.bigdata.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Description: glom
 * @Author: tangrenxin
 * @Date: 2021/3/7 23:38
 */
object Spark05_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子 glom
    val rdd = sc.makeRDD(List(1,2,3,4),2)

    val glomRDD = rdd.glom()
//    glomRDD.collect().foreach(println)
    glomRDD.collect().foreach(data=> println(data.mkString(",")))

    sc.stop()

  }
}
