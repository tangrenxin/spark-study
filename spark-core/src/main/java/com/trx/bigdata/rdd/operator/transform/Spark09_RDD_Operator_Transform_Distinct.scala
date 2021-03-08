package com.trx.bigdata.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Description: distinct
 *               去重数据
 * @Author: tangrenxin
 * @Date: 2021/3/7 23:38
 */
object Spark09_RDD_Operator_Transform_Distinct {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子 Sample
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 1, 2, 3, 4))
    // scala 集合的distinct函数去重的原理是，底层是用hashSet实现的，
    // 那么 RDD 中的去重方式什么呢？
    //源码： case _ => map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)
    // 1. map(x => (x, null)) :
    //  (1,null),(2,null),(3,null),(4,null),(1,null),(2,null),(3,null),(4,null)
    // 2. reduceByKey((x, _) => x, numPartitions)
    //  (1) (1,null)(1,null) key: 1 (null,null)
    //  (2) (null,null) => null value:null
    // 得到：(1,null)
    // 3. map(_._1) :
    // (1,null) => 1 最终得到 key : 1
    // 这就是RDD的distinct的执行原理
    val res = rdd.distinct()
    res.collect().foreach(println)
    sc.stop()

  }
}
