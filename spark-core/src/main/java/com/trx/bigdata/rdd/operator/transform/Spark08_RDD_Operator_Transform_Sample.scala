package com.trx.bigdata.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Description: Sample
 *               根据指定的规则，从数据集中抽取数据
 * @Author: tangrenxin
 * @Date: 2021/3/7 23:38
 */
object Spark08_RDD_Operator_Transform_Sample {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子 Sample
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    // sample 算子需要传递三个参数
    // 1.第一个表示，抽取数据后是否将数据放回ture（放回），false（丢弃）
    // 2.第二个表示，每条数据被抽取的概率
    //    基准值的概念：
    // 3.第三个表示，抽取数据是随机数算法的种子
    //    如果不传递第三个参数，那么使用当前系统时间
    println(rdd.sample(
      false,
      0.4
//      1
    ).collect().mkString(","))
    sc.stop()

  }
}
