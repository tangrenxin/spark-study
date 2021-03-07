package com.trx.bigdata.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Description: map 并行计算效果演示
 * @Author: tangrenxin
 * @Date: 2021/3/7 23:38
 */
object Spark01_RDD_Operator_Transform_Par {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子 map
    // 1.rdd的计算，一个分区内的数据是一个一个的执行逻辑
    //    只有前面一个数据全部执行完毕后，才会执行下一个数据。
    //    分区内数据的执行是有序的
    // 2.不同分区，数据计算是无序的
    val rdd = sc.makeRDD(List(1, 2, 3, 4),2)
    val mapRDD = rdd.map(num => {
      println(">>>>>> " + num)
      num
    })

    val mapRDD1 = mapRDD.map(num => {
      println("###### " + num)
      num
    })

    mapRDD1.collect()

    /**
     * 分区为1时结果：
     * >>>>>> 1
     * ###### 1
     * >>>>>> 2
     * ###### 2
     * >>>>>> 3
     * ###### 3
     * >>>>>> 4
     * ###### 4
     *
     * 分区为2时结果：
     * >>>>>> 3
     * ###### 3
     * >>>>>> 4
     * ###### 4
     * >>>>>> 1
     * ###### 1
     * >>>>>> 2
     * ###### 2
     */


    sc.stop()

  }
}
