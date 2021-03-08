package com.trx.bigdata.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Description: groupBy
 *               用grouBy实现Wordcount
 * @Author: tangrenxin
 * @Date: 2021/3/7 23:38
 */
object Spark06_RDD_Operator_Transform_GroupBy_Test2 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子 groupBy
    val rdd = sc.textFile("datas/1*.txt")
    // 拆成一个一个的单词，输入一（行），返回多行，用flatMap，输入一返回一用map
    val words = rdd.flatMap(line => line.split(" "))
    // 相同单词放在一起
    val wordRdd = words.groupBy(word => word)
    wordRdd.collect().foreach(println)

    // 通过模式匹配计算 Word count
    val wordCount = wordRdd.map {
      case (word, iter) => {
        (word, iter.size)
      }
    }
    println("=================")
    wordCount.collect().foreach(println)
    sc.stop()

  }
}
