package com.trx.bigdata.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Description:
 * @Author: tangrenxin
 * @Date: 2021/3/3 00:37
 */
object Spark01_WordCount {

  def main(args: Array[String]): Unit = {
    // Application
    // Spark 框架 是一个环境，用来运行spark程序的环境
    // 一、建立和spark框架的连接
    // SparkConf 是spark中的一些基础配置对象  .setMaster() 设置spark框架的运行环境
    val sparkConf = new SparkConf().setMaster("local").setAppName("TRXTest")
    val sc = new SparkContext(sparkConf) //环境对象
    // 二、执行业务操作
    // 1.读取文件，获取一行一行的数据
    // hello world
    val lines = sc.textFile("datas")
    // 2.将一行数据进行拆分，形成一个一个的单词（分词）
    // 扁平化:将整体拆分成个体的操作
    // "hello world" => hello,world,hello,world ...
    val words:RDD[String] = lines.flatMap(_.split(" "))
    // 3.将数据根据单词进行分组，便于统计
    // (hello,hello,hello...),(world,world...)
    val wordGroup = words.groupBy(word => word)
    // 4.对分组后的数据进行转换（聚合，求和...）在scala中将A变成B就是转换的意思
    // （hello,3）,(world,2)
    val wordToCount = wordGroup.map { // 模式匹配
      case (word, list) => {
        (word, list.size)
      }
    }
    // 5.将转换结果采集到控制台打印出来
    val array = wordToCount.collect() // 收集
    array.foreach(println)
    // 三、关闭连接
    sc.stop()

  }
}
