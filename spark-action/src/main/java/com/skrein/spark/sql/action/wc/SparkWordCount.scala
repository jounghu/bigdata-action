package com.skrein.spark.sql.action.wc

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author :hujiansong  
 * @date :2019/11/8 9:15
 * @since :1.8
 *
 */
object SparkWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wc").setMaster("local")

    val sc = new SparkContext(conf)


    val rdd = sc.textFile("D:\\spark\\in")


    rdd.flatMap(line=> line.split(" ")).map((_,1)).groupByKey().saveAsTextFile("hdfs://hdp1:9000/aaa")


    val map = scala.collection.mutable.Map((111,11))


  }
}
