package com.skrein.spark.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author :hujiansong  
 * @date :2019/11/5 16:10
 * @since :1.8
 *
 */
object SparkWordCount {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Spark Exercise: Spark Version Word Count Program")

    val sc = new SparkContext(conf)

    val rdd = sc.textFile("hdfs://hdp1:9000/hjs/hadoop")
    val count = rdd.flatMap(line => line.split(" ")).map((_, 1)).reduceByKey(_ + _)
    count.saveAsTextFile("hdfs://hdp1:9000/hjs/spark/out")
  }
}
