package com.skrein.spark.action.age

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author :hujiansong  
 * @date :2019/11/6 15:47
 * @since :1.8
 *
 */
object SparkAvgAge {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("avg age").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd = sc.textFile("hdfs://hdp1:9000/ages/in/")

    val ageSum = rdd.map(line => line.split(" ")(1).toInt).reduce(_+_)




    val personCount = rdd.count()

    println(s"totalAge ${ageSum} totalPerson ${personCount}")
    println(s"average is ${ageSum / personCount}")

  }
}
