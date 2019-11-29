package com.skrein.spark.sql.action.weibo

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author :hujiansong  
 * @date :2019/11/11 11:20
 * @since :1.8
 *
 */
object MapMain {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Spark Exercise: Spark Version Word Count Program").setMaster("local")

    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Array(1,2,3,4))
    if(!rdd.isEmpty()){
      rdd.foreach(println)
    }

  }
}
