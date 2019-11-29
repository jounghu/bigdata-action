package com.skrein.spark.sql.action.weibo

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author :hujiansong  
 * @date :2019/11/7 9:55
 * @since :1.8
 *
 */
object WeiboApps {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("weibo apps").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("C:\\A-Dev\\bigdata\\bigdata-action\\aWeibo\\")
    val groupArray = rdd.map(line => {
      val weiboPerson = line.split(" ")
      val topic = weiboPerson(0)
      val userId = weiboPerson(1)
      val fansNum = weiboPerson.length - 2

      (topic,userId,fansNum)
    }).map(person=>((person._1,person._2),person._3)).sortBy(person=>person._2,false).groupBy(person=>person._1._1)

    groupArray.foreach(println)

  }
}
