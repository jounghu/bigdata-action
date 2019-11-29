package com.skrein.spark.sql.action

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 *
 *
 * @author :hujiansong  
 * @date :2019/11/7 15:09
 * @since :1.8
 *
 */
object SparkSqlApp {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("HiveSupport")
      .master("local[2]")
      .enableHiveSupport() //开启支持hive
      .getOrCreate()

    val scoreDF = spark.sql("select * from test_hive.score")

    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "123456")
    scoreDF.write.mode(SaveMode.Overwrite)
      .option("driver", "com.mysql.jdbc.Driver")
      .jdbc("jdbc:mysql://hdp1:3306/test", "score", properties)


  }
}
