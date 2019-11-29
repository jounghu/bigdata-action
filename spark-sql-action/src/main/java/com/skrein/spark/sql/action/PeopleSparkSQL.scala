package com.skrein.spark.sql.action

import java.util.Properties

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 *
 *
 * @author :hujiansong  
 * @date :2019/11/28 14:57
 * @since :1.8
 *
 */
object PeopleSparkSQL {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("people sql")

    val sc = SparkSession.builder().config(conf).getOrCreate()
    val rdd = sc.sparkContext.textFile("C:\\A-Dev\\bigdata\\bigdata-action\\spark-sql-action\\src\\main\\resources\\people.txt")
      .map(line => {
        val peopleVal = line.split(" ")
        (peopleVal(0), peopleVal(1), peopleVal(2))
      })
    import sc.implicits._

    val df = rdd.toDF("id", "sex", "height")

    df.createOrReplaceTempView("people")

    //用 SQL 语句的方式统计男性中身高超过 180cm 的人数。
    val df1 = sc.sql("SELECT COUNT(1) FROM people where sex = 'M' and height >= 180")
    df1.show()


    // 用 SQL 语句的方式统计女性中身高超过 170cm 的人数。
    val df2 = sc.sql("SELECT COUNT(1) FROM people where sex = 'F' and height >= 170")
    df2.show()

    // 对人群按照性别分组并统计男女人数。
    val df3 = sc.sql("SELECT sex, COUNT(1) as total FROM people group by sex")

    df3.show()

    //

    val df4 = df.filter(row => row.getString(1).equals("M")).filter(row => row.getString(2).toLong > 160)

    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","123456")

    val jdbcDF = sc.read
      .option("driver", "com.mysql.jdbc.Driver")
      .jdbc("jdbc:mysql://10.191.80.139:3307/sunrise", "at_campaign",prop)

    println("jdbc.................")
    jdbcDF.show()


    val jdbcDF1 = jdbcDF.select("id","company_id","name","app_package_id","channel_id")

    jdbcDF1.write
      .mode(SaveMode.Overwrite)
      .format("jdbc")
      .option("url","jdbc:mysql://10.191.80.139:3307/sunrise")
      .option("user","root")
      .option("password","123456")
      .option(JDBCOptions.JDBC_TABLE_NAME,"at_campaign_bak")
      .save()









  }
}
