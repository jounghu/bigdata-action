package com.skrein.spark.sql.action.sex

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author :hujiansong  
 * @date :2019/11/6 18:22
 * @since :1.8
 *
 */
object SparkAgeSex {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SexAge").setMaster("local[*]")

    val sc = new SparkContext(conf)

//    val rdd = sc.textFile("hdfs://hdp1:9000/ageSex/in1/")
//    val rdd = sc.textFile("hdfs://hadoop-320:9000/tmp/ageSex/in/")
    val rdd = sc.textFile("C:\\A-Dev\\bigdata\\bigdata-action\\ageSex").persist()



    val manRdd = rdd.map(line => {
      val person = line.split(" ")
      (person(1), person(2).toInt)
    }).filter(person => "F".equals(person._1))

    val womanRdd = rdd.map(line => {
      val person = line.split(" ")
      (person(1), person(2).toInt)
    }).filter(person => "M".equals(person._1))


    val manRddMin = manRdd.map(person=>(person._2,person._1)).sortByKey(true).first
    val manRddMax = manRdd.map(person=>(person._2,person._1)).sortByKey(false).first


    val womanRddMin = womanRdd.map(person=>(person._2,person._1)).sortByKey(true).first
    val womanRddMax = womanRdd.map(person=>(person._2,person._1)).sortByKey(false).first

    println(s"man count ${manRdd.count()}")
    println(s"woman count ${womanRdd.count()}")
    println(s"manRdd min height ${manRddMin._1}")
    println(s"manRdd max height ${manRddMax._1}")
    println(s"womanRdd min height ${womanRddMin._1}")
    println(s"womanRdd max height ${womanRddMax._1}")

  }
}
