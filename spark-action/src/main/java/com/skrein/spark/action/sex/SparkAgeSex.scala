package com.skrein.spark.action.sex

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
    val conf = new SparkConf().setAppName("SexAge")

    val sc = new SparkContext(conf)

    val rdd = sc.textFile("hdfs://hdp1:9000/ageSex/in1/")

//    // 统计男女性别
//    val sexRdd = rdd.map(line => (line.split(" ")(1), 1)).reduceByKey(_ + _).map(sexCount => {
//      val gender = sexCount._1 match {
//        case "M" => "女"
//        case "F" => "男"
//      }
//      (gender, sexCount._2)
//    })
//    println(sexRdd)
//    sexRdd.repartition(1).saveAsTextFile("hdfs://hdp1:9000/ageSex/out1/sexCount.txt")


    //    val personRdd = rdd.map(line => {
    //      val person = line.split(" ")
    //      (person(1), person(2).toInt)
    //    }).cache()
    //
    //    val manRdd = personRdd.filter(person => "F".equals(person._1)).cache().repartition(1)
    //
    //    val minManHeight = manRdd.sortBy(_._2, ascending = true).take(1)
    //
    //    sc.makeRDD(minManHeight).saveAsTextFile("hdfs://hdp1:9000/ageSex/out/manMinHeight.txt")
    //
    //
    //    val maxManHeight = manRdd.sortBy(_._2, ascending = false).take(1)
    //    sc.makeRDD(maxManHeight).saveAsTextFile("hdfs://hdp1:9000/ageSex/out/manMaxHeight.txt")
    //
    //
    //
    //
    //    val womanRdd = personRdd.filter(person => "M".equals(person._1)).cache().repartition(1)
    //
    //    val minWomanHeight = womanRdd.sortBy(_._2, ascending = true).take(1)
    //
    //    sc.makeRDD(minWomanHeight).saveAsTextFile("hdfs://hdp1:9000/ageSex/out/womanMinHeight.txt")
    //
    //
    //    val maxWomanHeight = manRdd.sortBy(_._2, ascending = false).take(1)
    //    sc.makeRDD(maxWomanHeight).saveAsTextFile("hdfs://hdp1:9000/ageSex/out/womanMaxHeight.txt")

    val manRdd = rdd.map(line => {
      val person = line.split(" ")
      (person(1), person(2).toInt)
    }).filter(person => "F".equals(person._1)).repartition(1)

    val womanRdd = rdd.map(line => {
      val person = line.split(" ")
      (person(1), person(2).toInt)
    }).filter(person => "M".equals(person._1)).repartition(1)


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
