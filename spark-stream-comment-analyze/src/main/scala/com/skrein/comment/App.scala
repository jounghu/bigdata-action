package com.skrein.comment

import com.skrein.comment.service.{FenciService, OffsetService}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

/**
 *
 *
 * @author :hujiansong  
 * @date :2019/10/28 13:22
 * @since :1.8
 *
 */
object App {

  val logger = LoggerFactory.getLogger(App.getClass);

  def processRDD(rdd: RDD[ConsumerRecord[String, String]]) = {
    logger.warn("enter processRDD")
    val data = rdd.flatMap(rd => FenciService.fenci(rd.value())).reduceByKey(_ + _)
    data.foreachPartition(par => {
      val conn = OffsetService.getConn()
      conn.setAutoCommit(false)
      val stat = conn.createStatement()
      par.foreach(r => {
        val sql = s"INSERT INTO keywords (keyword,count) VALUES('${r._1}',${r._2}) on DUPLICATE key update count = count + ${r._2};"
        logger.warn("insert keywords SQL: {}", sql)
        stat.addBatch(sql)
      })
      stat.executeBatch()
      conn.commit()
    })
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("CommentFrequentCount")
    val ssc = new StreamingContext(conf, Seconds(20))


    val group = "comment-kafka-group"
    val topics = Array("comment1")


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val offsetMap = OffsetService.getOffset(group, topics(0))

    logger.warn("reset offsetMap {}", offsetMap)
    val consumerStrategy =
      if (offsetMap.isEmpty) {
        Subscribe[String, String](topics, kafkaParams)
      } else {
        Subscribe[String, String](topics, kafkaParams, offsetMap)
      }

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      consumerStrategy
    )


    stream.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      if (!rdd.isEmpty()) {
        processRDD(rdd)
      }
      OffsetService.updateOffset(offsetRanges, group)
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
