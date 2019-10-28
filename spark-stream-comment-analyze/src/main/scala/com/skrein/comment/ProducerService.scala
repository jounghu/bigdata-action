package com.skrein.comment

import java.util.{Properties, UUID}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random

/**
 *
 *
 * @author :hujiansong  
 * @date :2019/10/28 17:44
 * @since :1.8
 *
 */
object ProducerService {
  def main(args: Array[String]): Unit = {

    val events = 10
    val topic = "comment1"
    val brokers = "localhost:9092"
    val rnd = new Random()
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("client.id", "wordFreqGenerator")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    val t = System.currentTimeMillis()

    // 读取汉字字典
    val source = scala.io.Source.fromFile("C:\\ad-dev\\spark-streaming-in-action\\bigdata-action\\spark-stream-comment-analyze\\src\\main\\resources\\fenci.txt")

      val lines = try source.mkString finally source.close()
    while (true) {
      for (nEvents <- Range(0, events)) {

        val sb = lines.split("\r\n")(nEvents)
        val data = new ProducerRecord[String, String](topic, UUID.randomUUID().toString, sb.toString())

        //async
        //producer.send(data, (m,e) => {})
        //sync
        producer.send(data)
      }
      Thread.sleep(2000)
    }

    System.out.println("sent per second: " + events * 1000 / (System.currentTimeMillis() - t))
    producer.close()


  }
}
