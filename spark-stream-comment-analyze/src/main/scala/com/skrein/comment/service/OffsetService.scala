package com.skrein.comment.service

import java.sql.{Connection, DriverManager}

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
 *
 *
 * @author :hujiansong  
 * @date :2019/10/28 13:42
 * @since :1.8
 *
 */
object OffsetService {
  val logger = LoggerFactory.getLogger(OffsetService.getClass)

  def updateOffset(offsetRanges: Array[OffsetRange], groupName: String): Unit = {
    offsetRanges.foreach(offsetRange => {
      val conn = getConn()
      val sql = s"insert into offsets(group_name,topic_name,partition_index,offset_val) values (?,?,?,?)  on DUPLICATE key UPDATE offset_val = ${offsetRange.untilOffset}"
      val prep = conn.prepareStatement(sql)
      logger.warn("exec offset sql:  {}", sql)
      prep.setString(1, groupName)
      prep.setString(2, offsetRange.topic)
      prep.setInt(3, offsetRange.partition.toInt)
      prep.setLong(4, offsetRange.untilOffset.toLong)
      prep.execute()
    })
  }


  def getOffset(group: String, topic: String): mutable.Map[TopicPartition, Long] = {
    val topicOffsetMap = mutable.Map[TopicPartition, Long]()

    val conn = getConn()
    val sql = "select partition_index, offset_val from offsets where group_name=? and topic_name =?"
    logger.warn("select offset sql:  {}", sql)
    val ps = conn.prepareStatement(sql)
    ps.setString(1, group)
    ps.setString(2, topic)
    try {
      val rs = ps.executeQuery()
      while (rs.next()) {
        val partition = rs.getInt("partition_index")
        val offset = rs.getLong("offset_val")
        topicOffsetMap.put(new TopicPartition(topic, partition), offset)
      }
    } finally {
      conn.close()
    }
    topicOffsetMap
  }


  def getConn(): Connection = {
    val url = "jdbc:mysql://localhost:3306/spark?useSSL=false"
    val username = "root"
    val password = "Qwe123###"
    classOf[com.mysql.jdbc.Driver]
    DriverManager.getConnection(url, username, password)
  }

  def main(args: Array[String]): Unit = {
    val offset = getOffset("123", "213")
    offset.foreach(println)
  }

}
