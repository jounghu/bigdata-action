package com.skrein.spark.sql.action

import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Put}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.sql.SparkSession


/**
 *
 *
 * @author :hujiansong  
 * @date :2019/11/29 17:28
 * @since :1.8
 *
 */
case class People(id: String, gender: String, age: String) {
  override def toString: String = id + " " + gender + " " + age
}

object HbaseApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("bluck insert hbase")
      .master("local")
      .getOrCreate()


    val rdd = spark.sparkContext.textFile("C:\\A-Dev\\bigdata\\bigdata-action\\spark-sql-action\\src\\main\\resources\\people.txt", 2)

    rdd.map(line => {
      val peopleVal = line.split(" ")
      People(peopleVal(0), peopleVal(1), peopleVal(2))
    }).foreachPartition(p => {

      print(s"开始处理Parition${p}")

      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.set("hbase.zookeeper.quorum", "hdp1,hdp2,hdp3")
      //设置zookeeper连接端口，默认2181
      hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")

      val connect = ConnectionFactory.createConnection(hbaseConf)
      val tableName = "people"
      val table = connect.getTable(TableName.valueOf(tableName)).asInstanceOf[HTable]
      table.setAutoFlush(false, false)
      table.setWriteBufferSize(30 * 1024) // 30M flush

      p.foreach(people => {
        print(s"当前${people}")
        val put = new Put(people.id.getBytes())
        put.addColumn("info".getBytes(), "id".getBytes(), people.id.getBytes())
        put.addColumn("info".getBytes(), "gender".getBytes(), people.gender.getBytes())
        put.addColumn("info".getBytes(), "age".getBytes(), people.age.getBytes())
        table.put(put)
      })

      table.flushCommits()
      table.close()
    })


  }
}
