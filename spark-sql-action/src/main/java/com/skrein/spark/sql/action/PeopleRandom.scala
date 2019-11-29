package com.skrein.spark.sql.action

import java.io.{File, PrintWriter}
import java.util.Random

/**
 *
 *
 * @author :hujiansong  
 * @date :2019/11/28 14:37
 * @since :1.8
 *
 */
object PeopleRandom {
  def main(args: Array[String]): Unit = {
    val writer = new PrintWriter(new File("C:\\A-Dev\\bigdata\\bigdata-action\\spark-sql-action\\src\\main\\resources\\people.txt"))
    for (i <- 0 until 1000) {
      val gender = if (new Random().nextInt(Integer.MAX_VALUE) % 2 == 0) "M" else "F"
      val height = new Random().nextInt(30) + 160
      writer.write(i + " " + gender + " " + height + "\n")
    }
    writer.close()
  }
}
