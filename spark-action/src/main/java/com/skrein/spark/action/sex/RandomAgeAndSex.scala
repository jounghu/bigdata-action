package com.skrein.spark.action.sex

import java.io.{File, PrintWriter}

import scala.util.Random

/**
 *
 *
 * @author :hujiansong  
 * @date :2019/11/5 17:45
 * @since :1.8
 *
 */
object RandomAgeAndSex {
  def main(args: Array[String]): Unit = {
    val num = 100000000L
    val size = 50000000
    var writer: PrintWriter = null
    var a: Long = 0L
    for (a <- 0 to num.toInt) {
      if (a % size == 0) {
        if (writer != null) {
          writer.close()
        }
        writer = new PrintWriter(new File(s"C:\\A-Dev\\bigdata\\bigdata-action\\ageSex\\${a / 10000}"))
      }
      val sex = a % 2 match {
        case 0 => "M"
        case 1 => "F"
      }
      writer.write(s"${a} ${sex} ${new Random().nextInt(20)  + 150}")
      writer.write("\n")
    }
    if (writer != null) {
      writer.close()
    }
  }
}
