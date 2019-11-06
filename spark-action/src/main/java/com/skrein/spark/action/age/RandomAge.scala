package com.skrein.spark.action.age

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
object RandomAge {
  def main(args: Array[String]): Unit = {
    val num = 10000000L
    val size = 2000000
    var writer: PrintWriter = null
    var a: Long = 0L
    for (a <- 0 to num.toInt) {
      if (a % size == 0) {
        if (writer != null) {
          writer.close()
        }
        writer = new PrintWriter(new File(s"C:\\A-Dev\\bigdata\\bigdata-action\\ages\\${a / 10000}"))
      }

      writer.write(s"${a} ${new Random().nextInt(100) + 1}")
      writer.write("\n")
    }
    if (writer != null) {
      writer.close()
    }
  }
}
