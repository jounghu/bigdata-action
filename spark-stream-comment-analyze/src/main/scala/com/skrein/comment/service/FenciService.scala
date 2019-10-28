package com.skrein.comment.service

import java.net.URLEncoder

import com.alibaba.fastjson.{JSON, JSONArray}
import org.slf4j.LoggerFactory

import scala.collection.mutable


/**
 *
 *
 * @author :hujiansong  
 * @date :2019/10/28 14:24
 * @since :1.8
 *
 */
object FenciService {
  val logger = LoggerFactory.getLogger(FenciService.getClass)

  def fenci(text: String,
            connectTimeout: Int = 5000,
            readTimeout: Int = 5000,
            requestMethod: String = "GET"): Map[String, Int] = {
    import java.net.{HttpURLConnection, URL}
    val trimText = URLEncoder.encode(text.replaceAll(" ", ""), "utf-8")
    val url = s"http://localhost:9999/fenci/${trimText}"
    val connection = (new URL(url)).openConnection.asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(connectTimeout)
    connection.setReadTimeout(readTimeout)
    connection.setRequestMethod(requestMethod)
    connection.setRequestProperty("Accept", "application/json")
    var keywords = Map[String, Int]()
    var content: String = null
    try {
      val inputStream = connection.getInputStream
      content = scala.io.Source.fromInputStream(inputStream).mkString
      if (inputStream != null) inputStream.close
      val resp = JSON.parseObject(content)
      val words = resp.get("data").asInstanceOf[JSONArray]
      val data = mutable.ArrayBuffer[String]()
      for (i <- 0 until words.size()) {
        data.append(words.getString(i))
      }
      for (elem <- data) {
        keywords += elem -> 1
      }
    } catch {
      case e: Exception => {
        logger.error(s"fenci error ${content}", e)
      }
    }
    keywords
  }

  def main(args: Array[String]): Unit = {
    println(fenci("我今天吃的肉包子"))
  }

}
