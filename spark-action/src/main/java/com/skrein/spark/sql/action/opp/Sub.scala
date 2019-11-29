package com.skrein.spark.sql.action.opp

/**
 *
 *
 * @author :hujiansong  
 * @date :2019/11/12 18:05
 * @since :1.8
 *
 */
class Sub extends Supper {

  def method()={
      println(this.name)
  }




}


object App{


  def main(args: Array[String]): Unit = {
    val sub = new Sub
    sub.method()

  }
}
