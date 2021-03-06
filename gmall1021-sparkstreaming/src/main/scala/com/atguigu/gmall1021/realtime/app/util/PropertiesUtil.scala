package com.atguigu.gmall1021.realtime.app.util

import java.io.InputStreamReader
import java.util.Properties

/**
 * @Author: LiKai
 * @Date: 2021/2/28 17:54 
 * @Description:
 */
object PropertiesUtil {
  def main(args: Array[String]): Unit = {
    val properties: Properties =  PropertiesUtil.load("config.properties")
    println(properties.getProperty("kafka.broker.list"))
  }

  def load(propertieName:String): Properties ={
    val prop=new Properties();
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.
      getResourceAsStream(propertieName) , "UTF-8"))
    prop
  }


}
