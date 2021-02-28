package com.atguigu.gmall1021.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall1021.realtime.app.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis



/**
 * @Author: LiKai
 * @Date: 2021/2/28 12:28 
 * @Description:
 */
object DauApp {

  def main(args: Array[String]): Unit = {

    // 1. sparkstreaming 要能够消费kafka
    val sparkconf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[*]")

    val ssc = new StreamingContext(sparkconf, Seconds(5))

    // 2. 通过工具类 获得Kafka数据流
    val topic = "ODS_BASE_LOG"
    val groupId = "dau_group"
    val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)

  // inputDStream.map(_.value()).print()

    // 3. 统计用户的活跃信息 -- 首次访问 dau uv
    // 1) 可以通过判断 日志中page栏位是否有 last_page_id来决定该页面为首次访问页面
    // 2)  也可以通过启用日志来判断 是否首次访问

    // 先转换格式，转换成方便操作的jsonObj
    val logJsonDstream: DStream[JSONObject] = inputDStream.map {
      record =>
        val jsonString: String = record.value()
        val logJsonObj: JSONObject = JSON.parseObject(jsonString)
        // 把ts 转换 成日期和小时
        val ts: lang.Long = logJsonObj.getLong("ts")
        val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
        val dateHourString: String = simpleDateFormat.format(new Date(ts))
        val dt = dateHourString.split(" ")(0)
        val hr = dateHourString.split(" ")(1)
        logJsonObj.put("dt",dt)
        logJsonObj.put("hr",hr)
        logJsonObj
    }

    // 过滤 得到每次会话的第一个访问页面
    val firstPageDsrem: DStream[JSONObject] = logJsonDstream.filter {
      logJsonObj =>
        var isFirstPage = false
        // 有page元素，但是page元素中没有last_page_id 的要留（返回true） 其他的过滤掉（返回false）
        val pageJsonObj = logJsonObj.getJSONObject("page")
        if (pageJsonObj != null) {
          val lastPageid: String = pageJsonObj.getString("last_page_id")
          if (lastPageid == null) {
            isFirstPage = true
          }
        }
        isFirstPage
    }

//   //
//    firstPageDsrem.transform{rdd =>{
//      println("过滤前" + rdd.count())
//      rdd
//    }
//    }


    // 识别标志
    // mid 设备编号 作假比较难 一般芯片是写死的 含金量高些。弊端 手机 笔记本 平板可以同时登陆 可以归并但比较麻烦
    // uid 用户编号 弊端：容易掺水 一个设备可以注册多个账号

    // 要把每次会话的首页 --去重--> 当日的首次访问（日活）
    // 如何去重： 本质就是一种识别 识别没条日志的主体（mid） 对于当日来说是不是已经来过了
    // 如何保存用户的访问清单：  1 rdis        XXX 2 masql  3 hbase 4 es  5 hive  6 hdfs 7 kafka 8
    //  2 算子 updateStateByKey ----> 内部实时存储 checkpoint  --> 日期 + mid
    //                      -----> 弊端 存储在checkpoint 不方便管理 2 容易非常臃肿

    // 3 如何把每天用户访问清单保存在redis中
    // type?       key?       value?    读写api？    过期时间？

    // 1) type?     string || set
    // 2) key?      日期 dau：2021-02027
    // 3) value?    mid
    // 4) 读写api？ 读 sadd（返回0） 写 sadd   即做了判断又执行了写入
    // 5) 过期时间？ 24h

    val dauJsonDstream: DStream[JSONObject] = firstPageDsrem.filter {
      logJsonObj =>
        val jedis = new Jedis("hadoop102", 6379)
        // 设定key 每天一个清单 所以每个日期有一个key
        val dauKey = "dau" + logJsonObj.getString("dt")
        // 从json 中取得mid
        val mid: String = logJsonObj.getJSONObject("common").getString("mid")
        // sadd 如果清单中没有返回1：表示新数据  有返回0：表示清单中已有 去掉
        val isFirstVisit: lang.Long = jedis.sadd(dauKey, mid)
        if (isFirstVisit == 1L) {
          println("用户： " + mid + "首次访问保留")
          true
        } else {
          println("用户： " + mid + "已重复，清洗")
          false
        }
    }

    dauJsonDstream.print(1000)

    ssc.start()
    ssc.awaitTermination()
  }

}
