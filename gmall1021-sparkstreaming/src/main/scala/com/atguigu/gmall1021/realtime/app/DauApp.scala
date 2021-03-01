package com.atguigu.gmall1021.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall1021.realtime.app.util.{MyKafkaUtil, OffsetManageUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer


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

    // todo 1 此处完成读取Redis中的偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManageUtil.getOffset(topic, groupId)

    // todo 2 此处改造成通过偏移量从指定位置消费kafka数据
    var inputDStream: InputDStream[ConsumerRecord[String, String]] = null
    // 如果有偏移量值按照偏移量取数据，如果没有偏移量，则按照默认取最新的数据
    if (offsetMap != null & offsetMap.size > 0) {
      inputDStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      inputDStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    // inputDStream.map(_.value()).print()

    // todo 3 在此处 在数据流转换前得到偏移量的结束点
    //offsetRanges 包含了该批次偏移量结束点
    var offsetRanges: Array[OffsetRange] = null // 存放的位置 driver？ √   ex？
    // 周期性的在driver中执行任务 每批次执行一次
    val inputWithOffsetDstream: DStream[ConsumerRecord[String, String]] = inputDStream.transform { rdd => // 转换算子
      // 把rdd强转为某个特质，利用特质的offsetRanges方法 得到偏移量结束点（offsetRanges)
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges // 在哪执行？ driver
      rdd
    }

    // 3. 统计用户的活跃信息 -- 首次访问 dau uv
    // 1) 可以通过判断 日志中page栏位是否有 last_page_id来决定该页面为首次访问页面
    // 2)  也可以通过启用日志来判断 是否首次访问

    // 先转换格式，转换成方便操作的jsonObj
    val logJsonDstream: DStream[JSONObject] = inputWithOffsetDstream.map {
      record =>
        val jsonString: String = record.value()
        val logJsonObj: JSONObject = JSON.parseObject(jsonString)
        // 把ts 转换 成日期和小时
        val ts: lang.Long = logJsonObj.getLong("ts")
        val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
        val dateHourString: String = simpleDateFormat.format(new Date(ts))
        val dt = dateHourString.split(" ")(0)
        val hr = dateHourString.split(" ")(1)
        logJsonObj.put("dt", dt)
        logJsonObj.put("hr", hr)
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

    //    val dauJsonDstream: DStream[JSONObject] = firstPageDsrem.filter {
    //      logJsonObj =>
    //        //        val jedis = new Jedis("hadoop102", 6379)
    //
    //        // 优化1  把new jedis 改为连接池方式 减少开辟连接次数
    //        val jedis = RedisUtil.getJedisClient
    //
    //
    //        // 设定key 每天一个清单 所以每个日期有一个key
    //        val dauKey = "dau" + logJsonObj.getString("dt")
    //        // 从json 中取得mid
    //        val mid: String = logJsonObj.getJSONObject("common").getString("mid")
    //        // sadd 如果清单中没有返回1：表示新数据  有返回0：表示清单中已有 去掉
    //        val isFirstVisit: lang.Long = jedis.sadd(dauKey, mid)
    //        jedis.close()
    //        if (isFirstVisit == 1L) {
    //          println("用户： " + mid + "首次访问保留")
    //          true
    //        } else {
    //          println("用户： " + mid + "已重复，清洗")
    //          false
    //        }
    //    }
    //


    // todo 优化2  使用 mapPartions迭代
    val dauDstream: DStream[JSONObject] = firstPageDsrem.mapPartitions {
      jsonItr =>
        // todo 优化1  把new jedis 改为连接池方式 减少开辟连接次数
        val jedis = RedisUtil.getJedisClient // 每个分区每个批次
        val duaList = new ListBuffer[JSONObject] // 存放过滤后的结果
        for (logJsonObj <- jsonItr) {

          val jedis = RedisUtil.getJedisClient
          // 设定key 每天一个清单 所以每个日期有一个key
          val dauKey = "dau" + logJsonObj.getString("dt")
          // 从json 中取得mid
          val mid: String = logJsonObj.getJSONObject("common").getString("mid")
          // sadd 如果清单中没有返回1：表示新数据  有返回0：表示清单中已有 去掉
          val isFirstVisit: lang.Long = jedis.sadd(dauKey, mid)
          jedis.close()
          if (isFirstVisit == 1L) {
            println("用户： " + mid + "首次访问保留")
            duaList.append(logJsonObj)
          } else {
            println("用户： " + mid + "已重复，清洗")

          }
        }
        jedis.close()
        duaList.toIterator
    }


    //dauDstream.print(1000)

    // todo 4 此处 把偏移量的结束点 更新到redis中 做为偏移量的提交

    dauDstream.foreachRDD { rdd =>
      rdd.foreachPartition { jsonObject =>
        for (jsonObj <- jsonObject) {
          println(jsonObj)
          // todo dr? ex? 执行频率？ 1 每条数据 2 每分区 3 每批次 4 一次
          // a OffsetManageUtil.saveOffset(topic,groupId,offsetRanges) // e 1
        }
        // b  OffsetManageUtil.saveOffset(topic,groupId,offsetRanges)// e 2
      }

      //  println("111")

      // 在driver中每批次执行一次
      OffsetManageUtil.saveOffset(topic, groupId, offsetRanges) // d 3
    }
    //  d  OffsetManageUtil.saveOffset(topic,groupId,offsetRanges)// d 4 整个程序启动时执行一次

    // println("222") // 此时2先打印

    ssc.start() // 此时流 才启动
    ssc.awaitTermination()
  }

}
