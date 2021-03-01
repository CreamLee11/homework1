package com.atguigu.gmall1021.realtime.app.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import scala.collection.mutable

/**
 * @Author: LiKai
 * @Date: 2021/3/1 14:14 
 * @Description:
 */
object OffsetManageUtil {

  /**
   * 读取偏移量
   *
   * @param topic   主题
   * @param groupId 消费者组
   * @return 每个分区的偏移量
   */


  def getOffset(topic: String, groupId: String): Map[TopicPartition, Long] = {
    val jedis: Jedis = RedisUtil.getJedisClient
    // redis 中 偏移量的存储结构
    // type？ hash
    // key？  offset:[topic1]:[group1]
    // field？ partitionNum
    // value？ offset
    // 读取api? hgetall
    // 写入api？
    // 过期时间？不设时间

    // todo 1 从redis中查询偏移量

    val key = "offset:" + topic + ":" + groupId
    val offsetMap: util.Map[String, String] = jedis.hgetAll(key)

    jedis.close()

    // todo 2 把 redis查询的结构 转换为 未来kafka要用哪个的偏移量结构

    // 把 Java的Map 转成sacla的map
    import collection.JavaConverters._

    val topicPartitiomMap: mutable.Map[TopicPartition, Long] = offsetMap.asScala.map {
      case (partition, offset) =>
        val topicPartition: TopicPartition = new TopicPartition(topic, partition.toInt)
        println("偏移量读取： 分区：" + partition + "偏移量起始点" + offset)
        (topicPartition, offset.toLong)
    }
    topicPartitiomMap.toMap // 可变 转为 不可变
  }

  /**
   * 存储偏移量
   *
   * @param topic   主题
   * @param groupId 消费者组
   * @return 每个分区的偏移量结束点
   */

  def saveOffset(topic: String, groupId: String, offsetRanges: Array[OffsetRange]): Unit = {

    val jedis: Jedis = RedisUtil.getJedisClient
    // redis 中 偏移量的存储结构
    // type？ hash
    // key？  offset:[topic1]:[group1]
    // field？ partitionNum
    // value？ offset
    // 读取api？ hgetall
    // 写入api？ hset/hmset 新版本中通用
    // 过期时间？不设时间

    // 1 把 offsetRange 转换成 redis的偏移量存储结构
    val offsetMap: util.Map[String, String] = new util.HashMap[String, String] // 此处加一下泛型，后边直接加offsetMap
    for (offsetRange <- offsetRanges) {
      val partition: Int = offsetRange.partition
      val untilOffset: Long = offsetRange.untilOffset
      println("偏移量写入： 分区：" + partition + "偏移量结束" + untilOffset)
      offsetMap.put(partition.toString, untilOffset.toString)
    }
    // 2 存储到redis中
    val key = "offset:" + topic + ":" + groupId
    jedis hmset(key, offsetMap)

    jedis.close()
  }

}
