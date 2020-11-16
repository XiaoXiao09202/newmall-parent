package com.xiao.newmall.realtime.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis


object OffsetManager {

  //从redis中读取偏移量
  def getOffset(topicName:String,groupId:String):Map[TopicPartition,Long] ={
    //Redis中偏移量的保存格式
    // type?  hash
    // key?  "offset:[topic]:[groupid]"
    // field  partition
    // value  offset
    // expire
    val jedis: Jedis = XxRedisUtil.getJedisClient
    val offsetKey = s"offset:${topicName}:${groupId}"
    val offsetMap: util.Map[String, String] = jedis.hgetAll(offsetKey)
    jedis.close()
    import  scala.collection.JavaConversions._
    val kafkaOffsetMap: Map[TopicPartition, Long] = offsetMap.map { case (partitionId, offset) =>
      println(s"加载分区偏移量：${partitionId}:${offset}")
      (new TopicPartition(topicName, partitionId.toInt), offset.toLong)
    }.toMap
    kafkaOffsetMap
  }


  def saveOffset(topicName:String,groupId:String,offsetRanges:Array[OffsetRange])={
    // Redis 偏移量写入
    val offsetKey = s"offset:${topicName}:${groupId}"
    val offsetMap: util.HashMap[String, String] = new util.HashMap()

    //转换结构 offsetRanges -> map
    for (offset <- offsetRanges) {
      val partition: Int = offset.partition
      val untilOffset: Long = offset.untilOffset
      offsetMap.put(partition.toString,untilOffset.toString)
      println(s"写入分区：${partition}:${offset.fromOffset}-->${offset.untilOffset}")
    }

    //写入Redis
    if(offsetMap != null && offsetMap.size()>0){
      val jedis: Jedis = XxRedisUtil.getJedisClient
      jedis.hmset(offsetKey,offsetMap)
      jedis.close()
    }



  }
}