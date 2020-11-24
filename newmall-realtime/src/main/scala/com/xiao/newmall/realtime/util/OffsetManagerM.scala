package com.xiao.newmall.realtime.util

import com.alibaba.fastjson.JSONObject
import org.apache.kafka.common.TopicPartition

/**
  * mysql offset 管理
  */
object OffsetManagerM {

  def getOffset(topicName:String,groupId:String):Map[TopicPartition,Long] = {
    val sql = s"SELECT partition_id, topic_offset FROM offsets WHERE topic = '${topicName}' AND group_id='${groupId}'"
    val partitionOffsetList: List[JSONObject] = MySQLUtil.queryList(sql)

    val topicPartitionMap: Map[TopicPartition, Long] = partitionOffsetList.map { jsonObj =>
      val topicPartition: TopicPartition = new TopicPartition(topicName, jsonObj.getIntValue("partition_id"))
      val offset: Long = jsonObj.getLongValue("topic_offset")
      (topicPartition, offset)
    }.toMap
    topicPartitionMap
  }

}
