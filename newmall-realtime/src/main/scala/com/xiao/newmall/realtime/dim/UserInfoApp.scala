package com.xiao.newmall.realtime.dim

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.xiao.newmall.realtime.bean.dim.UserInfo
import com.xiao.newmall.realtime.util.{OffsetManager, XxKafkaUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

object UserInfoApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("dim_user_info_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val groupId = "newmall_user_info_group"
    val topic = "ODS_USER_INFO"

    // ===== 偏移量处理 =====
    val offsets: Map[TopicPartition, Long] = OffsetManager.getOffset(groupId, topic)

    var inputDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsets != null && offsets.size > 0) {
      inputDstream = XxKafkaUtil.getKafkaStream(topic, ssc, offsets, groupId)
    } else {
      inputDstream = XxKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //获得偏移结束点
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = inputDstream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }


    val userInfoDstream: DStream[UserInfo] = inputGetOffsetDstream.map { record =>
      val userInfoJsonStr: String = record.value()
      val userInfo: UserInfo = JSON.parseObject(userInfoJsonStr, classOf[UserInfo])
      val formattor = new SimpleDateFormat("yyyy-MM-dd")
      val date: Date = formattor.parse(userInfo.birthday)
      val curTs: Long = System.currentTimeMillis()
      val betweenMs: Long = curTs - date.getTime
      val age: Long = betweenMs / 1000L / 60L / 60L / 24L / 365L
      if (age < 20) {
        userInfo.age_group = "20岁及以下"
      } else if (age > 30) {
        userInfo.age_group = "30岁以上"
      } else {
        userInfo.age_group = "21岁到30岁"
      }

      if (userInfo.gender == "M") {
        userInfo.gender_name = "男"
      } else {
        userInfo.gender_name = "女"
      }
      userInfo
    }

    userInfoDstream.foreachRDD{rdd =>

      import org.apache.phoenix.spark._
      rdd.saveToPhoenix("NEWMALL_USER_INFO",Seq("ID","USER_LEVEL","BIRTHDAY","GENDER","AGE_GROUP","GENDER_NAME"),
        new Configuration,Some("centos01,centos02,centos03:2181"))

      OffsetManager.saveOffset(topic,groupId,offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()

  }
}
