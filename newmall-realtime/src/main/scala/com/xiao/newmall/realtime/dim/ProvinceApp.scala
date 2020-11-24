package com.xiao.newmall.realtime.dim

import com.alibaba.fastjson.{JSON, JSONObject}
import com.xiao.newmall.realtime.bean.dim.ProvinceInfo
import com.xiao.newmall.realtime.util.{OffsetManager, XxKafkaUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.phoenix.spark._

/**
  * 消费kafka数据，保存省维度信息到HBase
  */
object ProvinceApp {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("province_info_app").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val groupId = "newmall_province_group"
    val topic = "ODS_BASE_PROVINCE"
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

    val provinceInfoDstream: DStream[ProvinceInfo] = inputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      val provinceInfo: ProvinceInfo = JSON.parseObject(jsonString, classOf[ProvinceInfo])
      provinceInfo
    }

    provinceInfoDstream.cache()

    provinceInfoDstream.print(1000)

    provinceInfoDstream.foreachRDD { rdd =>
      if(!rdd.isEmpty()){
        rdd.saveToPhoenix("NEWMALL_PROVINCE_INFO",
          Seq("ID", "NAME", "REGION_ID", "AREA_CODE","ISO_CODE","ISO_3166_2"),
          new Configuration,
          Some("centos01,centos02,centos03:2181"))
      }

      //偏移量提交
      OffsetManager.saveOffset(topic,groupId,offsetRanges)
    }
    ssc.start()
    ssc.awaitTermination()

  }
}
