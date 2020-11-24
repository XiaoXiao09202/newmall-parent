package com.xiao.newmall.realtime.dim

import com.alibaba.fastjson.JSON
import com.xiao.newmall.realtime.bean.dim.SpuInfo
import com.xiao.newmall.realtime.util.{OffsetManager, XxKafkaUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SpuInfoApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("dim_spu_info_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val groupId = "newmall_spu_info_group"
    val topic = "ODS_SPU_INFO"

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


    val  spuInfoDstream: DStream[SpuInfo] = inputGetOffsetDstream.map { record =>
      val spuInfoJsonStr: String = record.value()
      val spuInfo: SpuInfo = JSON.parseObject(spuInfoJsonStr, classOf[SpuInfo])

      spuInfo
    }

    spuInfoDstream.foreachRDD{rdd =>

      import org.apache.phoenix.spark._
      rdd.saveToPhoenix("NEWMALL_SPU_INFO",Seq("ID","SPU_NAME"),
        new Configuration,Some("centos01,centos02,centos03:2181"))

      OffsetManager.saveOffset(topic,groupId,offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
