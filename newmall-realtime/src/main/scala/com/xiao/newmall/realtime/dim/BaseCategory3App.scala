package com.xiao.newmall.realtime.dim

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.xiao.newmall.realtime.bean.dim.{BaseCategory3, UserInfo}
import com.xiao.newmall.realtime.util.{OffsetManager, XxKafkaUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

object BaseCategory3App {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("dim_base_category3_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val groupId = "newmall_base_category3_group"
    val topic = "ODS_BASE_CATEGORY3"

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


    val  baseCategory3Dstream: DStream[BaseCategory3] = inputGetOffsetDstream.map { record =>
      val baseCategory3Str: String = record.value()
      val baseCategory3: BaseCategory3 = JSON.parseObject(baseCategory3Str, classOf[BaseCategory3])

      baseCategory3
    }

    baseCategory3Dstream.foreachRDD{rdd =>

      import org.apache.phoenix.spark._
      rdd.saveToPhoenix("NEWMALL_BASE_CATEGORY3",Seq("ID","NAME","CATEGORY2_ID"),
        new Configuration,Some("centos01,centos02,centos03:2181"))

      OffsetManager.saveOffset(topic,groupId,offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
