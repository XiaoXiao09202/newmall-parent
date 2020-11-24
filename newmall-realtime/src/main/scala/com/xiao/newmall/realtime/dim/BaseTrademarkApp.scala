package com.xiao.newmall.realtime.dim

import com.alibaba.fastjson.JSON
import com.xiao.newmall.realtime.bean.dim.BaseTrademark
import com.xiao.newmall.realtime.util.{OffsetManager, XxKafkaUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object BaseTrademarkApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("dim_base_trademark_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val groupId = "newmall_base_trademark_group"
    val topic = "ODS_BASE_TRADEMARK"

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


    val  baseTrademarkDstream: DStream[BaseTrademark] = inputGetOffsetDstream.map { record =>
      val baseTrademarkJsonStr: String = record.value()
      val baseTrademark: BaseTrademark = JSON.parseObject(baseTrademarkJsonStr, classOf[BaseTrademark])

      baseTrademark
    }

    baseTrademarkDstream.foreachRDD{rdd =>

      import org.apache.phoenix.spark._
      rdd.saveToPhoenix("NEWMALL_BASE_TRADEMARK",Seq("TM_ID","TM_NAME"),
        new Configuration,Some("centos01,centos02,centos03:2181"))

      OffsetManager.saveOffset(topic,groupId,offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
