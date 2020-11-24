package com.xiao.newmall.realtime.dim

import com.alibaba.fastjson.{JSON, JSONObject}
import com.xiao.newmall.realtime.bean.dim.{BaseCategory3, SkuInfo}
import com.xiao.newmall.realtime.util.{OffsetManager, PhoenixUtil, XxKafkaUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SkuInfoApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("dim_sku_info_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val groupId = "newmall_sku_info_group"
    val topic = "ODS_SKU_INFO"

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


    val  objectDstream: DStream[SkuInfo] = inputGetOffsetDstream.map { record =>
      val skuInfoStr: String = record.value()
      val obj: SkuInfo = JSON.parseObject(skuInfoStr, classOf[SkuInfo])

      obj
    }

    val skuInfoDstream: DStream[SkuInfo] = objectDstream.transform { rdd =>

      if (rdd.count() > 0) {
        //category3
        val category3Sql = "select id,name from newmall_base_category3" //driver端 周期执行
        val category3List: List[JSONObject] = PhoenixUtil.queryList(category3Sql)
        val category3Map: Map[String, JSONObject] = category3List.map(jsonObj => (jsonObj.getString("ID"), jsonObj)).toMap

        //tm_name
        val tmSql = "select tm_id,tm_name from newmall_base_trademark" //driver端 周期执行
        val tmList: List[JSONObject] = PhoenixUtil.queryList(tmSql)
        val tmMap: Map[String, JSONObject] = tmList.map(jsonObj => (jsonObj.getString("TM_ID"), jsonObj)).toMap

        //spu
        val spuSql = "select id,spu_name from newmall_spu_info" //driver端 周期执行
        val spuList: List[JSONObject] = PhoenixUtil.queryList(spuSql)
        val spuMap: Map[String, JSONObject] = spuList.map(jsonObj => (jsonObj.getString("ID"), jsonObj)).toMap

        //汇总到一个list 广播这个map
        val dimList = List[Map[String, JSONObject]](category3Map, tmMap, spuMap)
        val dimBC: Broadcast[List[Map[String, JSONObject]]] = ssc.sparkContext.broadcast(dimList)

        val skuInfoRDD: RDD[SkuInfo] = rdd.mapPartitions { skuInfoItr => //executor
          val dimList: List[Map[String, JSONObject]] = dimBC.value //接收bc
        val category3Map: Map[String, JSONObject] = dimList(0)
          val tmMap: Map[String, JSONObject] = dimList(1)
          val spuMap: Map[String, JSONObject] = dimList(2)

          val skuInfoList: List[SkuInfo] = skuInfoItr.toList
          for (skuInfo <- skuInfoList) {

            val category3JsonObj: JSONObject = category3Map.getOrElse(skuInfo.category3_id, null)
            if (category3JsonObj != null) {
              skuInfo.category3_name = category3JsonObj.getString("NAME")
            }

            val tmJsonObj: JSONObject = tmMap.getOrElse(skuInfo.tm_id, null)
            if (tmJsonObj != null) {
              skuInfo.tm_name = tmJsonObj.getString("TM_NAME")
            }

            val spuJsonObj: JSONObject = spuMap.getOrElse(skuInfo.spu_id, null)
            if (spuJsonObj != null) {
              skuInfo.spu_name = spuJsonObj.getString("SPU_NAME")
            }
          }
          skuInfoList.iterator
        }
        skuInfoRDD
      } else {
        rdd
      }
    }

    skuInfoDstream.foreachRDD{rdd =>

      import org.apache.phoenix.spark._
      rdd.saveToPhoenix("NEWMALL_SKU_INFO",Seq("ID","SPU_ID","PRICE","SKU_NAME","TM_ID","CATEGORY3_ID","CREATE_TIME","CATEGORY3_NAME","SPU_NAME","TM_NAME"),
        new Configuration,Some("centos01,centos02,centos03:2181"))

      OffsetManager.saveOffset(topic,groupId,offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
