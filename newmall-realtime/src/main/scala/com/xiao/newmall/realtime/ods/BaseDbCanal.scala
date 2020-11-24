package com.xiao.newmall.realtime.ods

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.xiao.newmall.realtime.util.{OffsetManager, XxKafkaSink, XxKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

object BaseDbCanal {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("base_db_canal_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    val topic = "NEWMALL2020_DB_C"
    val groupId = "base_db_canal_group"
    val kafkaOffsetMap: Map[TopicPartition, Long] = OffsetManager.getOffset(topic,groupId)
    var recordInputStream: InputDStream[ConsumerRecord[String, String]] = null
    if(kafkaOffsetMap != null && kafkaOffsetMap.size>0){
      recordInputStream = XxKafkaUtil.getKafkaStream(topic,ssc,kafkaOffsetMap,groupId)
    }else{
      recordInputStream = XxKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }

    //得到本批次的偏移量的结束位置，用于更新redis中的偏移量
    var offsetRanges:Array[OffsetRange] = Array.empty[OffsetRange]
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = recordInputStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //driver端 周期性执行
      rdd
    }

    val jsonObjDstream: DStream[JSONObject] = inputGetOffsetDstream.map(record => {
      val jsonString: String = record.value()
      val jsonObject: JSONObject = JSON.parseObject(jsonString)
      jsonObject
    })

    jsonObjDstream.foreachRDD{rdd=>

      //推回Kafka
      rdd.foreach(jsonObj=>{
        val jsonArr: JSONArray = jsonObj.getJSONArray("data")
        val tableName: String = jsonObj.getString("table")
        val topic = s"ODS_${tableName.toUpperCase}"
        import scala.collection.JavaConversions._
        for(jsonObj <- jsonArr){
          println(jsonObj.toString)
          val msg: String = jsonObj.toString
          XxKafkaSink.send(topic,msg)     //非幂等的操作，可能会导致数据重复
        }
      })

      //偏移量提交
      OffsetManager.saveOffset(topic,groupId,offsetRanges)

    }

    ssc.start()
    ssc.awaitTermination()


  }
}
