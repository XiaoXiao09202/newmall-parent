package com.xiao.newmall.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.xiao.newmall.realtime.bean.DauInfo
import com.xiao.newmall.realtime.util.{OffsetManager, XxEsUtil, XxKafkaUtil, XxRedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object DauApp {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("dau_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    val topic = "NEWMAll_EVENT"
    val groupId = "DAU_GROUP"
    val kafkaOffsetMap: Map[TopicPartition, Long] = OffsetManager.getOffset(topic,groupId)
    var recordInputStream: InputDStream[ConsumerRecord[String, String]] = null
    if(kafkaOffsetMap != null && kafkaOffsetMap.size>0){
      recordInputStream = XxKafkaUtil.getKafkaStream(topic,ssc,kafkaOffsetMap,groupId)
    }else{
      recordInputStream = XxKafkaUtil.getKafkaStream(topic, ssc)
    }

    //得到本批次的偏移量的结束位置，用于更新redis中的偏移量
    var offsetRanges:Array[OffsetRange] = Array.empty[OffsetRange]
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = recordInputStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //driver端 周期性执行
      rdd
    }


    //1 增加year hour字段
    val jsonObjStream = inputGetOffsetDstream.map(record => {

      val value = record.value()
      val JSONObject = JSON.parseObject(value)
      val ts = JSONObject.getLong("ts")
      val dateStr: String = new SimpleDateFormat("yyyy-MM-dd HH mm").format(new Date(ts))
      val yearAndHour = dateStr.split(" ")

      JSONObject.put("dt", yearAndHour(0))
      JSONObject.put("hr", yearAndHour(1))
      JSONObject.put("mi", yearAndHour(2))

      JSONObject
    })

    // 2 去重思路：利用redis保存今天访问过系统的用户清单
    //清单在redis中保存
    //redis：type？ string hash list set zset     key？       value？(filed? score?)  expire?
 /*   val filteredDstream: DStream[JSONObject] = jsonObjStream.filter { jsonObj =>
      val dt: String = jsonObj.getString("dt")
      val mid: String = jsonObj.getJSONObject("common").getString("mid")
      val jedis: Jedis = XxRedisUtil.getJedisClient
      val dayKey = "dau:" + dt
      val isNew: lang.Long = jedis.sadd(dayKey, mid) //如果未存在则保存 返回1 如果已经存在则不保存 返回0
      jedis.close()
      if (isNew == 1L) {
        true
      } else {
        false
      }

    }*/

    /*
    1.使用mapPartitions替代filter算子 每个分区获取一个redis连接
    2.Iterator只能迭代一次，如果想多次使用可先转换为List
     */
    val filteredDstream: DStream[JSONObject] = jsonObjStream.mapPartitions(jsonObjItr => {

      val jedis: Jedis = XxRedisUtil.getJedisClient //一个分区只申请一个连接
      val filteredList = new ListBuffer[JSONObject]()
      val jsonList: List[JSONObject] = jsonObjItr.toList

      //println("过滤前：" + jsonList.size)

      for (jsonObj <- jsonList) {
        val dt: String = jsonObj.getString("dt")
        val mid: String = jsonObj.getJSONObject("common").getString("mid")
        val dayKey = "dau:" + dt
        val isNew: lang.Long = jedis.sadd(dayKey, mid) //如果未存在则保存 返回1 如果已经存在则不保存 返回0
        jedis.expire(dayKey, 3600 * 24)
        if (isNew == 1L) {
          filteredList.append(jsonObj)
        }
      }

      jedis.close()

      //println("过滤后：" + filteredList.size)

      filteredList.iterator
    })

    //3 保存ES 使用action算子
    filteredDstream.foreachRDD(rdd=>{
      rdd.foreachPartition(jsonObjIter=>{
        val list: List[JSONObject] = jsonObjIter.toList

        //把源数据保存成要保存的数据格式
        val duaList: List[(String,DauInfo)] = list.map(jsonObj => {

          val commonJsonObj: JSONObject = jsonObj.getJSONObject("common")

          val dauinfo = DauInfo(
            commonJsonObj.getString("mid"),
            commonJsonObj.getString("uid"),
            commonJsonObj.getString("ar"),
            commonJsonObj.getString("ch"),
            commonJsonObj.getString("vc"),
            jsonObj.getString("dt"),
            jsonObj.getString("hr"),
            jsonObj.getString("mi"),
            jsonObj.getLong("ts")
          )
          (dauinfo.mid,dauinfo)
        })

        val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
        XxEsUtil.bulkDoc(duaList,"gmall_dau_info"+dt)

      })
      //偏移量提交
      OffsetManager.saveOffset(topic,groupId,offsetRanges)
    })

    ssc.start()
    ssc.awaitTermination()


  }
}
