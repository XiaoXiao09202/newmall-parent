package com.xiao.newmall.realtime.ads

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.alibaba.fastjson.{JSON, JSONObject}
import com.xiao.newmall.realtime.bean.OrderDetailWide
import com.xiao.newmall.realtime.util.{OffsetManagerM, XxKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs

/**
  * 品牌销售额实时统计
  */
object TrademarkStatApp {
  def main(args: Array[String]): Unit = {
    //===加载流====
    val sparkConf = new SparkConf().setAppName("trademark_stat_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    val topic = "DWS_ORDER_WIDE"
    val groupId = "ads_trademark_stat_group"
    //使用mysql中的偏移量加载
    val kafkaOffsetMap: Map[TopicPartition, Long] = OffsetManagerM.getOffset(topic,groupId)
    var recordInputStream: InputDStream[ConsumerRecord[String, String]] = null
    if(kafkaOffsetMap != null && kafkaOffsetMap.size>0){
      recordInputStream = XxKafkaUtil.getKafkaStream(topic,ssc,kafkaOffsetMap,groupId)
    }else{
      recordInputStream = XxKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }
    //得到本批次的偏移量的结束位置，用于更新偏移量
    var offsetRanges:Array[OffsetRange] = Array.empty[OffsetRange]
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = recordInputStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //driver端 周期性执行
      rdd
    }

    val jsonObjDstream: DStream[OrderDetailWide] = inputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      val jsonObj: OrderDetailWide = JSON.parseObject(jsonString,classOf[OrderDetailWide])
      jsonObj
    }

    //////////////聚合操作////////////

    val amountWithTmDstream: DStream[(String, Double)] = jsonObjDstream.map(orderWide=>(orderWide.tm_id+":"+orderWide.tm_name,orderWide.final_detail_amount))

    val amountByTmDstream: DStream[(String, Double)] = amountWithTmDstream.reduceByKey(_+_)

    ///////////存储    本地事务方式////////////
    amountByTmDstream.foreachRDD{rdd=>
      val amountArray: Array[(String, Double)] = rdd.collect()
      if(amountArray != null && amountArray.size > 0){
        import java.time.format.DateTimeFormatter
        val format: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

        DBs.setup()
        DB.localTx(implicit session=>{// 此括号内的代码 为 原子事务
          //sql1
          for ((tm,amount) <- amountArray) {
            //写数据库
            val tmSplits: Array[String] = tm.split(":")
            val tmId = tmSplits(0)
            val tmName = tmSplits(1)
            // 当前时间(统计时间)
            val localDateTime: LocalDateTime = LocalDateTime.now()
            val statTime: String = localDateTime.format(format)
            println("数据写入 执行")
            SQL("insert into trademark_amount_stat values(?,?,?,?)").bind(statTime,tmId,tmName,amount).update().apply()
          }

          //sql2   提交偏移量
          for (offsetRange <- offsetRanges) {
            val partitionId: Int = offsetRange.partition
            val offset: Long = offsetRange.untilOffset
            println("偏移量提交 执行")
            SQL("REPLACE INTO offsets(group_id,topic,partition_id,topic_offset) VALUES(?,?,?,?)")
              .bind(groupId,topic,partitionId,offset).update().apply()
          }

          //throw new RuntimeException("测试事务！！！！")
        })
      }
    }

    ssc.start()
    ssc.awaitTermination()

  }
}
