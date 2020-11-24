package com.xiao.newmall.realtime.dwd

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.xiao.newmall.realtime.bean.dim.{ProvinceInfo, UserState}
import com.xiao.newmall.realtime.bean.OrderInfo
import com.xiao.newmall.realtime.util.{OffsetManager, PhoenixUtil, XxEsUtil, XxKafkaSink, XxKafkaUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.phoenix.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object OrderInfoApp {
  def main(args: Array[String]): Unit = {

    //===加载流====
    val sparkConf = new SparkConf().setAppName("order_info_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    val topic = "ODS_ORDER_INFO"
    val groupId = "dwd_order_info_group"
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

    //数据标准化为OrderInfo
    val orderInfoDstream: DStream[OrderInfo] = inputGetOffsetDstream.map(record => {
      val jsonString: String = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])

      val createTimeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = createTimeArr(0)
      orderInfo.create_hour = createTimeArr(1).split(":")(0)

      orderInfo
    })


    //===关联HBase维表 查询用户状态====
    val orderWithIfFirstDstream: DStream[OrderInfo] = orderInfoDstream.mapPartitions { orderInfoItr =>
      val orderInfoList: List[OrderInfo] = orderInfoItr.toList
      if (orderInfoList.size > 0) {
        //针对分区中的订单中的所有客户 进行批量查询
        val userIds: String = orderInfoList.map("'" + _.user_id + "'").mkString(",")

        val sql = "select user_id,if_consumed from USER_STATE where user_id in (" + userIds + ")"
        val userStateList: List[JSONObject] = PhoenixUtil.queryList(sql)
        // [{USERID:123, IF_ORDERED:1 },{USERID:2334, IF_ORDERED:1 },{USERID:4355, IF_ORDERED:1 }]
        // 进行转换 把List[Map] 变成Map
        val userIfOrderedMap: Map[Long, String] = userStateList.map{userStateJsonObj =>
          (userStateJsonObj.getLong("USER_ID").toLong, userStateJsonObj.getString("IF_ORDERED"))
        }.toMap
        //{123:1,2334:1,4355:1}
        //进行判断 ，打首单标志
        for (orderInfo <- orderInfoList) {
          val ifOrderedUser: String = userIfOrderedMap.getOrElse(orderInfo.user_id, "0") //
          //是下单用户不是首单   否->首单
          if (ifOrderedUser == "1") {
            orderInfo.if_first_order = "0"
          } else {
            orderInfo.if_first_order = "1"
          }
        }
        orderInfoList.toIterator
      } else {
        orderInfoItr
      }

    }

    // 利用Hbase 进行查询过滤 识别首单，只能进行跨批次的判断
    // 如果新用户在同一批次内 多次下单 会造成 该批次该用户所有订单都识别为首单
    // 应该同一批次一个用户只有最早的订单 为首单 其他的单据为非首单
    // 处理办法： 1 同一批次 同一用户  2 最早的订单  3 标记首单
    //            1 分组 按用户        2 排序 取最早 3 如果最早的订单被标记为首单 除最早的单据一律改为非首单
    //            1 groupbykey         2 sortWith    3 ...

    //在一个批次内 第一笔如果是首单 那么本批次的该用户其他单据改为非首单
    // 以userId 进行分组
    val groupByUserDstream: DStream[(Long, Iterable[OrderInfo])] = orderWithIfFirstDstream.map(orderInfo => (orderInfo.user_id, orderInfo)).groupByKey()

    val orderInfoFinalDstream: DStream[OrderInfo] = groupByUserDstream.flatMap { case (userId, orderInfoItr) =>
      val orderList: List[OrderInfo] = orderInfoItr.toList
      //
      if (orderList.size > 1) { //   如果在这个批次中这个用户有多笔订单
        val sortedOrderList: List[OrderInfo] = orderList.sortWith((orderInfo1, orderInfo2) => orderInfo1.create_time < orderInfo2.create_time)
        if (sortedOrderList(0).if_first_order == "1") { //排序后，如果第一笔订单是首单，那么其他的订单都取消首单标志
          for (i <- 1 to sortedOrderList.size - 1) {
            sortedOrderList(i).if_first_order = "0"
          }
        }
        sortedOrderList
      } else { //就一笔订单
        orderList
      }
    }

    //=====合并维度信息（省 用户信息。。。）======
    //优化 ： 因为传输量小   使用数据的占比达  可以考虑使用广播变量  查询hbase的次数会变小   分区越多效果越明显
    //利用driver进行查询，再利用广播变量进行分发
    val orderInfoWithProvinceDstream: DStream[OrderInfo] = orderInfoFinalDstream.transform { rdd =>
      //driver 按批次周期性执行
      val provinceJsonObjList: List[JSONObject] = PhoenixUtil.queryList("select id,name,area_code,iso_code,iso_3166_2 from NEWMALL_PROVINCE_INFO")

      val provinceMap: Map[Long, ProvinceInfo] = provinceJsonObjList.map { jsonObj =>
        (jsonObj.getLong("ID").toLong, jsonObj.toJavaObject(classOf[ProvinceInfo]))
      }.toMap
      val provinceMapBC: Broadcast[Map[Long, ProvinceInfo]] = ssc.sparkContext.broadcast(provinceMap)//可以节省查询次数

      val orderInfoWithProvinceRDD: RDD[OrderInfo] = rdd.map { orderInfo =>
        val provinceMap: Map[Long, ProvinceInfo] = provinceMapBC.value
        val provinceInfo: ProvinceInfo = provinceMap.getOrElse(orderInfo.province_id, null)
        if (provinceInfo != null) {
          orderInfo.province_name = provinceInfo.name
          orderInfo.province_area_code = provinceInfo.area_code
          orderInfo.province_iso_code = provinceInfo.iso_code
          orderInfo.province_iso_3166_2 = provinceInfo.iso_3166_2
        }
        orderInfo
      }
      orderInfoWithProvinceRDD
    }

    //orderInfoFinalWithProvinceDstream.print(1000)

    //============== 用户信息关联 =================
    val orderInfoWithUserDstream: DStream[OrderInfo] = orderInfoWithProvinceDstream.mapPartitions { orderInfoItr =>
      val orderList: List[OrderInfo] = orderInfoItr.toList
      if (orderList.size > 0) {
        val userIdList: List[Long] = orderList.map(_.user_id)
        val sql = " select id, user_level, birthday, gender, age_group, gender_name from newmall_user_info where id in('" + userIdList.mkString("','") + "')"
        val userJsonObjList: List[JSONObject] = PhoenixUtil.queryList(sql)
        val userJsonObjMap: Map[Long, JSONObject] = userJsonObjList.map(userJsonObj => (userJsonObj.getLongValue("ID"), userJsonObj)).toMap
        for (orderInfo <- orderList) {
          val userJsonObj: JSONObject = userJsonObjMap.getOrElse(orderInfo.user_id, null)
          if(userJsonObj!=null){
            orderInfo.user_age_group = userJsonObj.getString("AGE_GROUP")
            orderInfo.user_gender = userJsonObj.getString("GENDER_NAME")
          }
        }
      }
      orderList.toIterator
    }

    //orderInfoWithUserDstream.print(1000)

   //保存数据
    orderInfoWithUserDstream.foreachRDD{ rdd=>

      rdd.cache()

      //同步到userState表中  只有标记了 是首单的用户 才需要同步到用户状态表中
      val userStateRdd: RDD[UserState] = rdd.filter(_.if_first_order == "1").map(orderInfo => UserState(orderInfo.id.toString, orderInfo.if_first_order))
      //Seq 中的字段顺序 和 rdd中对象的顺序一致
      userStateRdd.saveToPhoenix("USER_STATE", Seq("USER_ID", "IF_CONSUMED"), new Configuration(), Some("centos01,centos02,centos03:2181"))


      //将明细数据保存ES 并发送DWS层
      rdd.foreachPartition { orderInfoItr =>

        val orderInfoList: List[(String, OrderInfo)] = orderInfoItr.toList.map(orderInfo => (orderInfo.id.toString, orderInfo))
        val dateStr: String = new SimpleDateFormat("yyyyMMdd").format(new Date())
        XxEsUtil.bulkDoc(orderInfoList, "newmall_order_info_" + dateStr)

        for ((id,orderInfo) <- orderInfoList ) {
          println(orderInfo)
          XxKafkaSink.send("DWD_ORDER_INFO",id,JSON.toJSONString(orderInfo,new SerializeConfig(true)))
        }

      }
      OffsetManager.saveOffset(topic,groupId,offsetRanges)

    }


    ssc.start()
    ssc.awaitTermination()





  }
}
